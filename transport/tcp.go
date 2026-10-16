// Package transport is the first genuinely real (non-simulated) driver
// for quorum: actual TCP sockets moving actual bytes between actual
// processes, instead of sim.Network's in-memory, deterministic routing.
// Raft's own protocol already tolerates message loss, duplication, and
// reordering — that's what retries and idempotent handling are for — so
// this transport makes no stronger promise than "best effort, reconnect
// on failure." Correctness lives entirely in quorum.Node; this package's
// only job is getting bytes from one process to another.
package transport

import (
	"encoding/gob"
	"net"
	"sync"
	"time"

	"quorum"
)

// TCPTransport sends and receives quorum.Message values over TCP: one
// persistent outbound connection per peer that we dial and keep
// redialing on failure, and an accept loop for the connections peers
// dial to us. Wire encoding is gob — quorum.Message has enough fields
// that hand-rolling a binary format here would just be tedious busywork,
// unlike the WAL, where a compact, crash-safe format actually earns its
// complexity.
type TCPTransport struct {
	id        quorum.NodeID
	peerAddrs map[quorum.NodeID]string

	listener net.Listener

	mu      sync.Mutex
	senders map[quorum.NodeID]*gob.Encoder
	conns   map[quorum.NodeID]net.Conn // outbound, keyed by peer
	inbound map[net.Conn]bool          // accepted connections — tracked so Close can shut these down too, or a readLoop blocked on Decode() would never exit and wg.Wait() would hang forever
	closed  bool

	incoming chan quorum.Message
	done     chan struct{}
	wg       sync.WaitGroup
}

// NewTCPTransport starts listening on listenAddr and begins dialing out
// to every address in peerAddrs, redialing in the background for as
// long as the transport is open.
func NewTCPTransport(id quorum.NodeID, listenAddr string, peerAddrs map[quorum.NodeID]string) (*TCPTransport, error) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	if peerAddrs == nil {
		// Never leave this nil: RegisterPeer writes into it later, and a
		// write to a nil map panics — which would happen while holding
		// t.mu, permanently deadlocking every future Close() caller
		// rather than failing cleanly.
		peerAddrs = map[quorum.NodeID]string{}
	}
	t := &TCPTransport{
		id:        id,
		peerAddrs: peerAddrs,
		listener:  ln,
		senders:   map[quorum.NodeID]*gob.Encoder{},
		conns:     map[quorum.NodeID]net.Conn{},
		inbound:   map[net.Conn]bool{},
		incoming:  make(chan quorum.Message, 256),
		done:      make(chan struct{}),
	}
	t.wg.Add(1)
	go t.acceptLoop()
	for peer, addr := range peerAddrs {
		t.wg.Add(1)
		go t.maintainOutbound(peer, addr)
	}
	return t, nil
}

// RegisterPeer starts dialing out to a peer not known when this
// transport was constructed — the real-transport counterpart of a
// cluster membership change adding a new server. Safe to call
// concurrently with normal operation; a no-op if addr is already
// registered for peer.
func (t *TCPTransport) RegisterPeer(peer quorum.NodeID, addr string) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	if _, already := t.peerAddrs[peer]; already {
		t.mu.Unlock()
		return
	}
	t.peerAddrs[peer] = addr
	t.mu.Unlock()

	t.wg.Add(1)
	go t.maintainOutbound(peer, addr)
}

// Addr returns the actual listening address — useful in tests that bind
// to ":0" and need to discover the OS-assigned port afterward.
func (t *TCPTransport) Addr() string { return t.listener.Addr().String() }

// Incoming is the channel a driver reads received messages from.
func (t *TCPTransport) Incoming() <-chan quorum.Message { return t.incoming }

// Send is best-effort: if there's no live connection to m.To right now,
// or the write fails, the message is simply dropped and the connection
// (if any) is torn down so maintainOutbound redials. Raft's own retry
// logic (heartbeats, backoff on AppendEntries failure) is what makes
// this an acceptable contract rather than a gap to paper over here.
func (t *TCPTransport) Send(m quorum.Message) {
	t.mu.Lock()
	enc, ok := t.senders[m.To]
	conn := t.conns[m.To]
	t.mu.Unlock()
	if !ok {
		return
	}
	if err := enc.Encode(&m); err != nil {
		t.mu.Lock()
		if t.conns[m.To] == conn { // don't clobber a connection a concurrent redial already replaced
			delete(t.senders, m.To)
			delete(t.conns, m.To)
		}
		t.mu.Unlock()
		conn.Close()
	}
}

// Close shuts the transport down: stops accepting, stops redialing, and
// closes every open connection. Incoming() is not closed (a driver still
// mid-select on it would otherwise see a spurious zero-value read) —
// callers should stop reading once they've initiated Close themselves.
func (t *TCPTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	for _, c := range t.conns {
		c.Close()
	}
	for c := range t.inbound {
		c.Close()
	}
	t.mu.Unlock()

	close(t.done)
	err := t.listener.Close()
	t.wg.Wait()
	return err
}

func (t *TCPTransport) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.done:
				return
			default:
				continue
			}
		}
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			conn.Close()
			continue
		}
		t.inbound[conn] = true
		t.mu.Unlock()

		t.wg.Add(1)
		go t.readLoop(conn)
	}
}

func (t *TCPTransport) readLoop(conn net.Conn) {
	defer func() {
		t.mu.Lock()
		delete(t.inbound, conn)
		t.mu.Unlock()
		conn.Close()
		t.wg.Done()
	}()
	dec := gob.NewDecoder(conn)
	for {
		var m quorum.Message
		if err := dec.Decode(&m); err != nil {
			return // connection closed or corrupted; the sender's own maintainOutbound will redial if it was them
		}
		select {
		case t.incoming <- m:
		case <-t.done:
			return
		}
	}
}

func (t *TCPTransport) maintainOutbound(peer quorum.NodeID, addr string) {
	defer t.wg.Done()
	for {
		select {
		case <-t.done:
			return
		default:
		}

		t.mu.Lock()
		_, exists := t.senders[peer]
		t.mu.Unlock()
		if exists {
			select {
			case <-t.done:
				return
			case <-time.After(50 * time.Millisecond):
				continue
			}
		}

		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			select {
			case <-t.done:
				return
			case <-time.After(200 * time.Millisecond):
				continue
			}
		}

		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			conn.Close()
			return
		}
		t.senders[peer] = gob.NewEncoder(conn)
		t.conns[peer] = conn
		t.mu.Unlock()
	}
}
