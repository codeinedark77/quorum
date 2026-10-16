package transport

import (
	"sync"
	"time"

	"quorum"
)

// Driver runs one Node in real time against a real Storage and
// Transport: a single goroutine ticks it on a wall-clock interval,
// processes incoming network messages, and drains Ready() after each —
// mirroring exactly what sim.Network.Tick does for the deterministic
// simulator, just replacing "simulated tick, simulated routing" with
// "real timer, real sockets."
//
// Node is not safe for concurrent use, so every external call — Propose,
// RequestReadIndex, TransferLeadership, or just reading current state —
// is routed through Do, which hands a closure to the driver's own
// goroutine and blocks until it's run there. That's the entire
// concurrency story: exactly one goroutine ever touches the Node.
type Driver struct {
	node      *quorum.Node
	storage   quorum.Storage
	transport *TCPTransport
	tick      time.Duration

	cmds     chan doCmd
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once

	// Callbacks run synchronously on the driver's own goroutine — keep
	// them fast. A production caller wanting async dispatch should queue
	// work from inside these rather than block here.
	OnCommitted func(quorum.LogEntry)
	OnSnapshot  func(*quorum.Snapshot)
	OnReadReady func(quorum.ReadResult)

	// OnStorageError fires if a Storage call fails. This is not a corner
	// to cut silently — a failed durability write means Ready()'s
	// Messages for this same batch may already be queued for send,
	// which is exactly the crash-then-lie-on-restart scenario
	// persistence exists to prevent. If unset, the driver panics, since
	// continuing to run past an unpersisted write is worse than
	// stopping.
	OnStorageError func(error)
}

// doCmd pairs a closure with a completion signal that Run only closes
// AFTER processReady has fully run for it — not right after the closure
// itself returns. Signaling early would let Do's caller believe a
// Propose call is "done" before its resulting storage writes, message
// sends, or callbacks have actually happened.
type doCmd struct {
	fn   func(n *quorum.Node)
	done chan struct{}
}

func NewDriver(node *quorum.Node, storage quorum.Storage, tr *TCPTransport, tickInterval time.Duration) *Driver {
	return &Driver{
		node:      node,
		storage:   storage,
		transport: tr,
		tick:      tickInterval,
		cmds:      make(chan doCmd),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// Run blocks until Stop is called. Call it in its own goroutine.
func (d *Driver) Run() {
	defer close(d.done)
	ticker := time.NewTicker(d.tick)
	defer ticker.Stop()
	for {
		select {
		case <-d.stop:
			return
		case <-ticker.C:
			d.node.Tick()
			d.processReady()
		case m := <-d.transport.Incoming():
			d.node.Step(m)
			d.processReady()
		case cmd := <-d.cmds:
			cmd.fn(d.node)
			d.processReady()
			close(cmd.done)
		}
	}
}

// Stop ends Run's loop and waits for it to actually exit. Safe to call
// more than once — a second call just waits on the same exit signal
// rather than re-closing an already-closed channel.
func (d *Driver) Stop() {
	d.stopOnce.Do(func() {
		close(d.stop)
	})
	<-d.done
}

// Do runs fn on the driver's own goroutine and blocks until fn AND its
// resulting Ready() processing (storage writes, message sends,
// callbacks) have fully completed — the only safe way to touch the
// underlying Node from any other goroutine, with a completion signal
// callers can actually trust.
func (d *Driver) Do(fn func(n *quorum.Node)) {
	done := make(chan struct{})
	d.cmds <- doCmd{fn: fn, done: done}
	<-done
}

// Propose is Do wrapping quorum.Node.Propose for convenience — the most
// common external call a real caller needs.
func (d *Driver) Propose(data []byte) (idx quorum.LogIndex, term quorum.Term, err error) {
	d.Do(func(n *quorum.Node) { idx, term, err = n.Propose(data) })
	return
}

// State is a safe, point-in-time snapshot of the fields tests and
// simple callers most often need, taken atomically on the driver's own
// goroutine rather than raced from outside it.
type State struct {
	Role      quorum.Role
	Term      quorum.Term
	LastIndex quorum.LogIndex
}

func (d *Driver) State() (s State) {
	d.Do(func(n *quorum.Node) {
		s = State{Role: n.Role(), Term: n.Term(), LastIndex: n.LastIndex()}
	})
	return
}

func (d *Driver) processReady() {
	ready := d.node.Ready()

	if ready.TruncateFrom > 0 {
		d.checkStorage(d.storage.TruncateFrom(ready.TruncateFrom))
	}
	if len(ready.EntriesToSave) > 0 {
		d.checkStorage(d.storage.AppendEntries(ready.EntriesToSave))
	}
	if ready.HardState != nil {
		d.checkStorage(d.storage.SaveHardState(ready.HardState.Term, ready.HardState.VotedFor))
	}
	if ready.SnapshotBoundary != nil {
		d.checkStorage(d.storage.Compact(ready.SnapshotBoundary.Index, ready.SnapshotBoundary.Term))
	}

	for _, m := range ready.Messages {
		d.transport.Send(m)
	}

	if d.OnCommitted != nil {
		for _, e := range ready.CommittedEntries {
			d.OnCommitted(e)
		}
	}
	if d.OnSnapshot != nil && ready.InstalledSnapshot != nil {
		d.OnSnapshot(ready.InstalledSnapshot)
	}
	if d.OnReadReady != nil {
		for _, r := range ready.ReadyReads {
			d.OnReadReady(r)
		}
	}
}

func (d *Driver) checkStorage(err error) {
	if err == nil {
		return
	}
	if d.OnStorageError != nil {
		d.OnStorageError(err)
		return
	}
	panic("quorum/transport: storage failure with no OnStorageError handler set: " + err.Error())
}
