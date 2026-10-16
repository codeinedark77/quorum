// Package sim provides a deterministic, in-memory network for testing
// quorum.Node clusters. Nothing here touches a real clock or a real
// socket: Tick() advances every node's logical clock by one step, then
// routes whatever messages fell out under whatever faults have been
// configured. A test can therefore compress an election, a partition,
// and a recovery into a few hundred Tick() calls that run in
// microseconds and reproduce byte-for-byte given a fixed seed.
package sim

import (
	"fmt"
	"math/rand"

	"quorum"
)

// Network is a deterministic transport plus a fault-injection surface:
// message drops and arbitrary group partitions. It also plays the role a
// real driver would: persisting each node's Ready() output to that
// node's Storage in the required order before delivering messages,
// tracking what each node has applied — with the same index-based dedup
// a real state machine would need to make replay-after-restart safe —
// and remembering the latest snapshot bytes per node the way a state
// machine's own snapshot store would, so SimulateRestart can hand them
// back on reconstruction.
type Network struct {
	nodes    map[quorum.NodeID]*quorum.Node
	storages map[quorum.NodeID]quorum.Storage
	order    []quorum.NodeID // stable iteration order, so a fixed seed is reproducible

	group    map[quorum.NodeID]int // same group == can communicate
	dropRate float64
	rng      *rand.Rand

	applied       map[quorum.NodeID][]quorum.LogEntry
	appliedIndex  map[quorum.NodeID]quorum.LogIndex
	snapshotBytes map[quorum.NodeID][]byte
	readyReads    map[quorum.NodeID][]quorum.ReadResult
}

func NewNetwork(seed int64) *Network {
	return &Network{
		nodes:         map[quorum.NodeID]*quorum.Node{},
		storages:      map[quorum.NodeID]quorum.Storage{},
		group:         map[quorum.NodeID]int{},
		rng:           rand.New(rand.NewSource(seed)),
		applied:       map[quorum.NodeID][]quorum.LogEntry{},
		appliedIndex:  map[quorum.NodeID]quorum.LogIndex{},
		snapshotBytes: map[quorum.NodeID][]byte{},
		readyReads:    map[quorum.NodeID][]quorum.ReadResult{},
	}
}

// Add registers a node with a fresh in-memory Storage backing it. Tests
// that need to simulate a crash and restart use SimulateRestart, which
// reuses the same Storage instance the node was already writing to.
func (net *Network) Add(n *quorum.Node) {
	net.nodes[n.ID()] = n
	net.storages[n.ID()] = quorum.NewMemStorage()
	net.order = append(net.order, n.ID())
	net.group[n.ID()] = 0 // fully connected by default
}

// Split partitions the network into isolated groups: nodes in different
// groups can't exchange messages until Heal is called. Any node not
// mentioned keeps its previous group.
func (net *Network) Split(groups ...[]quorum.NodeID) {
	for g, ids := range groups {
		for _, id := range ids {
			net.group[id] = g + 1 // +1 so group 0 always means "healed"
		}
	}
}

// Isolate cuts a single node off from the rest of the cluster while
// leaving everyone else fully connected — the common "leader goes dark"
// scenario.
func (net *Network) Isolate(id quorum.NodeID) {
	var rest []quorum.NodeID
	for _, other := range net.order {
		if other != id {
			rest = append(rest, other)
		}
	}
	net.Split([]quorum.NodeID{id}, rest)
}

// Heal reconnects every node.
func (net *Network) Heal() {
	for id := range net.group {
		net.group[id] = 0
	}
}

func (net *Network) SetDropRate(p float64) { net.dropRate = p }

func (net *Network) deliverable(m quorum.Message) bool {
	if net.group[m.From] != net.group[m.To] {
		return false
	}
	if net.dropRate > 0 && net.rng.Float64() < net.dropRate {
		return false
	}
	return true
}

// Tick advances every node by one logical step, persists whatever each
// node's Ready() says must be durable, collects newly-applied entries
// and outbound messages, then delivers the messages. Persistence happens
// in the order Storage's contract requires: TruncateFrom before
// AppendEntries, since a follower's log conflict resolution can report
// both in the same Ready() and getting this backwards would let a
// truncation silently erase the entries meant to replace what it just
// discarded.
//
// Message collection and delivery are two strictly separate passes — a
// message delivered this tick can prompt a response, but that response
// is only collected on the *next* Tick call. That gives a clean,
// order-independent "one hop per Tick" model instead of latency
// depending on iteration order.
func (net *Network) Tick() {
	for _, id := range net.order {
		net.nodes[id].Tick()
	}

	var outbox []quorum.Message
	for _, id := range net.order {
		ready := net.nodes[id].Ready()
		storage := net.storages[id]

		if ready.TruncateFrom > 0 {
			if err := storage.TruncateFrom(ready.TruncateFrom); err != nil {
				panic(fmt.Sprintf("sim: storage truncate failed for %s: %v", id, err))
			}
		}
		if len(ready.EntriesToSave) > 0 {
			if err := storage.AppendEntries(ready.EntriesToSave); err != nil {
				panic(fmt.Sprintf("sim: storage append failed for %s: %v", id, err))
			}
		}
		if ready.HardState != nil {
			if err := storage.SaveHardState(ready.HardState.Term, ready.HardState.VotedFor); err != nil {
				panic(fmt.Sprintf("sim: storage save hard state failed for %s: %v", id, err))
			}
		}
		if ready.SnapshotBoundary != nil {
			if err := storage.Compact(ready.SnapshotBoundary.Index, ready.SnapshotBoundary.Term); err != nil {
				panic(fmt.Sprintf("sim: storage compact failed for %s: %v", id, err))
			}
		}

		if ready.InstalledSnapshot != nil {
			// Mirrors a state machine loading a snapshot wholesale: its
			// own durable applied-index jumps straight to the snapshot's
			// index, and it durably keeps the bytes themselves so a
			// later restart (SimulateRestart) can still serve them to a
			// lagging peer.
			net.appliedIndex[id] = ready.InstalledSnapshot.Index
			net.snapshotBytes[id] = ready.InstalledSnapshot.Data
			net.applied[id] = nil // the individual entries it summarizes no longer exist to compare against
		}

		if len(ready.ReadyReads) > 0 {
			net.readyReads[id] = append(net.readyReads[id], ready.ReadyReads...)
		}

		for _, e := range ready.CommittedEntries {
			if e.Index <= net.appliedIndex[id] {
				continue
			}
			net.applied[id] = append(net.applied[id], e)
			net.appliedIndex[id] = e.Index
		}

		outbox = append(outbox, ready.Messages...)
	}

	for _, m := range outbox {
		if !net.deliverable(m) {
			continue
		}
		if to, ok := net.nodes[m.To]; ok {
			to.Step(m)
		}
	}
}

func (net *Network) TickN(n int) {
	for i := 0; i < n; i++ {
		net.Tick()
	}
}

// Leaders returns every node currently in the Leader role.
func (net *Network) Leaders() []*quorum.Node {
	var out []*quorum.Node
	for _, id := range net.order {
		if net.nodes[id].Role() == quorum.Leader {
			out = append(out, net.nodes[id])
		}
	}
	return out
}

func (net *Network) Node(id quorum.NodeID) *quorum.Node { return net.nodes[id] }

// Applied returns every log entry durably applied for the given node, in
// order, deduplicated by index exactly as a real state machine's own
// applied-index tracking would. It resets to empty the moment a snapshot
// is installed (the individual entries the snapshot summarizes are gone,
// not comparable to another node's discrete entries at those indices).
func (net *Network) Applied(id quorum.NodeID) []quorum.LogEntry {
	return net.applied[id]
}

// ReadyReads returns every read request confirmed safe to serve for the
// given node so far. Query this instead of calling Ready() directly on a
// node obtained via Node() — Tick already calls Ready() internally as
// part of normal operation, so a second direct call would just see
// nothing, everything having already been drained.
func (net *Network) ReadyReads(id quorum.NodeID) []quorum.ReadResult {
	return net.readyReads[id]
}

// CompactLog asks node id to fold its log up through lastIncludedIndex
// into a snapshot, and remembers the snapshot bytes the way a state
// machine's own store would, so a later SimulateRestart can re-supply
// them.
func (net *Network) CompactLog(id quorum.NodeID, lastIncludedIndex quorum.LogIndex, data []byte) error {
	_, err := net.nodes[id].CompactLog(lastIncludedIndex, data)
	if err != nil {
		return err
	}
	net.snapshotBytes[id] = data
	return nil
}

// SimulateRestart discards a node's in-memory instance and reconstructs
// a fresh one purely from whatever its Storage actually persisted, plus
// whatever snapshot bytes this Network has separately tracked for it —
// modeling a process crash and restart where the state machine's own
// snapshot store survived independently of quorum's WAL, exactly as
// NewNodeFromStorage's contract expects. Nothing the crashed instance
// held that wasn't durably saved survives; that's the entire point.
func (net *Network) SimulateRestart(id quorum.NodeID) {
	storage := net.storages[id]
	term, votedFor, snapIdx, snapTerm, log, err := storage.Load()
	if err != nil {
		panic(fmt.Sprintf("sim: storage load failed for %s: %v", id, err))
	}

	var peers []quorum.NodeID
	for _, other := range net.order {
		if other != id {
			peers = append(peers, other)
		}
	}

	seed := int64(len(id)) + int64(len(net.order))*7919
	restarted := quorum.NewNodeFromStorage(
		id, peers, quorum.DefaultConfig(), seed,
		term, votedFor, snapIdx, snapTerm, log,
		net.snapshotBytes[id], net.appliedIndex[id],
	)
	net.nodes[id] = restarted
}
