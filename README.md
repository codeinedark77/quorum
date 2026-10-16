# quorum

[![CI](https://github.com/codeinedark77/quorum/actions/workflows/ci.yml/badge.svg)](https://github.com/codeinedark77/quorum/actions/workflows/ci.yml)
![Tests](https://img.shields.io/badge/tests-passing%20(race--clean)-brightgreen)
![Go Version](https://img.shields.io/badge/go-1.21%20%7C%201.22%20%7C%201.23-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Goroutines](https://img.shields.io/badge/goroutines%20in%20core-0-success)
![Sockets](https://img.shields.io/badge/sockets%20in%20core-0-success)
![Timers](https://img.shields.io/badge/timers%20in%20core-0-success)

**`quorum`** is a deterministic, ultra-pure Raft consensus implementation in Go, engineered from first principles with **0 goroutines, 0 sockets, and 0 timers** in its consensus core.

By completely separating state machine transitions from I/O, threads, and wall-clock time, `quorum` achieves mathematical determinism: entire multi-node clusters can be simulated and stress-tested in a single OS thread across millions of randomized chaos schedules, reproducing any subtle consensus failure down to the exact microsecond.

---

## Key Architectural Principles

1. **Pure State Machine Core (`0-0-0`)**
   - The consensus core owns **0 goroutines**, **0 network sockets**, and **0 wall-clock timers**.
   - Timing is driven by explicit `Tick()` calls; RPCs are passed via `Step(msg)`.
   - All side-effects (messages to transmit, log entries to persist, snapshots to apply) are yielded via `Ready()` batches and acknowledged via `Advance()`.

2. **PreVote Safety Protocol**
   - Implements Raft §9.6 PreVote. A partitioned or lagging node tests whether it can win a majority *before* bumping its term.
   - Prevents partitioned nodes from causing disruption or forcing valid leaders to step down upon reconnection.

3. **Linearizable Reads (`ReadIndex`)**
   - Serves high-throughput read requests without writing entries to the Raft log.
   - The leader checks a heartbeat quorum with a majority of peers and waits until the local application index catches up to the confirmed commit barrier before returning data.

4. **Dynamic Membership Transitions**
   - Log-replicated single-node membership changes (`AddVoter`, `RemoveVoter`).
   - Guarantees overlapping majorities at all times; safely retires departing nodes without stale-ack race conditions.

5. **Graceful Leadership Transfer**
   - Replicates pending log entries to the target follower, pauses client proposals, and issues an immediate `MsgTimeoutNow` to initiate a zero-downtime, election-free handover.

6. **Crash-Safe Storage Engine (`FileStorage`)**
   - Durable append-only disk storage with CRC32 frame checksums, atomic `fsync` barriers, and automatic torn-write truncation.
   - Recovers safely from power cuts or `SIGKILL` mid-write without losing committed history.

7. **Zero Third-Party Dependencies**
   - Uses exclusively Go standard library (`math/rand`, `sync`, `net`, `os`).

---

## Directory Layout

```
quorum/
├── node.go             # Raft consensus state machine (Follower, Candidate, PreCandidate, Leader)
├── node_test.go        # Exhaustive unit test suite covering state transitions and edge cases
├── message.go          # Wire protocol and RPC message definitions (Vote, AppendEntries, Snapshots)
├── types.go            # LogIndex, Term, NodeID, LogEntry, and Ready struct
├── storage.go          # Storage interface contract (Log entries, HardState, Snapshots)
├── memstorage.go       # High-speed in-memory storage for testing and volatile nodes
├── filestorage.go      # Crash-safe, CRC32-checksummed append-only disk storage
├── storage_test.go     # Conformance and torn-write recovery tests
├── readindex.go        # Raft ReadIndex protocol for linearizable queries
├── membership.go       # Log-replicated membership reconfiguration logic
├── transfer.go         # Graceful leadership transfer protocol (MsgTimeoutNow)
├── sim/                # Deterministic multi-node network simulator with chaos fault injection
│   ├── network.go      # Simulated network with drops, partitions, duplicates, and reordering
│   ├── election_test.go
│   ├── prevote_test.go
│   ├── replication_test.go
│   ├── membership_test.go
│   └── snapshot_test.go
├── kv/                 # Replicated distributed key-value store built on Quorum
│   ├── kv.go           # In-memory KV state machine with snapshot serialization
│   ├── server.go       # High-level server binding Quorum + KVStore + TCP
│   └── server_test.go  # Linearizable Get/Put tests under real restarts and snapshot transfers
└── transport/          # Production TCP networking layer
    ├── tcp.go          # Non-blocking TCP connection manager with framed binary encoding
    ├── driver.go       # Event-driven driver loop binding Transport, Storage, and Node
    ├── tcp_test.go     # TCP reconnection, payload fidelity, and multi-node cluster tests
    └── driver_test.go  # Driver lifecycle, storage error propagation, and idempotency tests
```

---

## Quick Start

### 1. Using the Deterministic Core Directly

For custom event loops, simulations, or actor systems:

```go
package main

import (
	"fmt"
	"quorum"
)

func main() {
	storage := quorum.NewMemStorage()
	cfg := quorum.DefaultConfig()

	// Initialize node 1 in a 3-node cluster
	peers := []quorum.NodeID{1, 2, 3}
	node := quorum.NewNode(1, peers, cfg, storage)

	// Advance virtual time
	for i := 0; i < cfg.ElectionTicksMax; i++ {
		node.Tick()
	}

	// Drain ready batch
	rd := node.Ready()

	// 1. Save HardState and Entries to stable storage
	storage.Append(rd.Entries)

	// 2. Transmit outgoing messages to peers
	for _, msg := range rd.Messages {
		fmt.Printf("Send RPC %s from Node %d to Node %d\n", msg.Type, msg.From, msg.To)
	}

	// 3. Apply committed entries to application state machine
	for _, entry := range rd.CommittedEntries {
		fmt.Printf("Apply committed entry at index %d\n", entry.Index)
	}

	// 4. Acknowledge batch processing
	node.Advance()
}
```

### 2. Running a Replicated Cluster over TCP

For production network deployments, use the `transport.Driver`:

```go
package main

import (
	"log"
	"time"
	"quorum"
	"quorum/transport"
)

func main() {
	nodeID := quorum.NodeID(1)
	peers := []quorum.NodeID{1, 2, 3}
	cfg := quorum.DefaultConfig()
	storage := quorum.NewMemStorage()

	node := quorum.NewNode(nodeID, peers, cfg, storage)
	trans := transport.NewTCPTransport("127.0.0.1:9001")
	if err := trans.Listen(); err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Register cluster peer endpoints
	trans.RegisterPeer(2, "127.0.0.1:9002")
	trans.RegisterPeer(3, "127.0.0.1:9003")

	// Start the autonomous event loop
	driver := transport.NewDriver(node, trans, storage, 10*time.Millisecond)
	driver.Start()
	defer driver.Stop()

	// Propose data through consensus
	err := driver.Propose([]byte("user_tx_data_payload"))
	if err != nil {
		log.Printf("propose error: %v", err)
	}
}
```

---

## Verification & Testing

The test suite runs with `-race` across all packages:

```bash
# Run all tests with race detector
go test -v -race ./...

# Run fresh uncached test passes
go test -count=1 ./...
```

### Test Coverage Highlights

- **`TestRealClusterElectsLeaderAndReplicatesOverTCP`**: Boots a 3-node cluster with real loopback TCP sockets, elects a leader, and verifies consensus replication across all sockets.
- **`TestFileStorageRecoversFromTornWrite`**: Simulates power failure and partial disk writes; verifies that `FileStorage` automatically truncates corrupt trailing frames and recovers valid history.
- **`TestElectionSafetyAcrossManySeeds`**: Runs the deterministic simulator across hundreds of PRNG seeds with random message drop and delay schedules, verifying that at most one leader can be elected per term.
- **`TestPreVotePreventsDisruptionFromIsolatedNode`**: Confirms that a partitioned node cannot inflate cluster terms or disrupt healthy leaders.
- **`TestRealKVClusterPutAndLinearizableGet`**: Verifies linearizability and state-machine snapshot transfer across live nodes.

---

## License

MIT License. Copyright (c) 2026 Yash Kandhare.
