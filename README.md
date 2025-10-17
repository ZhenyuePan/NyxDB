# NyxDB · Cloud-Native Distributed SQL Database

NyxDB is an open-source, cloud-native, strongly consistent database that speaks standard SQL (PostgreSQL / MySQL compatibility in progress) while running on top of an append-only MVCC storage engine, a replicated Raft log, and a modular service layer. The goal is simple: **deliver elastic scale, financial-grade reliability, and a developer-friendly SQL surface on commodity infrastructure.**

---

## Vision & Design Tenets

| Vision | Details |
| --- | --- |
| **Cloud Native First** | NyxDB is designed to live inside Kubernetes: stateless SQL frontends, stateful Raft groups, sidecar aware health signaling, and chaos-tested failure handling. |
| **Separation of Concerns** | SQL ↔ Transaction ↔ Storage ↔ Replication are cleanly layered. Each tier can evolve independently (e.g. swap parser, tune compaction, change transport). |
| **Log-Structured & MVCC** | Storage is an append-only Bitcask-inspired engine with multi-version concurrency control. Reads never block writes; merges reclaim space safely. |
| **Raft is the new Paxos** | Every shard is a Raft group; the Raft log is a first-class API. Linearizable reads rely on ReadIndex, and persistence uses a compacted RaftStorage. |
| **Automation** | A future PD (Placement Driver) manages region splits, rebalancing, and global timestamp/oracle service. Current work focuses on single-group high availability. |

---

## Architecture Overview

```
┌──────────────────────┐
│  SQL Gateway (WIP)   │  ← PostgreSQL/MySQL wire compat planned
└──────────┬───────────┘
           │ SQL plan / transactions
┌──────────▼───────────┐
│  Txn Coordinator     │  ← MVCC + 2PC (percolator-style roadmap)
└──────────┬───────────┘
           │ Replicated log ops
┌──────────▼───────────┐
│  Raft Group (HA)     │  ← etcd/raft, gRPC transport, linearizable reads
└──────────┬───────────┘
           │ Append-only log records
┌──────────▼───────────┐
│  Storage Engine      │  ← Bitcask + MVCC, merge/compaction, backup/restore
└──────────────────────┘
```

Current milestone (Phase 2/4):
1. **Robust Storage Core** – ACID semantics, snapshot isolation, merge GC (complete).
2. **Highly Available Cluster** – Raft replication, gRPC transport, membership & chaos scenarios (in progress).
3. **Scalable Cluster** – Region/Shard + PD orchestration (planned).
4. **Developer Ready** – SQL protocol, coprocessor pushdown, observability, operator (planned).

---

## Key Capabilities (current)

- **Strong consistency**: writes go through Raft, reads use ReadIndex + applied-index waiting.
- **Snapshot isolation**: `BeginReadTxn / ReadTxnGet / EndReadTxn` with automatic handle TTL reclamation.
- **Automatic member persistence**: cluster membership survives restarts via `cluster/members.json`.
- **Chaos-tested replication**: integration tests cover partitions, artificial delay, node restarts.
- **Diagnostics-ready storage**: `raft/raft_state.bin` persists HardState, entries, snapshots for crash recovery.

---

## Quick Start

### Prerequisites
- Go 1.21+
- `protoc` / gRPC tooling (if regenerating APIs)

### Single-node playground
```bash
go run cmd/nyxdb-server/main.go \
  -config configs/server.example.yaml

# Example gRPC calls via grpcurl (Put/Get)
grpcurl -plaintext -d '{"key":"a","value":"b"}' localhost:10001 nyxdb.api.KV/Put
grpcurl -plaintext -d '{"key":"a"}' localhost:10001 nyxdb.api.KV/Get
```

### Multi-node (3 replicas)
1. Copy `configs/server.example.yaml` into three variants: `server1.yaml`, `server2.yaml`, `server3.yaml`.
   ```yaml
   # server1.yaml
   nodeID: 1
   engine:
     dir: /tmp/nyxdb-node1
     enableDiagnostics: true
   cluster:
     clusterMode: true
     nodeAddress: 127.0.0.1:9001    # gRPC address reused if omitted
     clusterAddresses:
       - 1@127.0.0.1:9001
       - 2@127.0.0.1:9002
       - 3@127.0.0.1:9003
   grpc:
     address: 127.0.0.1:10001
   ```
2. Start each node with its config (ports adjusted accordingly).
3. Use the gRPC Admin service:
   ```bash
   # Join additional nodes from leader's perspective
   grpcurl -plaintext -d '{"nodeId":2,"address":"127.0.0.1:9002"}' 127.0.0.1:10001 nyxdb.api.Admin/Join
   grpcurl -plaintext 127.0.0.1:10001 nyxdb.api.Admin/Members
   ```
4. Chaos testing (partition/delay) is covered by `go test ./internal/cluster -run TestClusterChaosResilience`.

---

## gRPC APIs

| Service | Methods | Notes |
| --- | --- | --- |
| `nyxdb.api.KV` | `Put`, `Get`, `Delete`, `BeginReadTxn`, `ReadTxnGet`, `EndReadTxn` | `Get` returns `codes.FailedPrecondition` with `leader=` hint when not leader. |
| `nyxdb.api.Admin` | `Join`, `Leave`, `Members`, `TriggerMerge` | Merge can be forced; future: automatic scheduling. |
| `nyxdb.api.RaftTransport` | `Send(stream)` | Internal replication stream; exposed via gRPC server. |

Future: SQL service (`SQL.Query/Exec`), health checks, metrics endpoints.

---

## Project Layout

```
cmd/
  nyxdb-server/      # Main server process (gRPC, Raft bootstrap)
  nyxdb-cli/         # CLI (planned)
internal/
  cluster/           # Cluster manager, membership, integration tests
  config/            # YAML config loader → engine options
  engine/            # Storage engine (Bitcask+MVCC), options, merge, iterator
  node/              # Raft node wrapper around etcd/raft
  raft/              # gRPC transport, transport abstractions
  server/grpc/       # KV/Admin services, gRPC server lifecycle
pkg/api/             # gRPC stubs (placeholder, to be replaced by generated code)
configs/             # Sample configs
docs/                # Design notes, architecture discussions
```

---

## Observability & State Files

| Path | Description |
| --- | --- |
| `<data_dir>/raft/raft_state.bin` | Raft HardState + entries + snapshot metadata. |
| `<data_dir>/cluster/members.json` | Persisted nodeID → address map (reloaded at startup). |
| Merge artifacts | `-merge/` directories are cleaned after successful compaction. |

Observability features:

- gRPC health: standard `grpc.health.v1.Health` service is exposed on the main gRPC port for liveness/readiness probes.
- Prometheus metrics: set `observability.metricsAddress` in the server config (example: `127.0.0.1:2112`) to expose `/metrics` with gauges for Raft term/indices, member counts, snapshot progress, and read-transactions.
- Snapshot metrics: each snapshot export records duration and payload size, published via `/metrics` (`cluster_last_snapshot_duration_seconds`, `cluster_last_snapshot_size_bytes`) and `admin snapshot-status`.

The longer-term roadmap still includes richer metrics (engine IO, pprof) and SQL-layer observability.

### Snapshot tuning knobs

`cluster` config exposes several guardrails to control snapshot cadence:

- `snapshotMinInterval`: minimum wall-clock gap between successive snapshots.
- `snapshotMaxAppliedLag`: maximum allowed `lastIndex - appliedIndex` gap before snapshots are blocked (defaults to `2 * snapshotCatchUpEntries`).
- `snapshotDirSizeThreshold`, `snapshotThreshold`, and `snapshotCatchUpEntries` continue to bound log growth and retention.

---

## Roadmap Snapshot

| Phase | Focus | Status |
| --- | --- | --- |
| Phase 1 | Storage engine, MVCC, merge, backups | ✅ |
| Phase 2 | Raft HA, gRPC transport, membership, linearizable reads | 🛠️ (current) |
| Phase 3 | Region sharding, PD service, distributed transactions | 🔜 |
| Phase 4 | SQL compatibility, coprocessor pushdown, operator & observability | 🔜 |

Near-term TODOs:
1. Transport address configuration (reuse gRPC address by default, documented).
2. Leader hint on NotLeader errors (implemented).
3. Read-transaction TTL cleaner (implemented).
4. Raft snapshot/export + log compaction.
5. CLI tooling & SQL gateway MVP.

---

## Contributing & Testing

```bash
# Run all tests (includes chaos integration suites)
go test ./...

# Run only the cluster chaos test
go test ./internal/cluster -run TestClusterChaosResilience -count=1
```

We welcome design discussions, issue reports, and contributions around SQL compatibility, PD orchestration, and observability tooling.

---

## License

This repository is released under the MIT License. See `LICENSE` for details.
