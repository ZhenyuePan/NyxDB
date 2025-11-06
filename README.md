# NyxDB

English | [简体中文](README.zh-CN.md)

```
N   N  Y   Y  X   X  DDDD   BBBB 
NN  N   Y Y    X X   D   D  B   B
N N N    Y      X    D   D  BBBB 
N  NN    Y     X X   D   D  B   B
N   N    Y    X   X  DDDD   BBBB 
```

NyxDB is an embeddable, trim, strongly consistent key‑value store. It uses etcd/raft for linearizable replication and a Bitcask‑style append‑only engine with MVCC for snapshot‑friendly durability and point‑in‑time recovery. A single‑port gRPC surface (KV/Admin/Raft) with zero hard external deps lets you spin up on a laptop or a 3‑node cluster in seconds. For different workloads, NyxDB offers pluggable indexes and tunable write durability/latency knobs (Group Commit, BytesPerSync, SyncWrites), and can optionally attach a PD control plane for multi‑region management and scheduling. It fits microservice embedding, edge/private small clusters, platform metadata, and job/checkpoint state that demand consistency and recoverability.

---

## Why NyxDB?
- **Linearizable semantics by default** – every write is replicated through etcd/raft and acknowledged only after a quorum persists it.
- **Snapshot-aware storage** – the embedded MVCC engine supports fast point-in-time reads and log truncation via manual or automatic snapshots.
- **Operational guardrails** – health probes, membership management, and disk-backed metadata allow clusters to restart cleanly without external coordination.
- **Batteries included CLI** – a single `nyxdb-cli` handles KV mutations, admin operations, and snapshot triggers with transparent leader redirection.
- **Optional control plane** – wire up the PD prototype to experiment with store/region heartbeats, scheduling decisions, and observability when you need them.

## Relation to etcd
- Similarities:
  - Majority replication via etcd/raft; linearizable reads/writes;
  - MVCC semantics for snapshot reads and point-in-time visibility;
  - Snapshots/truncation and membership management.
- Differences (NyxDB’s focus):
  - Storage layout: Bitcask‑style append‑only with per‑key version chains (`CommitTs + Prev*`) for simpler, predictable I/O;
  - Write control: first‑class group commit (`MaxBatchEntries/MaxBatchBytes/MaxWait`) and `BytesPerSync/SyncWrites` knobs to balance latency vs durability;
  - Pluggable indexes: B‑Tree/SkipList/ART/Sharded‑BTree selectable per workload;
  - Embeddable: single‑port gRPC (KV/Admin/RaftTransport) with light deps, suited to in‑process use or small/edge clusters;
  - Optional control plane: PD is optional; the system is not positioned as a “big control plane” by default.
- Prefer NyxDB when:
  - You need an embeddable, trim strong‑consistency KV base;
  - You want fine‑grained control over write durability/latency (group commit, sync policy, index choices);
  - You target small clusters/edge/local‑first scenarios.
- Prefer etcd when:
  - You need a battle‑tested general‑purpose control‑plane KV with a mature ecosystem and full features (watch/leases, etc.).

---

## Feature Overview
| Capability | Details |
| --- | --- |
| Consistency | Raft replication, linearizable reads via ReadIndex + WaitApplied |
| Storage | Append-only Bitcask layout with MVCC metadata, per-key version chains |
| Snapshots | Manual trigger via CLI, auto snapshots governed by interval and entry thresholds |
| Membership | Join/Leave, persisted membership list, automatic leader election on restart |
| Transport | gRPC streaming transport for Raft messages plus built-in health RPC |
| Observability | Optional Prometheus `/metrics`, diagnostics logging toggle |
| Tooling | CLI for KV and admin, example configs for single-node or multi-node labs |

---

## Quickstart

### Prerequisites
- Go 1.21 or newer.
- `protoc` only required if you regenerate gRPC bindings (`scripts/genproto.sh`).

### Run a single node
```bash
go run cmd/nyxdb-server/main.go -config configs/server.single.yaml
```

### Interact with the cluster
```bash
# linearizable write/read
go run cmd/nyxdb-cli/main.go kv put --addr 127.0.0.1:10001 --key greeting --value hello
go run cmd/nyxdb-cli/main.go kv get --addr 127.0.0.1:10001 --key greeting

# optional snapshot
go run cmd/nyxdb-cli/main.go admin snapshot --addr 127.0.0.1:10001 --force
```

To simulate multiple replicas, duplicate the provided config, assign unique data directories and `grpc.address` values, then launch each node with `go run` or `go build`.

### Optional services
- **PD prototype** (control plane):
  ```bash
  go run cmd/nyxdb-pd/main.go -addr 0.0.0.0:18080 -data /tmp/nyxdb-pd
  ```
  Fill `pd.address` in the server config to enable store/region heartbeats.
- **Prometheus metrics**: set `observability.metricsAddress` to e.g. `127.0.0.1:2112` to expose `/metrics`.

---

## Architecture at a Glance
- **Cluster manager (`internal/cluster`)** – wraps the etcd/raft node lifecycle (Start/Ready/Apply), drives proposals and linearizable reads (ReadIndex + WaitApplied), manages membership and regions, orchestrates snapshots, and serves as the coordination entry for KV/Admin.
- **Raft layer (`internal/layers/raft`)**:
  - `node`: integrates `raft.Node`, bridging storage and transport;
  - `storage`: implements `raft.Storage` with log/snapshot persistence;
  - `transport`: provides gRPC transport and a local Noop transport. The gRPC transport uses `pkg/api.RaftTransport` and is registered on the same gRPC server port.
- **Storage engine (`internal/layers/engine`)** – Bitcask‑style (append‑only, not LSM). MVCC uses per‑key version chains (`CommitTs + PrevFid/PrevOffset`). Indexes: B‑Tree, SkipList, ART, and Sharded B‑Tree (default). Group commit (`MaxBatchEntries/MaxBatchBytes/MaxWait`), `BytesPerSync`, `SyncWrites`. Startup can use MMap; writes use standard file IO.
- **Transactions & replication (`internal/layers/txn`)** – `replication` safely applies raft‑committed ops to the engine; `percolator` hosts local MVCC/transaction experiments (not a distributed transaction layer).
- **Control plane (`internal/layers/pd`)** – in‑memory PD prototype with optional BoltDB persistence for region metadata; includes gRPC client/server.
- **Server & CLI (`cmd/nyxdb-server`, `cmd/nyxdb-cli`)** – a single gRPC port hosts KV/Admin and RaftTransport with built‑in health checks; optional observability under `internal/layers/observability/*`.

### ASCII Architecture

```
            +-------------------+                +----------------------+
            |  Clients / CLI    |  gRPC (KV/Admin)|      PD (optional)  |
            |  (nyxdb-cli)      |<---------------->|  store/region meta  |
            +---------^---------+                +----------^-----------+
                      |                                   |
                      | gRPC (one port)                   |
              +-------+--------+                          |
              |   gRPC Server  |  registers              |
              | (health+KV+PD) |<------------------------+
              +-------+--------+
                      |
                      v
              +-------+--------+     Raft msgs     +--------------------+
              |  Cluster Mgr   |<----------------->|  Raft Transport    |
              | (ReadIndex,    |                    |  (gRPC/Noop)      |
              |  snapshots,    |                    +--------------------+
              |  membership)   |
              +-------+--------+
                      |
                      v
              +-------+--------+
              |    Raft Node   |
              +-------+--------+
                      |
                      v
              +-------+--------+        +-----------------+
              |   Engine       |<------>|  In‑mem Index   |
              | (Bitcask+MVCC) |        | (BTree/ART/...) |
              +-------+--------+        +-----------------+
                      |
                      v
              +-----------------+   files   +-----------------+
              |  Data Files     |<--------->|   Hint / Merge  |
              +-----------------+           +-----------------+
```

Additional design notes live in `docs/architecture.mdx`, `docs/development_status.md`, and the engine docs under `docs/engine/`.

---

## Benchmarks
Local measurements (Intel i7-14700, Go 1.21, Ubuntu) provide a baseline for experimentation. Full results are stored under `test_result/`.

| Scenario | Throughput / Latency |
| --- | --- |
| Engine PUT (1 KB value, no fsync) | ~183K ops/s · 6.8 µs/op [[details]](test_result/benchmark.txt) |
| Engine GET (hot key) | ~4.1M ops/s · 287 ns/op [[details]](test_result/benchmark.txt) |
| Cluster PUT (single region) | ~330 ops/s · 3.6 ms/op [[details]](test_result/benchmark_cluster.txt) |
| Cluster GET (prefilled) | ~680K ops/s · 1.7 µs/op [[details]](test_result/benchmark_cluster.txt) |

> Numbers emphasize baseline behaviour; enabling fsync, networking, or PD will change outcomes. Use the bundled `go test -bench` suites under `benchmark/` to reproduce and extend measurements.

---

## Roadmap
- **Near term**
  - Harden Raft lifecycle: chunked snapshot streaming, leader transfer, backpressure.
  - Expand observability: richer CLI inspection, metrics dashboards, tracing hooks.
  - Optimize engine writes: reduce encode allocations, optional zero-copy paths.
- **Mid term**
  - Multi-region routing with PD-driven placement.
  - Automated rebalancing: store/region scheduling, split/merge workflows.
  - Read leases and follower reads for low-latency workloads.
- **Long term**
  - Distributed transactions (Percolator-style), lock conflict handling, recovery tooling.
  - SQL or document-layer prototypes atop the KV API.

Progress and open discussions live in `docs/development_status.md`; contributions and ideas are welcome.

---

## Repository Layout
- `cmd/nyxdb-server` – main server binary.
- `cmd/nyxdb-cli` – CLI for KV/admin operations.
- `internal/` – engine, Raft integration, cluster manager, PD prototype.
- `benchmark/` – micro and cluster benchmarks.
- `configs/` – ready-to-use server configuration examples.
- `docs/` – architecture notes, development status, deep dives.
- `examples/` – small programs demonstrating MVCC reads and client usage.
- `scripts/` – tooling (protobuf generation, bench helpers).

---

## Contributing
NyxDB is rapidly evolving. Bug reports, benchmark results, and design discussions are all valuable. Feel free to open an issue or PR, or reach out with ideas for extending the control plane, storage engine, or transaction layer.
