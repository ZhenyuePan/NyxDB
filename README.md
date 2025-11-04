# NyxDB

English | [简体中文](README.zh-CN.md)

NyxDB is a strongly consistent key–value database that packages a Raft-based cluster, a multi-version Bitcask engine, and operational tooling into a single Go project. It is designed as an experimental data platform skeleton: bring a binary online in seconds, grow it into a multi-region deployment, and iterate on high-level features such as transactions or scheduling without rebuilding the plumbing.

---

## Why NyxDB?
- **Linearizable semantics by default** – every write is replicated through etcd/raft and acknowledged only after a quorum persists it.
- **Snapshot-aware storage** – the embedded MVCC engine supports fast point-in-time reads and log truncation via manual or automatic snapshots.
- **Operational guardrails** – health probes, membership management, and disk-backed metadata allow clusters to restart cleanly without external coordination.
- **Batteries included CLI** – a single `nyxdb-cli` handles KV mutations, admin operations, and snapshot triggers with transparent leader redirection.
- **Optional control plane** – wire up the PD prototype to experiment with store/region heartbeats, scheduling decisions, and observability when you need them.

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
- **Cluster manager (`internal/cluster`)** – wraps Raft, coordinates proposals, manages membership, and orchestrates snapshots.
- **Raft layer (`internal/layers/raft`)** – etcd/raft integration with custom storage, transport abstractions, and ReadIndex helpers.
- **Storage engine (`internal/layers/engine`)** – Bitcask-inspired append-only files with MVCC metadata, skiplist/B-tree/ART indexes, and optional group commit.
- **Transaction experiments (`internal/layers/txn`)** – Percolator-style MVCC prototypes and replication appliers.
- **Control plane (`internal/layers/pd`)** – lightweight scheduler prototype backed by BoltDB.
- **Server & CLI (`cmd/nyxdb-server`, `cmd/nyxdb-cli`)** – gRPC services, health reporting, and operational commands.

Additional design notes live in `docs/architecture.mdx` and `docs/development_status.md`.

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
