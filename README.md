# Distributed Key-Value Store

A Java implementation companion for the CSE wiki and raw blog series on distributed key-value stores.

The project target is a Dynamo/Cassandra-style leaderless AP store with tunable consistency, replica convergence, and an LSM-backed local storage abstraction. Raft and FoundationDB-style designs are treated as contrast points, not the main data path.

## Current Scope

This repository is at checkpoint 19: durable single-node storage, Phase 2 partitioning, bounded Phase 3 write/read replication primitives, and Phase 4 convergence primitives for hinted handoff, read repair execution, Merkle anti-entropy repair execution, repair backpressure, convergence metric export, deterministic Merkle repair scheduling, concrete HTTP transport for replica/repair flows, durable repair leases, node-side lease backend wiring, an embedded HTTP node runtime, ring-driven coordinator planning, and coordinator-side durable hint recording.

Implemented:
- `StorageEngine` contract.
- RocksDB-backed storage engine.
- Stored-record serialization.
- Tombstone records for deletes.
- TTL expiry metadata.
- LWW-style version ordering by timestamp and mutation id.
- SHA-256 digest support for future digest reads.
- Unit tests for persistence, tombstones, TTL, late stale mutation handling, and digest changes.
- Deterministic SHA-256 token hashing for keys and vnodes.
- Immutable ring snapshots with epoch metadata.
- Clockwise replica placement that skips duplicate physical owners.
- Consistency wait-policy math for `ONE`, `QUORUM`, `ALL`, `ANY`, and `LOCAL_QUORUM`.
- Transport-agnostic write replication coordinator that fans out to all planned replicas, captures per-replica failures, and evaluates the requested consistency level.
- Per-replica retry attempt accounting with configurable max attempts.
- Local-datacenter-aware `LOCAL_QUORUM` acknowledgement filtering.
- File-backed hinted-handoff store plus planner for recording failed replica writes as durable hints.
- Hint replay worker with exponential backoff scheduling and delivery removal.
- Digest read coordinator that fans reads to planned replicas and identifies digest disagreement.
- Read repair planner that selects the newest returned record and targets stale successful replicas.
- Read repair executor that applies the latest returned record through the replica write boundary.
- Convergence metrics snapshot for pending hints, hint replay outcomes, and read repair outcomes.
- Token range model for ring-aware repair windows.
- Deterministic Merkle tree builder over token-range record digests.
- Merkle repair planner that compares two replica trees and returns only differing leaf ranges.
- Storage scan primitive for repair jobs.
- Storage-backed Merkle range scanner for tree construction.
- Local Merkle repair executor that applies missing or stale records across two storage engines.
- Merkle repair budget that caps ranges, scanned records, and write attempts per run.
- Dependency-free convergence metric samples and exporter boundary for hint, read-repair, and Merkle-repair outcomes.
- Merkle repair scheduler that runs due replica-pair range tasks, limits tasks per tick, reschedules clean/incomplete runs, captures missing-replica/failed-task outcomes, and returns aggregate repair results.
- Transport-agnostic Merkle range streaming boundary and remote repair executor that streams range records from replica nodes and writes repair mutations through `ReplicaWriter`.
- Merkle repair lease contract, in-memory lease store with fencing tokens, and lease-guarded scheduler that skips tasks already owned by another worker.
- JDBC-backed Merkle repair lease store with PostgreSQL-shaped schema initialization, row-level locking, active/inactive lease rows, and monotonic per-task fencing tokens across release/reacquire cycles.
- `kv-node` repair lease backend configuration and factory wiring for `in-memory` or `jdbc` lease storage.
- Concrete HTTP client/handler transport in `kv-node` for replica writes, digest/full reads, and streamed Merkle range repair using binary request/response payloads over JDK `HttpClient` and `HttpHandler`.
- Property-backed embedded `kv-node` runtime that opens RocksDB storage, starts a JDK `HttpServer`, registers fixed replica/repair endpoints, returns the actual bound `ClusterNode`, and wires the selected repair lease backend into one lifecycle.
- Coordinator config, replica planners, service, client, and HTTP handlers in `kv-node` that route external write/read requests across configured cluster nodes, optionally derive per-key replica plans from the consistent-hash ring, persist durable hints for failed planned replicas, allow `ANY` writes to succeed via coordinator-side hint recording, and run opportunistic read repair on digest mismatch.

Hint replay scheduling in the runtime, repair tick orchestration, Micrometer/Prometheus binding, repair task persistence, Docker Compose/GKE runtime packaging, and a gRPC alternative come next.

Node runtime properties:

| Property | Default | Meaning |
|---|---|---|
| `kv.node.id` | none | Stable node identifier |
| `kv.node.host` | `127.0.0.1` | HTTP bind and advertised host |
| `kv.node.port` | `8080` | HTTP bind port; use `0` for an ephemeral test port |
| `kv.node.storage.rocksdb.path` | none | Local RocksDB directory for this node |
| `kv.node.http.request-timeout` | `PT5S` | Outbound replica/repair HTTP timeout |

Coordinator routing properties:

| Property | Default | Meaning |
|---|---|---|
| `kv.node.coordinator.node-count` | `0` | Number of configured cluster-node entries; `0` means local-node-only coordinator routing |
| `kv.node.coordinator.nodes.<i>.node-id` | none | Cluster node identifier for entry `i` |
| `kv.node.coordinator.nodes.<i>.self` | `false` | Whether entry `i` should resolve to the local bound node |
| `kv.node.coordinator.nodes.<i>.host` | none | Remote host for non-self entries |
| `kv.node.coordinator.nodes.<i>.port` | none | Remote port for non-self entries |
| `kv.node.coordinator.nodes.<i>.datacenter` | none | Datacenter label used for `LOCAL_QUORUM` counting |
| `kv.node.coordinator.replication-factor` | unset | When set, enable ring-driven per-key placement over the configured cluster nodes |
| `kv.node.coordinator.vnode-count` | `32` | Vnodes per physical node for ring-driven coordinator planning |
| `kv.node.coordinator.ring-epoch` | `1` | Ring snapshot epoch metadata for ring-driven coordinator planning |
| `kv.node.coordinator.local-datacenter` | none | Local datacenter name required for `LOCAL_QUORUM` coordinator requests |
| `kv.node.coordinator.max-attempts` | `1` | Per-replica retry attempts for coordinator writes |
| `kv.node.coordinator.read-repair-enabled` | `true` | Whether digest mismatch triggers coordinator-side read repair |
| `kv.node.coordinator.hinted-handoff-enabled` | `true` | Whether failed planned replica writes should be persisted as coordinator-side hints |
| `kv.node.coordinator.hint-store-path` | `<rocksdb>/coordinator-hints.log` | Override for the durable hint file used by the embedded node runtime |

Legacy aliases `kv.node.coordinator.replica-count` and `kv.node.coordinator.replicas.<i>.*` are still accepted for compatibility.

Repair lease backend properties:

| Property | Default | Meaning |
|---|---|---|
| `kv.repair.lease.backend` | `in-memory` | `in-memory` for local simulation or `jdbc` for durable PostgreSQL-backed leases |
| `kv.repair.lease.jdbc.url` | none | JDBC URL required when backend is `jdbc` |
| `kv.repair.lease.jdbc.username` | none | Optional JDBC username |
| `kv.repair.lease.jdbc.password` | empty | Optional JDBC password |
| `kv.repair.lease.jdbc.table` | `kv_merkle_repair_leases` | Lease table name |
| `kv.repair.lease.jdbc.initialize-schema` | `true` for JDBC | Whether the node should create the lease table on startup |

## Planned Local Runtime

The local runtime will use Docker Compose with a 5-node ring, stable node IDs, deterministic token assignments, a client/admin container, and failure-injection support.

Local scope should exercise:
- Token ring and vnode placement.
- `ONE`, `QUORUM`, `ALL`, and `ANY` consistency levels.
- Hinted handoff.
- Digest reads and read repair.
- Merkle-tree anti-entropy.
- Tombstones, TTL, and repair cadence.
- Bootstrap, decommission, and replacement flows.

## Planned GCP Runtime

The scaled deployment target is a regional GKE cluster:
- One StatefulSet for KV nodes.
- Headless Service for internode identity.
- One PVC per KV node.
- Zone labels treated as rack placement hints.
- Region labels treated as datacenter placement hints for future multi-region work.

## Modules

- `kv-common`: shared value types and consistency-level vocabulary.
- `kv-storage-api`: storage engine boundary and stored-record model.
- `kv-storage-rocksdb`: production-path local storage implementation using RocksDB.
- `kv-storage-toy-lsm`: educational LSM implementation for explaining WAL, memtable, SSTable, and compaction.
- `kv-membership`: gossip, health, and cluster metadata.
- `kv-partitioning`: token ring, vnodes, ring epochs, and replica placement.
- `kv-replication`: coordinator read/write flow and consistency-level wait policies.
- `kv-repair`: hinted handoff, read repair, Merkle repair, and tombstone safety.
- `kv-node`: embedded node runtime, HTTP transport adapters, and runtime wiring.
- `kv-client`: client library and CLI-facing API.
- `kv-admin`: ring/admin operations.
- `kv-simulator`: deterministic failure simulation.
- `kv-bench`: benchmark and load-generation tools.

## Wiki Links

- CSE source page: `wiki/sources/distributed-key-value-store-deep-research.md`
- Existing concept pages: consistent hashing, quorum, LSM tree, hinted handoff, Merkle tree, gossip protocol, vector clocks.
- Existing system pages: Amazon Dynamo, Cassandra, Bigtable, FoundationDB.
