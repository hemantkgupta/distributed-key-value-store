# Distributed Key-Value Store

A Java implementation companion for the CSE wiki and raw blog series on distributed key-value stores.

The project target is a Dynamo/Cassandra-style leaderless AP store with tunable consistency, replica convergence, and an LSM-backed local storage abstraction. Raft and FoundationDB-style designs are treated as contrast points, not the main data path.

## Current Scope

This repository is at checkpoint 11: durable single-node storage, Phase 2 partitioning, bounded Phase 3 write/read replication primitives, and Phase 4 convergence primitives for hinted handoff, read repair execution, Merkle anti-entropy repair execution, repair backpressure, convergence metric export, and deterministic Merkle repair scheduling.

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

Transport, timeout budgets, remote Merkle streaming, distributed leases, Micrometer/Prometheus binding, and node-runtime integration come next.

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
- `kv-node`: node runtime.
- `kv-client`: client library and CLI-facing API.
- `kv-admin`: ring/admin operations.
- `kv-simulator`: deterministic failure simulation.
- `kv-bench`: benchmark and load-generation tools.

## Wiki Links

- CSE source page: `wiki/sources/distributed-key-value-store-deep-research.md`
- Existing concept pages: consistent hashing, quorum, LSM tree, hinted handoff, Merkle tree, gossip protocol, vector clocks.
- Existing system pages: Amazon Dynamo, Cassandra, Bigtable, FoundationDB.
