# Implementation Plan

## Phase 1: Durable Single-Node Core - Done

- Defined key, value, mutation, stored-record, and consistency-level types.
- Implemented `StorageEngine` contract.
- Added RocksDB-backed storage as the primary path.
- Added tombstone and TTL representation.
- Added SHA-256 digest support for future digest reads.
- Added LWW-style version ordering by timestamp and mutation id.
- Added unit tests for put/get/delete/tombstone/TTL/persistence/digest semantics.

## Phase 2: Partitioning - Done

- Implemented deterministic SHA-256-derived token hashing.
- Implemented vnode ownership with deterministic vnode tokens per physical node.
- Built replica preference lists that walk clockwise and select distinct physical nodes.
- Added ring epoch metadata through immutable `PartitionRingSnapshot`.
- Added tests for deterministic placement, duplicate physical-owner filtering, wraparound, epoch preservation, invalid inputs, and limited remapping after removal.

## Phase 3: Leaderless Replication - In Progress

- Implemented in-process coordinator write fanout to all planned replicas.
- Added wait-policy math for `ONE`, `QUORUM`, `ALL`, `ANY`, and `LOCAL_QUORUM`.
- Captured replica writer failures as failed responses so one failed replica does not abort fanout.
- Add idempotent mutation IDs and retry handling.
- Add network transport and node-runtime integration.
- Add durable hints for `ANY` and transient replica failure.
- Implement read fanout and basic reconciliation.

## Phase 4: Convergence

- Add durable hinted handoff.
- Add digest reads and mismatch escalation.
- Add read repair modes.
- Add Merkle tree repair per token range.
- Add metrics for hint backlog age, repair lag, and dropped hints.

## Phase 5: Topology Change

- Add bootstrap, decommission, and replace-node flows.
- Add range streaming.
- Prevent a joining node from serving an owned range until streaming and validation complete.

## Phase 6: Simulation And Benchmarks

- Add deterministic scheduler and fake network.
- Simulate partitions, crashes, delayed messages, duplicate messages, clock skew, compaction lag, and repair delay.
- Add load tests for consistency level latency, read amplification, hint backlog, and hot-key skew.

## Phase 7: Local And GCP Runtimes

- Add Docker Compose 5-node ring.
- Add GKE StatefulSet manifests.
- Add headless Service, PVC templates, zone-aware labels, and resource requests.
- Document local-vs-GCP behavioral differences.
