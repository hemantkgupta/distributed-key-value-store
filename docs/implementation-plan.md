# Implementation Plan

## Phase 1: Durable Single-Node Core

- Define key, value, mutation, stored-record, and consistency-level types.
- Implement `StorageEngine` contract.
- Add RocksDB-backed storage as the primary path.
- Add tombstone and TTL representation.
- Add unit tests for put/get/delete/tombstone semantics.

## Phase 2: Partitioning

- Implement token hashing and vnode ownership.
- Build replica preference lists that select distinct physical nodes.
- Add ring epoch metadata.
- Add tests for join, leave, duplicate physical owner filtering, and deterministic placement.

## Phase 3: Leaderless Replication

- Implement coordinator write fanout to all replicas.
- Add wait policies for `ONE`, `QUORUM`, `ALL`, and `ANY`.
- Add idempotent mutation IDs and retry handling.
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
