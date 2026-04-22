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
- Implemented token ranges with wraparound/full-ring semantics for repair windows.
- Implemented vnode ownership with deterministic vnode tokens per physical node.
- Built replica preference lists that walk clockwise and select distinct physical nodes.
- Added ring epoch metadata through immutable `PartitionRingSnapshot`.
- Added tests for deterministic placement, duplicate physical-owner filtering, wraparound, epoch preservation, invalid inputs, and limited remapping after removal.

## Phase 3: Leaderless Replication - In Progress

- Implemented in-process coordinator write fanout to all planned replicas.
- Implemented digest-read fanout to all planned replicas.
- Added wait-policy math for `ONE`, `QUORUM`, `ALL`, `ANY`, and `LOCAL_QUORUM`.
- Captured replica writer failures as failed responses so one failed replica does not abort fanout.
- Added configurable retry attempts and per-replica attempt accounting.
- Added local-datacenter-aware acknowledgement filtering for `LOCAL_QUORUM`.
- Mutation records already carry idempotent mutation IDs; retry deduplication across remote nodes remains planned.
- Add network transport and node-runtime integration.
- Add timeout budgets.

## Phase 4: Convergence - In Progress

- Added file-backed hinted-handoff records and a planner that records failed replica writes as durable hints.
- Added hint replay worker with exponential backoff scheduling.
- Added digest read result analysis, read-repair planning, and read-repair execution through the write boundary.
- Added convergence metrics snapshots for hint backlog, hint replay outcomes, and read repair outcomes.
- Added deterministic Merkle tree construction over token-range record digests.
- Added Merkle repair planning that compares replica trees and returns differing leaf ranges.
- Add range scan/streaming and repair execution for Merkle differences.
- Add metrics export, alerting, and repair lag/dropped-hint counters.

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
