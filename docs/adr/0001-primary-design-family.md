# ADR 0001: Primary Design Family

## Status

Accepted for initial implementation.

## Decision

Build a Dynamo/Cassandra-style leaderless AP key-value store as the primary artifact.

## Context

The topic could be implemented as:
- A leaderless AP store with tunable consistency and convergence mechanisms.
- A CP Raft-backed key-value store.
- A transactional FoundationDB-style key-value layer.

These are different products, not configuration variants of one design.

## Rationale

The project is intended to explain the mechanisms behind distributed key-value stores at depth. The Dynamo/Cassandra family exposes the richest set of mechanisms for this guide: token rings, vnodes, tunable quorum, sloppy quorum, hinted handoff, digest reads, read repair, Merkle repair, tombstones, LSM storage, and operational convergence debt.

## Consequences

- Raft and FoundationDB remain contrast sections and future extension paths.
- The data path does not require a consensus group per shard.
- Correctness depends on convergence, repair, versioning, tombstone safety, and clear consistency-level semantics.
