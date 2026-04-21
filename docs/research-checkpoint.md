# Research Checkpoint

## Direction

Build the flagship artifact as a Dynamo/Cassandra-style distributed key-value store:
- Leaderless coordinator-per-request data path.
- Tunable consistency by operation.
- Sloppy quorum and hinted handoff for availability under transient failure.
- Digest reads, read repair, and Merkle anti-entropy for convergence.
- LSM-backed local storage abstraction.

Raft and FoundationDB-style designs remain explicit contrast points:
- Raft explains the CP replicated-state-machine alternative.
- FoundationDB explains the transactional KV alternative with MVCC, OCC, sequencers, resolvers, log servers, and deterministic simulation.

## Foundation

- Key-value API: `GET`, `PUT`, `DELETE`, TTL, and consistency level.
- Hash ring: token space, vnodes, physical-node distinctness.
- Replication: replication factor and preference list.
- Quorum vocabulary: `N`, `R`, `W`, `ONE`, `QUORUM`, `ALL`, `ANY`.
- Local storage: WAL, memtable, immutable SSTables, compaction, tombstones.

## Going Deeper

- Sloppy quorum weakens the clean strict-quorum overlap model during failure.
- Hinted handoff is best-effort convergence, not a replacement for repair.
- Digest reads detect mismatch without fetching every full value.
- Read repair improves freshness but must be explicit about blocking vs async behavior.
- Merkle repair catches long-lived divergence.
- Tombstone retention must be tied to outage and repair cadence.
- LWW makes clocks part of correctness; vector clocks push reconciliation to the API.

## At Scale

- Compaction debt changes read p99 long before users can name the storage problem.
- Hot keys are not fixed by token rebalancing.
- Multi-region must be modeled as a product decision, not one global ring stretched across continents.
- GKE StatefulSets provide stable identity and volume attachment, but system-level durability still comes from replication and repair.
- Deterministic simulation is required to find rare interleaving bugs.

## Recommended Defaults

- Conflict mode: LWW with hybrid logical timestamps first; vector clocks as an advanced mode.
- Key model: opaque KV first, internally ordered storage where useful.
- Consistency levels: `ONE`, `QUORUM`, `ALL`, `ANY`; add `LOCAL_QUORUM` after datacenter metadata exists.
- Vnodes: conservative initial default, such as 16 or 32 per node.
- Repair: explicit SLA before tombstone garbage collection.
- Multi-region: region-local quorum plus async cross-region convergence first.
