# Code Companion

This file maps the future Complete Engineering Guide sections to code locations.

| Guide Section | Code Location | Status |
|---|---|---|
| Foundation: API and data model | `kv-common`, `kv-storage-api` | Implemented for storage boundary |
| Foundation: durable single-node storage | `kv-storage-rocksdb` | Implemented with RocksDB |
| Foundation: token ring and vnodes | `kv-partitioning` | Implemented |
| Foundation: write/read path | `kv-replication` | In-process write fanout, retries, and local-quorum filtering implemented; read path planned |
| Going Deeper: hinted handoff | `kv-repair` | Durable hint store and failed-write planner implemented; replay worker planned |
| Going Deeper: digest reads and read repair | `kv-storage-api`, `kv-storage-rocksdb`, `kv-replication`, `kv-repair` | Digest primitive implemented; read repair planned |
| Going Deeper: Merkle anti-entropy | `kv-repair` | Planned |
| Going Deeper: tombstones and TTL | `kv-storage-api`, `kv-storage-rocksdb` | Implemented as stored-record metadata |
| At Scale: compaction debt | `kv-storage-rocksdb`, `kv-storage-toy-lsm`, `kv-bench` | RocksDB dependency in place; metrics planned |
| At Scale: deterministic simulation | `kv-simulator` | Planned |
| At Scale: local 5-node ring | `deploy/compose` | Planned |
| At Scale: GCP deployment | `deploy/gke` | Planned |

## Sync Rule

When the guide claims a mechanism exists, this companion must point to the file or test that implements it. If the code only simulates a production behavior locally, say so here and in the guide.

## Phase 1 Code Map

- `kv-storage-api/src/main/java/com/hkg/kv/storage/StorageEngine.java` defines `apply`, `get`, `digest`, and `close`.
- `kv-storage-api/src/main/java/com/hkg/kv/storage/StoredRecord.java` carries tombstone, timestamp, expiry, mutation id, and version-ordering behavior.
- `kv-storage-rocksdb/src/main/java/com/hkg/kv/storage/rocksdb/RocksDbStorageEngine.java` persists records in RocksDB and ignores older late-arriving mutations.
- `kv-storage-rocksdb/src/main/java/com/hkg/kv/storage/rocksdb/StoredRecordCodec.java` serializes stored records.
- `kv-storage-rocksdb/src/test/java/com/hkg/kv/storage/rocksdb/RocksDbStorageEngineTest.java` verifies persistence, tombstones, TTL metadata, stale mutation protection, and digest changes.

## Phase 2 Code Map

- `kv-partitioning/src/main/java/com/hkg/kv/partitioning/KeyTokenHasher.java` hashes keys and vnode identities into 64-bit tokens using SHA-256.
- `kv-partitioning/src/main/java/com/hkg/kv/partitioning/Vnode.java` models one virtual-node token owned by a physical node.
- `kv-partitioning/src/main/java/com/hkg/kv/partitioning/PartitionRingSnapshot.java` stores immutable ring epoch metadata and performs clockwise distinct-owner replica selection.
- `kv-partitioning/src/main/java/com/hkg/kv/partitioning/ConsistentHashReplicaPlacementPolicy.java` exposes the replica-placement policy used by future coordinators.
- `kv-partitioning/src/test/java/com/hkg/kv/partitioning/ConsistentHashReplicaPlacementPolicyTest.java` verifies deterministic placement, distinct owners, wraparound, epoch metadata, invalid inputs, and bounded remapping.

## Phase 3 Primitive Map

- `kv-replication/src/main/java/com/hkg/kv/replication/ConsistencyWaitPolicy.java` computes acknowledgement thresholds for `ONE`, `QUORUM`, `ALL`, `ANY`, and `LOCAL_QUORUM`.
- `kv-replication/src/test/java/com/hkg/kv/replication/ConsistencyWaitPolicyTest.java` verifies N=3/N=5 quorum math and local-quorum validation.
- `kv-replication/src/main/java/com/hkg/kv/replication/ReplicaWriter.java` abstracts transport-specific mutation delivery to one replica.
- `kv-replication/src/main/java/com/hkg/kv/replication/ReplicationOptions.java` configures local-datacenter scoping and per-replica max attempts.
- `kv-replication/src/main/java/com/hkg/kv/replication/ReplicationCoordinator.java` fans one mutation out to every planned replica, retries failed replica writes, and evaluates consistency without depending on HTTP/gRPC.
- `kv-replication/src/main/java/com/hkg/kv/replication/ReplicationResult.java` exposes ordered responses, failed responses, acknowledgement count, total attempts, wait policy, and final consistency satisfaction.
- `kv-replication/src/test/java/com/hkg/kv/replication/ReplicationCoordinatorTest.java` verifies quorum success, `ALL` failure, fanout after early quorum, key mismatch rejection, exception capture, retries, and local-datacenter quorum filtering.

## Phase 4 Primitive Map

- `kv-repair/src/main/java/com/hkg/kv/repair/HintRecord.java` models a durable hinted-handoff record for a failed target replica.
- `kv-repair/src/main/java/com/hkg/kv/repair/HintStore.java` defines the persistence boundary for pending hints.
- `kv-repair/src/main/java/com/hkg/kv/repair/FileHintStore.java` persists hints as line-delimited records and rewrites the file on delivery/removal.
- `kv-repair/src/main/java/com/hkg/kv/repair/HintedHandoffService.java` creates, lists, and marks hints delivered.
- `kv-repair/src/main/java/com/hkg/kv/repair/HintedHandoffPlanner.java` records durable hints for failed replica responses from a replication result.
- `kv-repair/src/test/java/com/hkg/kv/repair/FileHintStoreTest.java` verifies persistence across instances, delivery removal, and failed-attempt metadata replacement.
- `kv-repair/src/test/java/com/hkg/kv/repair/HintedHandoffPlannerTest.java` verifies failed replica hint creation and rejects responses outside the replication plan.
