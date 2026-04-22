# Code Companion

This file maps the future Complete Engineering Guide sections to code locations.

| Guide Section | Code Location | Status |
|---|---|---|
| Foundation: API and data model | `kv-common`, `kv-storage-api` | Implemented for storage boundary |
| Foundation: durable single-node storage | `kv-storage-rocksdb` | Implemented with RocksDB |
| Foundation: token ring and vnodes | `kv-partitioning` | Implemented |
| Foundation: write/read path | `kv-replication` | In-process write fanout, retries, local-quorum filtering, and digest-read fanout implemented |
| Going Deeper: hinted handoff | `kv-repair` | Durable hint store, failed-write planner, and replay/backoff worker implemented |
| Going Deeper: digest reads and read repair | `kv-storage-api`, `kv-storage-rocksdb`, `kv-replication`, `kv-repair` | Digest read result analysis, read-repair planning, and write-boundary execution implemented |
| Going Deeper: Merkle anti-entropy | `kv-repair` | Planned |
| Going Deeper: tombstones and TTL | `kv-storage-api`, `kv-storage-rocksdb` | Implemented as stored-record metadata |
| At Scale: compaction debt | `kv-storage-rocksdb`, `kv-storage-toy-lsm`, `kv-bench` | RocksDB dependency in place; storage metrics planned |
| At Scale: deterministic simulation | `kv-simulator` | Planned |
| At Scale: local 5-node ring | `deploy/compose` | Planned |
| At Scale: GCP deployment | `deploy/gke` | Planned |

## Sync Rule

When the guide claims a mechanism exists, this companion must point to the file or test that implements it. If the code only simulates a production behavior locally, say so here and in the guide.

## Phase 1 Code Map

- `kv-storage-api/src/main/java/com/hkg/kv/storage/StorageEngine.java` defines `apply`, `get`, `digest`, and `close`.
- `kv-storage-api/src/main/java/com/hkg/kv/storage/StoredRecord.java` carries tombstone, timestamp, expiry, mutation id, and version-ordering behavior.
- `kv-storage-api/src/main/java/com/hkg/kv/storage/StoredRecord.java` also converts stored records back to `MutationRecord` for read repair without losing tombstone or TTL metadata.
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
- `kv-replication/src/main/java/com/hkg/kv/replication/ReplicaReader.java` abstracts transport-specific digest/full-record reads from one replica.
- `kv-replication/src/main/java/com/hkg/kv/replication/DigestReadCoordinator.java` fans one read to every planned replica and captures failed read responses.
- `kv-replication/src/main/java/com/hkg/kv/replication/DigestReadResult.java` reports successful responses, failed responses, digest agreement, and digest mismatches.
- `kv-replication/src/test/java/com/hkg/kv/replication/DigestReadCoordinatorTest.java` verifies digest agreement, mismatch detection, exception capture, and wrong-node response handling.

## Phase 4 Primitive Map

- `kv-repair/src/main/java/com/hkg/kv/repair/HintRecord.java` models a durable hinted-handoff record for a failed target replica.
- `kv-repair/src/main/java/com/hkg/kv/repair/HintStore.java` defines the persistence boundary for pending hints.
- `kv-repair/src/main/java/com/hkg/kv/repair/FileHintStore.java` persists hints as line-delimited records and rewrites the file on delivery/removal.
- `kv-repair/src/main/java/com/hkg/kv/repair/HintedHandoffService.java` creates, lists, and marks hints delivered.
- `kv-repair/src/main/java/com/hkg/kv/repair/HintedHandoffPlanner.java` records durable hints for failed replica responses from a replication result.
- `kv-repair/src/main/java/com/hkg/kv/repair/HintReplayPolicy.java` computes retry backoff for failed hint replay attempts.
- `kv-repair/src/main/java/com/hkg/kv/repair/HintReplayWorker.java` replays due hints, deletes delivered hints, and reschedules failed hints.
- `kv-repair/src/main/java/com/hkg/kv/repair/ReadRepairPlanner.java` selects the newest successful read record and targets stale successful replicas for repair.
- `kv-repair/src/main/java/com/hkg/kv/repair/ReadRepairExecutor.java` applies a read-repair plan by writing the latest returned record to stale target replicas through `ReplicaWriter`.
- `kv-repair/src/main/java/com/hkg/kv/repair/ConvergenceMetricsCollector.java` snapshots pending hint backlog shape, hint replay outcomes, and read repair outcomes.
- `kv-repair/src/test/java/com/hkg/kv/repair/FileHintStoreTest.java` verifies persistence across instances, delivery removal, and failed-attempt metadata replacement.
- `kv-repair/src/test/java/com/hkg/kv/repair/HintedHandoffPlannerTest.java` verifies failed replica hint creation and rejects responses outside the replication plan.
- `kv-repair/src/test/java/com/hkg/kv/repair/HintReplayWorkerTest.java` verifies delivered hint deletion, failed hint rescheduling, not-due skipping, and delivery exception handling.
- `kv-repair/src/test/java/com/hkg/kv/repair/ReadRepairPlannerTest.java` verifies empty repair plans, stale target selection, tombstone-as-latest behavior, and no-record handling.
- `kv-repair/src/test/java/com/hkg/kv/repair/ReadRepairExecutorTest.java` verifies successful repair writes, missing target handling, and exception capture.
- `kv-repair/src/test/java/com/hkg/kv/repair/ConvergenceMetricsCollectorTest.java` verifies hint backlog shape plus replay/read-repair outcome counters.
