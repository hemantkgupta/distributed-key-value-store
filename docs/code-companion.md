# Code Companion

This file maps the future Complete Engineering Guide sections to code locations.

| Guide Section | Code Location | Status |
|---|---|---|
| Foundation: API and data model | `kv-common`, `kv-storage-api` | Implemented for storage boundary, including repair scans |
| Foundation: durable single-node storage | `kv-storage-rocksdb` | Implemented with RocksDB, including full local scans |
| Foundation: token ring and vnodes | `kv-partitioning` | Implemented, including token ranges for repair windows |
| Foundation: write/read path | `kv-replication`, `kv-node` | In-process write fanout, retries, local-quorum filtering, digest-read fanout, and concrete HTTP transport adapters implemented |
| Going Deeper: hinted handoff | `kv-repair` | Durable hint store, failed-write planner, replay/backoff worker, and metrics counters implemented |
| Going Deeper: digest reads and read repair | `kv-storage-api`, `kv-storage-rocksdb`, `kv-replication`, `kv-repair` | Digest read result analysis, read-repair planning, write-boundary execution, and metrics counters implemented |
| Going Deeper: Merkle anti-entropy | `kv-storage-api`, `kv-storage-rocksdb`, `kv-partitioning`, `kv-replication`, `kv-repair`, `kv-node` | Storage-backed tree construction, differing-range planning, local repair execution, per-run backpressure budgets, deterministic due-task scheduling, transport-agnostic range streaming, concrete HTTP range streaming, lease-guarded scheduling, JDBC durable lease backend, node-side backend selection, and metric samples implemented; full node lifecycle integration planned |
| Going Deeper: tombstones and TTL | `kv-storage-api`, `kv-storage-rocksdb` | Implemented as stored-record metadata |
| At Scale: compaction debt | `kv-storage-rocksdb`, `kv-storage-toy-lsm`, `kv-bench` | RocksDB dependency in place; storage metrics planned |
| At Scale: deterministic simulation | `kv-simulator` | Planned |
| At Scale: local 5-node ring | `deploy/compose` | Planned |
| At Scale: GCP deployment | `deploy/gke` | Planned |

## Sync Rule

When the guide claims a mechanism exists, this companion must point to the file or test that implements it. If the code only simulates a production behavior locally, say so here and in the guide.

## Phase 1 Code Map

- `kv-storage-api/src/main/java/com/hkg/kv/storage/StorageEngine.java` defines `apply`, `get`, `scanAll`, `digest`, and `close`.
- `kv-storage-api/src/main/java/com/hkg/kv/storage/StoredRecord.java` carries tombstone, timestamp, expiry, mutation id, and version-ordering behavior.
- `kv-storage-api/src/main/java/com/hkg/kv/storage/StoredRecord.java` also converts stored records back to `MutationRecord` for read repair without losing tombstone or TTL metadata.
- `kv-storage-rocksdb/src/main/java/com/hkg/kv/storage/rocksdb/RocksDbStorageEngine.java` persists records in RocksDB, scans stored records for repair jobs, and ignores older late-arriving mutations.
- `kv-storage-rocksdb/src/main/java/com/hkg/kv/storage/rocksdb/StoredRecordCodec.java` serializes stored records.
- `kv-storage-rocksdb/src/test/java/com/hkg/kv/storage/rocksdb/RocksDbStorageEngineTest.java` verifies persistence, tombstones, TTL metadata, stale mutation protection, scan coverage, and digest changes.

## Phase 2 Code Map

- `kv-partitioning/src/main/java/com/hkg/kv/partitioning/KeyTokenHasher.java` hashes keys and vnode identities into 64-bit tokens using SHA-256.
- `kv-partitioning/src/main/java/com/hkg/kv/partitioning/TokenRange.java` models start-exclusive/end-inclusive token ranges, wrapping ranges, full-ring ranges, and deterministic splits for repair.
- `kv-partitioning/src/main/java/com/hkg/kv/partitioning/Vnode.java` models one virtual-node token owned by a physical node.
- `kv-partitioning/src/main/java/com/hkg/kv/partitioning/PartitionRingSnapshot.java` stores immutable ring epoch metadata and performs clockwise distinct-owner replica selection.
- `kv-partitioning/src/main/java/com/hkg/kv/partitioning/ConsistentHashReplicaPlacementPolicy.java` exposes the replica-placement policy used by future coordinators.
- `kv-partitioning/src/test/java/com/hkg/kv/partitioning/ConsistentHashReplicaPlacementPolicyTest.java` verifies deterministic placement, distinct owners, wraparound, epoch metadata, invalid inputs, and bounded remapping.
- `kv-partitioning/src/test/java/com/hkg/kv/partitioning/TokenRangeTest.java` verifies non-wrapping, wrapping, full-ring, split, and too-small-to-split behavior.

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
- `kv-repair/src/main/java/com/hkg/kv/repair/ConvergenceMetricsCollector.java` snapshots pending hint backlog shape, hint replay outcomes, read repair outcomes, Merkle repair outcomes, and budget stops.
- `kv-repair/src/main/java/com/hkg/kv/repair/ConvergenceMetric.java` models one dependency-free metric sample with a stable name, value, and optional attributes.
- `kv-repair/src/main/java/com/hkg/kv/repair/ConvergenceMetricsExporter.java` defines the sink boundary for metrics export.
- `kv-repair/src/main/java/com/hkg/kv/repair/ConvergenceMetricsReporter.java` exports a `ConvergenceMetricsSnapshot` as stable `kv.repair.*` metric samples.
- `kv-repair/src/main/java/com/hkg/kv/repair/MerkleRecordDigest.java` represents a token, key, and record digest emitted by a future per-range scan.
- `kv-repair/src/main/java/com/hkg/kv/repair/MerkleTreeBuilder.java` builds deterministic SHA-256 Merkle trees over token-range record digests.
- `kv-repair/src/main/java/com/hkg/kv/repair/MerkleRepairPlanner.java` compares two same-range trees and returns the differing leaf ranges that need scan/stream repair.
- `kv-repair/src/main/java/com/hkg/kv/repair/MerkleRangeScanner.java` scans a storage engine, filters records by token range, and builds storage-backed Merkle trees.
- `kv-repair/src/main/java/com/hkg/kv/repair/MerkleRepairBudget.java` caps max ranges, scanned records, and write attempts in one repair run.
- `kv-repair/src/main/java/com/hkg/kv/repair/MerkleRepairRangeReconciler.java` contains shared missing/stale/equal-version reconciliation logic used by local and remote repair executors.
- `kv-repair/src/main/java/com/hkg/kv/repair/MerkleRepairExecutor.java` applies a repair plan locally by scanning differing ranges and writing the latest record or tombstone to the stale side.
- `kv-repair/src/main/java/com/hkg/kv/repair/MerkleRepairResult.java` reports scanned records, applied writes, failed writes, already-converged keys, skipped ranges, and budget stops.
- `kv-repair/src/main/java/com/hkg/kv/repair/MerkleRepairTask.java` models a scheduled replica-pair token-range repair task with next-run time and incomplete-run backoff state.
- `kv-repair/src/main/java/com/hkg/kv/repair/MerkleRepairSchedulePolicy.java` computes clean-run cadence and bounded exponential backoff for incomplete repair runs.
- `kv-repair/src/main/java/com/hkg/kv/repair/MerkleRepairScheduleBudget.java` caps how many due repair tasks a scheduler tick can attempt and passes a per-task Merkle repair budget to the executor.
- `kv-repair/src/main/java/com/hkg/kv/repair/MerkleRepairScheduler.java` runs due repair tasks against local storage handles, builds current Merkle trees, plans differences, executes bounded local repair, isolates failed tasks, and reschedules each task.
- `kv-repair/src/main/java/com/hkg/kv/repair/MerkleRepairScheduleSummary.java` reports per-task statuses, next tasks, aggregate repair results, and budget-stop counts.
- `kv-repair/src/main/java/com/hkg/kv/repair/MerkleRangeStreamer.java` abstracts transport-specific streaming of stored records for one token range from one `ClusterNode`.
- `kv-repair/src/main/java/com/hkg/kv/repair/RemoteMerkleRepairExecutor.java` repairs streamed replica ranges by comparing returned records and writing repair mutations through `ReplicaWriter`.
- `kv-repair/src/main/java/com/hkg/kv/repair/MerkleRepairLease.java` models ownership, fencing token, acquire time, and expiry for one repair task lease.
- `kv-repair/src/main/java/com/hkg/kv/repair/MerkleRepairLeaseStore.java` defines the lease backend boundary for acquiring, releasing, and inspecting repair task leases.
- `kv-repair/src/main/java/com/hkg/kv/repair/InMemoryMerkleRepairLeaseStore.java` provides a deterministic in-memory implementation of lease acquire/expiry/release semantics for tests and local simulation.
- `kv-repair/src/main/java/com/hkg/kv/repair/JdbcMerkleRepairLeaseStore.java` provides a PostgreSQL-shaped durable lease backend using JDBC transactions, row locks, active/inactive rows, and per-task fencing tokens.
- `kv-repair/src/main/java/com/hkg/kv/repair/LeasedMerkleRepairScheduler.java` wraps `MerkleRepairScheduler`, acquiring a lease before running a due task and releasing it after the attempt.
- `kv-repair/src/test/java/com/hkg/kv/repair/FileHintStoreTest.java` verifies persistence across instances, delivery removal, and failed-attempt metadata replacement.
- `kv-repair/src/test/java/com/hkg/kv/repair/HintedHandoffPlannerTest.java` verifies failed replica hint creation and rejects responses outside the replication plan.
- `kv-repair/src/test/java/com/hkg/kv/repair/HintReplayWorkerTest.java` verifies delivered hint deletion, failed hint rescheduling, not-due skipping, and delivery exception handling.
- `kv-repair/src/test/java/com/hkg/kv/repair/ReadRepairPlannerTest.java` verifies empty repair plans, stale target selection, tombstone-as-latest behavior, and no-record handling.
- `kv-repair/src/test/java/com/hkg/kv/repair/ReadRepairExecutorTest.java` verifies successful repair writes, missing target handling, and exception capture.
- `kv-repair/src/test/java/com/hkg/kv/repair/ConvergenceMetricsCollectorTest.java` verifies hint backlog shape, replay/read-repair/Merkle outcome counters, stable metric samples, and reporter export.
- `kv-repair/src/test/java/com/hkg/kv/repair/MerkleTreeBuilderTest.java` verifies stable root hashes, full-depth empty-range shape, and range validation.
- `kv-repair/src/test/java/com/hkg/kv/repair/MerkleRepairPlannerTest.java` verifies no-op matching trees, changed digest detection, missing-record detection, and range validation.
- `kv-repair/src/test/java/com/hkg/kv/repair/MerkleRangeScannerTest.java` verifies storage-backed tree construction and deterministic scan ordering.
- `kv-repair/src/test/java/com/hkg/kv/repair/MerkleRepairExecutorTest.java` verifies missing-record repair, stale-version repair, tombstone repair, no-op plans, failed write accounting, equal-version divergence handling, and range/scan/write budget stops.
- `kv-repair/src/test/java/com/hkg/kv/repair/MerkleRepairSchedulerTest.java` verifies due repair execution, clean/incomplete rescheduling, task-per-tick deferral, missing-replica backoff, failed-task isolation, and aggregate repair summaries.
- `kv-repair/src/test/java/com/hkg/kv/repair/RemoteMerkleRepairExecutorTest.java` verifies streamed missing-record repair, stale-version repair, wrong-node write response accounting, write-budget stops, and streamed-record range validation.
- `kv-repair/src/test/java/com/hkg/kv/repair/InMemoryMerkleRepairLeaseStoreTest.java` verifies lease acquisition, lease contention, expiry takeover with higher fencing token, and release-by-matching-token semantics.
- `kv-repair/src/test/java/com/hkg/kv/repair/JdbcMerkleRepairLeaseStoreTest.java` verifies schema initialization, cross-instance lease contention, expired lease takeover, owner/token release protection, and monotonic fencing tokens after release.
- `kv-repair/src/test/java/com/hkg/kv/repair/LeasedMerkleRepairSchedulerTest.java` verifies lease acquisition/release around successful repair, lease-held skip behavior, expired-lease takeover, and task-budget deferral without lease acquisition.

## Runtime Wiring Map

- `kv-node/src/main/java/com/hkg/kv/node/RepairLeaseStoreConfig.java` parses repair lease backend properties for `in-memory` and `jdbc` modes.
- `kv-node/src/main/java/com/hkg/kv/node/RepairLeaseStoreFactory.java` creates the selected `MerkleRepairLeaseStore` and initializes the JDBC schema when configured.
- `kv-node/src/main/java/com/hkg/kv/node/DriverManagerDataSource.java` supplies a framework-neutral `DataSource` for the JDBC lease store until the full node runtime owns connection pooling.
- `kv-node/src/main/java/com/hkg/kv/node/HttpReplicaTransportClient.java` implements `ReplicaWriter`, `ReplicaReader`, and `MerkleRangeStreamer` over JDK `HttpClient`.
- `kv-node/src/main/java/com/hkg/kv/node/HttpReplicaTransportHandlers.java` exposes JDK `HttpHandler` endpoints for replica write, read, and streamed Merkle range repair operations.
- `kv-node/src/main/java/com/hkg/kv/node/HttpReplicaTransportCodec.java` encodes binary mutation, record, token-range, and replica-response payloads without pulling in a JSON dependency.
- `kv-node/src/test/java/com/hkg/kv/node/RepairLeaseStoreConfigTest.java` verifies property parsing and invalid backend handling.
- `kv-node/src/test/java/com/hkg/kv/node/RepairLeaseStoreFactoryTest.java` verifies in-memory creation and initialized H2-backed JDBC lease creation.
- `kv-node/src/test/java/com/hkg/kv/node/HttpReplicaTransportClientTest.java` verifies HTTP write/read/range streaming plus end-to-end Merkle repair over an embedded JDK `HttpServer`.
