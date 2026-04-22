package com.hkg.kv.repair;

import com.hkg.kv.partitioning.ClusterNode;
import com.hkg.kv.partitioning.KeyTokenHasher;
import com.hkg.kv.partitioning.TokenRange;
import com.hkg.kv.replication.ReplicaResponse;
import com.hkg.kv.replication.ReplicaWriter;
import com.hkg.kv.storage.StoredRecord;
import java.util.ArrayList;
import java.util.List;

public final class RemoteMerkleRepairExecutor {
    private final MerkleRangeStreamer rangeStreamer;
    private final ReplicaWriter replicaWriter;

    public RemoteMerkleRepairExecutor(MerkleRangeStreamer rangeStreamer, ReplicaWriter replicaWriter) {
        if (rangeStreamer == null) {
            throw new IllegalArgumentException("range streamer must not be null");
        }
        if (replicaWriter == null) {
            throw new IllegalArgumentException("replica writer must not be null");
        }
        this.rangeStreamer = rangeStreamer;
        this.replicaWriter = replicaWriter;
    }

    public MerkleRepairResult execute(MerkleRepairPlan plan, ClusterNode leftReplica, ClusterNode rightReplica) {
        return execute(plan, leftReplica, rightReplica, MerkleRepairBudget.unbounded());
    }

    public MerkleRepairResult execute(
            MerkleRepairPlan plan,
            ClusterNode leftReplica,
            ClusterNode rightReplica,
            MerkleRepairBudget budget
    ) {
        if (plan == null) {
            throw new IllegalArgumentException("Merkle repair plan must not be null");
        }
        if (leftReplica == null) {
            throw new IllegalArgumentException("left replica must not be null");
        }
        if (rightReplica == null) {
            throw new IllegalArgumentException("right replica must not be null");
        }
        if (leftReplica.nodeId().equals(rightReplica.nodeId())) {
            throw new IllegalArgumentException("Merkle repair replicas must be distinct");
        }
        if (budget == null) {
            throw new IllegalArgumentException("Merkle repair budget must not be null");
        }
        if (!plan.requiresRepair()) {
            return MerkleRepairResult.empty();
        }

        int scannedLeftRecords = 0;
        int scannedRightRecords = 0;
        int appliedToLeft = 0;
        int appliedToRight = 0;
        int failedWrites = 0;
        int alreadyConvergedKeys = 0;
        int processedRanges = 0;
        int partiallyProcessedRanges = 0;
        boolean stoppedByBudget = false;

        for (MerkleDifference difference : plan.differences()) {
            if (!budget.canStartRange(processedRanges)) {
                stoppedByBudget = true;
                break;
            }

            int expectedRangeRecords = difference.leftRecordCount() + difference.rightRecordCount();
            if (!budget.canScan(scannedLeftRecords + scannedRightRecords, expectedRangeRecords)) {
                stoppedByBudget = true;
                break;
            }

            List<StoredRecord> leftRecords = streamRecords(leftReplica, difference.range());
            List<StoredRecord> rightRecords = streamRecords(rightReplica, difference.range());
            int rangeScannedRecords = leftRecords.size() + rightRecords.size();
            if (!budget.canScan(scannedLeftRecords + scannedRightRecords, rangeScannedRecords)) {
                stoppedByBudget = true;
                break;
            }

            scannedLeftRecords += leftRecords.size();
            scannedRightRecords += rightRecords.size();
            processedRanges++;

            int writeAttemptsBeforeRange = MerkleRepairRangeReconciler.writeAttempts(
                    appliedToLeft,
                    appliedToRight,
                    failedWrites
            );
            MerkleRepairRangeReconciler.RepairCounters counters = MerkleRepairRangeReconciler.repairRange(
                    leftRecords,
                    rightRecords,
                    budget,
                    writeAttemptsBeforeRange,
                    new MerkleRepairRangeReconciler.RepairWriter() {
                        @Override
                        public boolean applyToLeft(StoredRecord record) {
                            return writeRepair(leftReplica, record);
                        }

                        @Override
                        public boolean applyToRight(StoredRecord record) {
                            return writeRepair(rightReplica, record);
                        }
                    }
            );
            appliedToLeft += counters.appliedToLeft();
            appliedToRight += counters.appliedToRight();
            failedWrites += counters.failedWrites();
            alreadyConvergedKeys += counters.alreadyConvergedKeys();
            if (counters.stoppedByBudget()) {
                stoppedByBudget = true;
                partiallyProcessedRanges++;
                break;
            }
        }

        return new MerkleRepairResult(
                plan.differences().size(),
                scannedLeftRecords,
                scannedRightRecords,
                appliedToLeft,
                appliedToRight,
                failedWrites,
                alreadyConvergedKeys,
                plan.differences().size() - processedRanges + partiallyProcessedRanges,
                stoppedByBudget
        );
    }

    private List<StoredRecord> streamRecords(ClusterNode replica, TokenRange range) {
        List<StoredRecord> records = rangeStreamer.stream(replica, range);
        if (records == null) {
            throw new IllegalStateException("range streamer returned null records");
        }

        ArrayList<StoredRecord> validated = new ArrayList<>(records.size());
        for (StoredRecord record : records) {
            if (record == null) {
                throw new IllegalStateException("range streamer returned a null record");
            }
            if (!range.contains(KeyTokenHasher.tokenFor(record.key()))) {
                throw new IllegalStateException("range streamer returned a record outside the requested range");
            }
            validated.add(record);
        }
        return List.copyOf(validated);
    }

    private boolean writeRepair(ClusterNode replica, StoredRecord record) {
        try {
            ReplicaResponse response = replicaWriter.write(replica, record.toMutationRecord());
            return response != null && response.success() && replica.nodeId().equals(response.nodeId());
        } catch (RuntimeException exception) {
            return false;
        }
    }
}
