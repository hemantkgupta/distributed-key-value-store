package com.hkg.kv.repair;

import com.hkg.kv.common.NodeId;
import com.hkg.kv.replication.DigestReadResult;
import com.hkg.kv.replication.ReplicaReadResponse;
import com.hkg.kv.storage.StoredRecord;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public final class ReadRepairPlanner {
    public ReadRepairPlan plan(DigestReadResult readResult) {
        if (readResult == null) {
            throw new IllegalArgumentException("read result must not be null");
        }

        Optional<ReplicaReadResponse> latestResponse = latestSuccessfulResponseWithRecord(readResult);
        if (latestResponse.isEmpty()) {
            return new ReadRepairPlan(readResult.key(), Optional.empty(), List.of());
        }

        byte[] latestDigest = latestResponse.get().digest();
        StoredRecord latestRecord = latestResponse.get().record().orElseThrow();
        ArrayList<NodeId> targets = new ArrayList<>();
        for (ReplicaReadResponse response : readResult.successfulResponses()) {
            if (!Arrays.equals(response.digest(), latestDigest)) {
                targets.add(response.nodeId());
            }
        }

        return new ReadRepairPlan(readResult.key(), Optional.of(latestRecord), targets);
    }

    private static Optional<ReplicaReadResponse> latestSuccessfulResponseWithRecord(DigestReadResult readResult) {
        return readResult.successfulResponses().stream()
                .filter(response -> response.record().isPresent())
                .max((left, right) -> left.record().orElseThrow().compareVersionTo(right.record().orElseThrow()));
    }
}
