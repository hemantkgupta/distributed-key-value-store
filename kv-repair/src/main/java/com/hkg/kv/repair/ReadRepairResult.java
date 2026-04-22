package com.hkg.kv.repair;

import com.hkg.kv.replication.ReplicaResponse;
import java.util.List;

public record ReadRepairResult(List<ReplicaResponse> responses, int missingTargets) {
    public ReadRepairResult {
        if (responses == null) {
            throw new IllegalArgumentException("responses must not be null");
        }
        for (ReplicaResponse response : responses) {
            if (response == null) {
                throw new IllegalArgumentException("responses must not contain null entries");
            }
        }
        responses = List.copyOf(responses);
        if (missingTargets < 0) {
            throw new IllegalArgumentException("missing targets must not be negative");
        }
    }

    public int successfulRepairs() {
        return (int) responses.stream()
                .filter(ReplicaResponse::success)
                .count();
    }

    public int failedRepairs() {
        return (int) responses.stream()
                .filter(response -> !response.success())
                .count();
    }

    public int attemptedRepairs() {
        return responses.size();
    }
}
