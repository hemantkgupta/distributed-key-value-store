package com.hkg.kv.node;

import java.util.List;

public record CoordinatorWriteResponse(
        boolean success,
        int acknowledgements,
        int acknowledgementsRequired,
        int totalAttempts,
        List<String> failedReplicaDetails
) {
    public CoordinatorWriteResponse {
        if (acknowledgements < 0) {
            throw new IllegalArgumentException("acknowledgements must not be negative");
        }
        if (acknowledgementsRequired < 0) {
            throw new IllegalArgumentException("acknowledgements required must not be negative");
        }
        if (totalAttempts < 0) {
            throw new IllegalArgumentException("total attempts must not be negative");
        }
        if (failedReplicaDetails == null) {
            throw new IllegalArgumentException("failed replica details must not be null");
        }
        failedReplicaDetails = List.copyOf(failedReplicaDetails);
        for (String detail : failedReplicaDetails) {
            if (detail == null) {
                throw new IllegalArgumentException("failed replica details must not contain null entries");
            }
        }
    }
}
