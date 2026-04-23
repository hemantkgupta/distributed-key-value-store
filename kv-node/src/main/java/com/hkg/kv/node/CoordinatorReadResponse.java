package com.hkg.kv.node;

import com.hkg.kv.storage.StoredRecord;
import java.util.List;
import java.util.Optional;

public record CoordinatorReadResponse(
        boolean success,
        Optional<StoredRecord> record,
        int successfulResponses,
        int responsesRequired,
        boolean digestsAgree,
        int successfulRepairs,
        int failedRepairs,
        List<String> failedReplicaDetails,
        String detail
) {
    public CoordinatorReadResponse {
        record = record == null ? Optional.empty() : record;
        if (successfulResponses < 0) {
            throw new IllegalArgumentException("successful responses must not be negative");
        }
        if (responsesRequired < 0) {
            throw new IllegalArgumentException("responses required must not be negative");
        }
        if (successfulRepairs < 0) {
            throw new IllegalArgumentException("successful repairs must not be negative");
        }
        if (failedRepairs < 0) {
            throw new IllegalArgumentException("failed repairs must not be negative");
        }
        if (failedReplicaDetails == null) {
            throw new IllegalArgumentException("failed replica details must not be null");
        }
        failedReplicaDetails = List.copyOf(failedReplicaDetails);
        for (String failedReplicaDetail : failedReplicaDetails) {
            if (failedReplicaDetail == null) {
                throw new IllegalArgumentException("failed replica details must not contain null entries");
            }
        }
        detail = detail == null ? "" : detail;
    }
}
