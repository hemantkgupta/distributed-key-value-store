package com.hkg.kv.replication;

import java.util.List;

public record ReplicationResult(
        List<ReplicaResponse> responses,
        int successfulAcknowledgements,
        ConsistencyWaitPolicy waitPolicy,
        boolean consistencySatisfied
) {
    public ReplicationResult {
        if (responses == null) {
            throw new IllegalArgumentException("responses must not be null");
        }
        responses = List.copyOf(responses);
        for (ReplicaResponse response : responses) {
            if (response == null) {
                throw new IllegalArgumentException("responses must not contain null entries");
            }
        }
        if (successfulAcknowledgements < 0 || successfulAcknowledgements > responses.size()) {
            throw new IllegalArgumentException("successful acknowledgements must be within response bounds");
        }
        if (waitPolicy == null) {
            throw new IllegalArgumentException("wait policy must not be null");
        }
        consistencySatisfied = successfulAcknowledgements >= waitPolicy.acknowledgementsRequired();
    }

    public List<ReplicaResponse> failedResponses() {
        return responses.stream()
                .filter(response -> !response.success())
                .toList();
    }
}
