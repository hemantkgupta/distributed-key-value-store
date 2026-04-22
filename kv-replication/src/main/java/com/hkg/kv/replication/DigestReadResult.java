package com.hkg.kv.replication;

import com.hkg.kv.common.Key;
import java.util.ArrayList;
import java.util.List;

public record DigestReadResult(Key key, List<ReplicaReadResponse> responses) {
    public DigestReadResult {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        if (responses == null) {
            throw new IllegalArgumentException("responses must not be null");
        }
        for (ReplicaReadResponse response : responses) {
            if (response == null) {
                throw new IllegalArgumentException("responses must not contain null entries");
            }
        }
        responses = List.copyOf(responses);
    }

    public List<ReplicaReadResponse> successfulResponses() {
        return responses.stream()
                .filter(ReplicaReadResponse::success)
                .toList();
    }

    public List<ReplicaReadResponse> failedResponses() {
        return responses.stream()
                .filter(response -> !response.success())
                .toList();
    }

    public boolean digestsAgree() {
        List<ReplicaReadResponse> successfulResponses = successfulResponses();
        if (successfulResponses.size() < 2) {
            return true;
        }
        ReplicaReadResponse first = successfulResponses.get(0);
        return successfulResponses.stream().allMatch(first::sameDigest);
    }

    public List<ReplicaReadResponse> mismatchedResponses() {
        List<ReplicaReadResponse> successfulResponses = successfulResponses();
        if (successfulResponses.size() < 2 || digestsAgree()) {
            return List.of();
        }

        ArrayList<ReplicaReadResponse> mismatches = new ArrayList<>();
        ReplicaReadResponse first = successfulResponses.get(0);
        for (ReplicaReadResponse response : successfulResponses) {
            if (!first.sameDigest(response)) {
                mismatches.add(response);
            }
        }
        return List.copyOf(mismatches);
    }
}
