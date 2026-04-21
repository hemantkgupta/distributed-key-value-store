package com.hkg.kv.replication;

import com.hkg.kv.common.ConsistencyLevel;

/**
 * Resolves the acknowledgement threshold for a given consistency level.
 *
 * <p>{@code ANY} is intentionally explicit here: a single durable acknowledgement is enough,
 * and that acknowledgement may come from a local hint write instead of a replica response.
 */
public record ConsistencyWaitPolicy(int acknowledgementsRequired, boolean hintMaySatisfyAcknowledgement) {
    public ConsistencyWaitPolicy {
        if (acknowledgementsRequired < 1) {
            throw new IllegalArgumentException("acknowledgements required must be positive");
        }
    }

    public static ConsistencyWaitPolicy forLevel(
            ConsistencyLevel consistencyLevel,
            int replicaCount,
            Integer localReplicaCount
    ) {
        if (consistencyLevel == null) {
            throw new IllegalArgumentException("consistency level must not be null");
        }
        if (replicaCount < 1) {
            throw new IllegalArgumentException("replica count must be positive");
        }
        if (localReplicaCount != null && localReplicaCount < 1) {
            throw new IllegalArgumentException("local replica count must be positive");
        }
        if (localReplicaCount != null && localReplicaCount > replicaCount) {
            throw new IllegalArgumentException("local replica count must not exceed replica count");
        }

        return switch (consistencyLevel) {
            case ONE -> new ConsistencyWaitPolicy(1, false);
            case QUORUM -> new ConsistencyWaitPolicy(quorum(replicaCount), false);
            case ALL -> new ConsistencyWaitPolicy(replicaCount, false);
            case ANY -> new ConsistencyWaitPolicy(1, true);
            case LOCAL_QUORUM -> new ConsistencyWaitPolicy(quorum(requireLocalReplicaCount(localReplicaCount)), false);
        };
    }

    private static int quorum(int replicaCount) {
        return (replicaCount / 2) + 1;
    }

    private static int requireLocalReplicaCount(Integer localReplicaCount) {
        if (localReplicaCount == null) {
            throw new IllegalArgumentException("local replica count is required for LOCAL_QUORUM");
        }
        return localReplicaCount;
    }
}
