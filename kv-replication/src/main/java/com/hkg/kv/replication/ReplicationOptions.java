package com.hkg.kv.replication;

public record ReplicationOptions(Integer localReplicaCount, String localDatacenter, int maxAttempts) {
    public static final String DATACENTER_LABEL = "datacenter";
    public static final String DC_LABEL = "dc";

    public ReplicationOptions {
        if (localReplicaCount != null && localReplicaCount <= 0) {
            throw new IllegalArgumentException("local replica count must be positive");
        }
        localDatacenter = normalize(localDatacenter);
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("max attempts must be positive");
        }
    }

    public static ReplicationOptions defaults() {
        return new ReplicationOptions(null, null, 1);
    }

    public static ReplicationOptions withLocalReplicaCount(int localReplicaCount) {
        return new ReplicationOptions(localReplicaCount, null, 1);
    }

    public ReplicationOptions withMaxAttempts(int maxAttempts) {
        return new ReplicationOptions(localReplicaCount, localDatacenter, maxAttempts);
    }

    public ReplicationOptions withLocalDatacenter(String localDatacenter) {
        return new ReplicationOptions(localReplicaCount, localDatacenter, maxAttempts);
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }
}
