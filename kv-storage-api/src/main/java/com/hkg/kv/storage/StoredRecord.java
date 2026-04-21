package com.hkg.kv.storage;

import com.hkg.kv.common.Key;
import com.hkg.kv.common.Value;
import java.time.Instant;
import java.util.Optional;

public record StoredRecord(
        Key key,
        Optional<Value> value,
        boolean tombstone,
        Instant timestamp,
        Optional<Instant> expiresAt,
        String mutationId
) {
    public StoredRecord {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        value = value == null ? Optional.empty() : value;
        if (!tombstone && value.isEmpty()) {
            throw new IllegalArgumentException("live record must carry a value");
        }
        if (timestamp == null) {
            throw new IllegalArgumentException("timestamp must not be null");
        }
        expiresAt = expiresAt == null ? Optional.empty() : expiresAt;
        if (mutationId == null || mutationId.isBlank()) {
            throw new IllegalArgumentException("mutation id must not be blank");
        }
    }

    public static StoredRecord from(MutationRecord mutation) {
        Optional<Instant> expiresAt = mutation.ttl().map(mutation.timestamp()::plus);
        return new StoredRecord(
                mutation.key(),
                mutation.value(),
                mutation.tombstone(),
                mutation.timestamp(),
                expiresAt,
                mutation.mutationId()
        );
    }

    public boolean isExpiredAt(Instant now) {
        if (now == null) {
            throw new IllegalArgumentException("now must not be null");
        }
        return expiresAt.map(expiry -> !now.isBefore(expiry)).orElse(false);
    }

    public boolean isLiveAt(Instant now) {
        return !tombstone && !isExpiredAt(now);
    }

    public int compareVersionTo(StoredRecord other) {
        int timestampCompare = timestamp.compareTo(other.timestamp);
        if (timestampCompare != 0) {
            return timestampCompare;
        }
        return mutationId.compareTo(other.mutationId);
    }
}
