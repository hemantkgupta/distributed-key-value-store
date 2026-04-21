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
        if (mutationId == null || mutationId.isBlank()) {
            throw new IllegalArgumentException("mutation id must not be blank");
        }
    }
}
