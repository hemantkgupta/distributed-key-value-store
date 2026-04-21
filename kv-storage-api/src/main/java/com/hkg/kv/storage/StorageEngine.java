package com.hkg.kv.storage;

import com.hkg.kv.common.Key;
import java.util.Optional;

public interface StorageEngine extends AutoCloseable {
    void apply(MutationRecord mutation);

    Optional<StoredRecord> get(Key key);

    byte[] digest(Key key);

    @Override
    void close();
}
