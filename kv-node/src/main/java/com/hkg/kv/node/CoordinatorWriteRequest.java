package com.hkg.kv.node;

import com.hkg.kv.common.ConsistencyLevel;
import com.hkg.kv.storage.MutationRecord;

public record CoordinatorWriteRequest(MutationRecord mutation, ConsistencyLevel consistencyLevel) {
    public CoordinatorWriteRequest {
        if (mutation == null) {
            throw new IllegalArgumentException("coordinator write mutation must not be null");
        }
        if (consistencyLevel == null) {
            throw new IllegalArgumentException("coordinator write consistency level must not be null");
        }
    }
}
