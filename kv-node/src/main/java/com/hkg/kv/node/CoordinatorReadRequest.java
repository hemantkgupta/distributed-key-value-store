package com.hkg.kv.node;

import com.hkg.kv.common.ConsistencyLevel;
import com.hkg.kv.common.Key;

public record CoordinatorReadRequest(Key key, ConsistencyLevel consistencyLevel) {
    public CoordinatorReadRequest {
        if (key == null) {
            throw new IllegalArgumentException("coordinator read key must not be null");
        }
        if (consistencyLevel == null) {
            throw new IllegalArgumentException("coordinator read consistency level must not be null");
        }
    }
}
