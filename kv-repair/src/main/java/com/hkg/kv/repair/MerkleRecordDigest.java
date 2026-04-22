package com.hkg.kv.repair;

import com.hkg.kv.common.Key;
import com.hkg.kv.partitioning.Token;

public final class MerkleRecordDigest {
    private final Token token;
    private final Key key;
    private final byte[] digest;

    public MerkleRecordDigest(Token token, Key key, byte[] digest) {
        if (token == null) {
            throw new IllegalArgumentException("token must not be null");
        }
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        if (digest == null) {
            throw new IllegalArgumentException("digest must not be null");
        }
        this.token = token;
        this.key = key;
        this.digest = digest.clone();
    }

    public Token token() {
        return token;
    }

    public Key key() {
        return key;
    }

    public byte[] digest() {
        return digest.clone();
    }
}
