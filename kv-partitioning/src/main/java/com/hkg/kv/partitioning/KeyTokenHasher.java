package com.hkg.kv.partitioning;

import com.hkg.kv.common.Key;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class KeyTokenHasher {
    private KeyTokenHasher() {
    }

    public static Token tokenFor(Key key) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        return tokenFromDigest(key.bytes());
    }

    public static Token vnodeTokenFor(ClusterNode node, int vnodeIndex) {
        if (node == null) {
            throw new IllegalArgumentException("node must not be null");
        }
        if (vnodeIndex < 0) {
            throw new IllegalArgumentException("vnode index must not be negative");
        }

        MessageDigest digest = newDigest();
        digest.update(node.nodeId().value().getBytes(StandardCharsets.UTF_8));
        digest.update((byte) 0);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(vnodeIndex).array());
        return tokenFromBytes(digest.digest());
    }

    private static Token tokenFromDigest(byte[] input) {
        MessageDigest digest = newDigest();
        digest.update(input);
        return tokenFromBytes(digest.digest());
    }

    private static Token tokenFromBytes(byte[] digest) {
        long value = ByteBuffer.wrap(digest, 0, Long.BYTES).getLong();
        return new Token(value);
    }

    private static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
