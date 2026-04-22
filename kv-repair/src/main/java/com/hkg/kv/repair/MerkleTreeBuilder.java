package com.hkg.kv.repair;

import com.hkg.kv.partitioning.TokenRange;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public final class MerkleTreeBuilder {
    private static final byte LEAF_HASH_DOMAIN = 0x00;
    private static final byte PARENT_HASH_DOMAIN = 0x01;

    private final int maxDepth;

    public MerkleTreeBuilder(int maxDepth) {
        if (maxDepth < 0) {
            throw new IllegalArgumentException("max depth must not be negative");
        }
        if (maxDepth > 16) {
            throw new IllegalArgumentException("max depth is too large");
        }
        this.maxDepth = maxDepth;
    }

    public MerkleTree build(TokenRange range, List<MerkleRecordDigest> records) {
        if (range == null) {
            throw new IllegalArgumentException("range must not be null");
        }
        if (records == null) {
            throw new IllegalArgumentException("records must not be null");
        }

        ArrayList<MerkleRecordDigest> sortedRecords = new ArrayList<>(records.size());
        for (MerkleRecordDigest record : records) {
            if (record == null) {
                throw new IllegalArgumentException("records must not contain null entries");
            }
            if (!range.contains(record.token())) {
                throw new IllegalArgumentException("record token falls outside Merkle tree range");
            }
            sortedRecords.add(record);
        }
        sortedRecords.sort(recordComparator());

        return new MerkleTree(range, buildNode(range, sortedRecords, 0), maxDepth);
    }

    private MerkleTreeNode buildNode(TokenRange range, List<MerkleRecordDigest> records, int depth) {
        if (depth >= maxDepth || !range.canSplit()) {
            return leaf(range, records);
        }

        List<TokenRange> children = range.split();
        ArrayList<MerkleRecordDigest> leftRecords = new ArrayList<>();
        ArrayList<MerkleRecordDigest> rightRecords = new ArrayList<>();
        for (MerkleRecordDigest record : records) {
            if (children.get(0).contains(record.token())) {
                leftRecords.add(record);
            } else if (children.get(1).contains(record.token())) {
                rightRecords.add(record);
            } else {
                throw new IllegalStateException("split token ranges did not cover record token");
            }
        }

        MerkleTreeNode left = buildNode(children.get(0), leftRecords, depth + 1);
        MerkleTreeNode right = buildNode(children.get(1), rightRecords, depth + 1);
        return new MerkleTreeNode(
                range,
                parentHash(range, left.hash(), right.hash()),
                left.recordCount() + right.recordCount(),
                Optional.of(left),
                Optional.of(right)
        );
    }

    private static MerkleTreeNode leaf(TokenRange range, List<MerkleRecordDigest> records) {
        return new MerkleTreeNode(range, leafHash(range, records), records.size(), Optional.empty(), Optional.empty());
    }

    private static byte[] leafHash(TokenRange range, List<MerkleRecordDigest> records) {
        MessageDigest digest = newDigest();
        digest.update(LEAF_HASH_DOMAIN);
        updateRange(digest, range);
        updateInt(digest, records.size());
        for (MerkleRecordDigest record : records) {
            updateLong(digest, record.token().value());
            byte[] keyBytes = record.key().bytes();
            updateBytes(digest, keyBytes);
            updateBytes(digest, record.digest());
        }
        return digest.digest();
    }

    private static byte[] parentHash(TokenRange range, byte[] leftHash, byte[] rightHash) {
        MessageDigest digest = newDigest();
        digest.update(PARENT_HASH_DOMAIN);
        updateRange(digest, range);
        updateBytes(digest, leftHash);
        updateBytes(digest, rightHash);
        return digest.digest();
    }

    private static Comparator<MerkleRecordDigest> recordComparator() {
        return (left, right) -> {
            int tokenComparison = left.token().compareTo(right.token());
            if (tokenComparison != 0) {
                return tokenComparison;
            }
            return compareUnsigned(left.key().bytes(), right.key().bytes());
        };
    }

    private static int compareUnsigned(byte[] left, byte[] right) {
        int length = Math.min(left.length, right.length);
        for (int index = 0; index < length; index++) {
            int comparison = Integer.compare(Byte.toUnsignedInt(left[index]), Byte.toUnsignedInt(right[index]));
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    private static void updateRange(MessageDigest digest, TokenRange range) {
        updateLong(digest, range.startExclusive().value());
        updateLong(digest, range.endInclusive().value());
    }

    private static void updateBytes(MessageDigest digest, byte[] bytes) {
        updateInt(digest, bytes.length);
        digest.update(bytes);
    }

    private static void updateInt(MessageDigest digest, int value) {
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
    }

    private static void updateLong(MessageDigest digest, long value) {
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
    }

    private static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
