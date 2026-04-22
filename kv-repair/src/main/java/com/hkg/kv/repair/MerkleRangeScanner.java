package com.hkg.kv.repair;

import com.hkg.kv.common.Key;
import com.hkg.kv.partitioning.KeyTokenHasher;
import com.hkg.kv.partitioning.Token;
import com.hkg.kv.partitioning.TokenRange;
import com.hkg.kv.storage.StorageEngine;
import com.hkg.kv.storage.StoredRecord;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class MerkleRangeScanner {
    public List<StoredRecord> recordsInRange(StorageEngine storage, TokenRange range) {
        if (storage == null) {
            throw new IllegalArgumentException("storage must not be null");
        }
        if (range == null) {
            throw new IllegalArgumentException("range must not be null");
        }

        ArrayList<StoredRecord> records = new ArrayList<>();
        for (StoredRecord record : storage.scanAll()) {
            if (record == null) {
                throw new IllegalStateException("storage scan returned a null record");
            }
            if (range.contains(KeyTokenHasher.tokenFor(record.key()))) {
                records.add(record);
            }
        }
        records.sort(recordComparator());
        return List.copyOf(records);
    }

    public List<MerkleRecordDigest> digestsInRange(StorageEngine storage, TokenRange range) {
        ArrayList<MerkleRecordDigest> digests = new ArrayList<>();
        for (StoredRecord record : recordsInRange(storage, range)) {
            Token token = KeyTokenHasher.tokenFor(record.key());
            digests.add(new MerkleRecordDigest(token, record.key(), storage.digest(record.key())));
        }
        return List.copyOf(digests);
    }

    public MerkleTree treeFor(StorageEngine storage, TokenRange range, int maxDepth) {
        return new MerkleTreeBuilder(maxDepth).build(range, digestsInRange(storage, range));
    }

    private static Comparator<StoredRecord> recordComparator() {
        return (left, right) -> {
            int tokenComparison = KeyTokenHasher.tokenFor(left.key()).compareTo(KeyTokenHasher.tokenFor(right.key()));
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
}
