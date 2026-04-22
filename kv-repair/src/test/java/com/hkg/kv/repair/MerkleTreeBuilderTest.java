package com.hkg.kv.repair;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hkg.kv.common.Key;
import com.hkg.kv.partitioning.Token;
import com.hkg.kv.partitioning.TokenRange;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class MerkleTreeBuilderTest {
    @Test
    void buildsStableRootHashIndependentOfInputOrder() {
        TokenRange range = new TokenRange(new Token(0L), new Token(8L));
        MerkleTreeBuilder builder = new MerkleTreeBuilder(2);

        MerkleTree first = builder.build(range, List.of(
                digest(7, "k7", 7),
                digest(1, "k1", 1),
                digest(5, "k5", 5)
        ));
        MerkleTree second = builder.build(range, List.of(
                digest(5, "k5", 5),
                digest(7, "k7", 7),
                digest(1, "k1", 1)
        ));

        assertThat(first.recordCount()).isEqualTo(3);
        assertThat(Arrays.equals(first.rootHash(), second.rootHash())).isTrue();
    }

    @Test
    void buildsFullDepthTreeEvenForEmptyRanges() {
        TokenRange range = new TokenRange(new Token(0L), new Token(8L));
        MerkleTree tree = new MerkleTreeBuilder(2).build(range, List.of(digest(1, "k1", 1)));

        assertThat(tree.root().isLeaf()).isFalse();
        assertThat(tree.root().left().orElseThrow().isLeaf()).isFalse();
        assertThat(tree.root().right().orElseThrow().isLeaf()).isFalse();
        assertThat(tree.root().right().orElseThrow().recordCount()).isZero();
    }

    @Test
    void rejectsRecordOutsideRange() {
        TokenRange range = new TokenRange(new Token(0L), new Token(8L));
        MerkleTreeBuilder builder = new MerkleTreeBuilder(2);

        assertThatThrownBy(() -> builder.build(range, List.of(digest(9, "k9", 9))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outside");
    }

    @Test
    void rejectsDepthThatWouldCreateTooManyInMemoryNodes() {
        assertThatThrownBy(() -> new MerkleTreeBuilder(17))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("too large");
    }

    private static MerkleRecordDigest digest(long token, String key, int digestByte) {
        return new MerkleRecordDigest(new Token(token), Key.utf8(key), new byte[] {(byte) digestByte});
    }
}
