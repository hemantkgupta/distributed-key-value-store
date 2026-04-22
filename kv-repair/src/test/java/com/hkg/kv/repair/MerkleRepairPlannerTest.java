package com.hkg.kv.repair;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hkg.kv.common.Key;
import com.hkg.kv.partitioning.Token;
import com.hkg.kv.partitioning.TokenRange;
import java.util.List;
import org.junit.jupiter.api.Test;

class MerkleRepairPlannerTest {
    private static final TokenRange RANGE = new TokenRange(new Token(0L), new Token(8L));

    @Test
    void returnsEmptyPlanWhenTreesMatch() {
        MerkleTree left = tree(List.of(digest(1, "k1", 1), digest(5, "k5", 5)));
        MerkleTree right = tree(List.of(digest(1, "k1", 1), digest(5, "k5", 5)));

        MerkleRepairPlan plan = new MerkleRepairPlanner().plan(left, right);

        assertThat(plan.requiresRepair()).isFalse();
        assertThat(plan.differences()).isEmpty();
    }

    @Test
    void drillsDownToOnlyDifferingLeafRanges() {
        MerkleTree left = tree(List.of(
                digest(1, "k1", 1),
                digest(3, "k3", 3),
                digest(5, "k5", 5),
                digest(7, "k7", 7)
        ));
        MerkleTree right = tree(List.of(
                digest(1, "k1", 1),
                digest(3, "k3", 33),
                digest(5, "k5", 5),
                digest(7, "k7", 7)
        ));

        MerkleRepairPlan plan = new MerkleRepairPlanner().plan(left, right);

        assertThat(plan.requiresRepair()).isTrue();
        assertThat(plan.differences()).hasSize(1);
        assertThat(plan.differences().get(0).range()).isEqualTo(new TokenRange(new Token(2L), new Token(4L)));
        assertThat(plan.differences().get(0).leftRecordCount()).isEqualTo(1);
        assertThat(plan.differences().get(0).rightRecordCount()).isEqualTo(1);
    }

    @Test
    void detectsMissingRecordsAsDifferingLeafRanges() {
        MerkleTree left = tree(List.of(digest(1, "k1", 1), digest(5, "k5", 5)));
        MerkleTree right = tree(List.of(digest(1, "k1", 1)));

        MerkleRepairPlan plan = new MerkleRepairPlanner().plan(left, right);

        assertThat(plan.differences()).hasSize(1);
        assertThat(plan.differences().get(0).range()).isEqualTo(new TokenRange(new Token(4L), new Token(6L)));
        assertThat(plan.differences().get(0).leftRecordCount()).isEqualTo(1);
        assertThat(plan.differences().get(0).rightRecordCount()).isZero();
    }

    @Test
    void rejectsMismatchedTreeRanges() {
        MerkleTree left = tree(List.of(digest(1, "k1", 1)));
        MerkleTree right = new MerkleTreeBuilder(2).build(
                new TokenRange(new Token(8L), new Token(16L)),
                List.of(digest(9, "k9", 9))
        );

        assertThatThrownBy(() -> new MerkleRepairPlanner().plan(left, right))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("same token range");
    }

    private static MerkleTree tree(List<MerkleRecordDigest> records) {
        return new MerkleTreeBuilder(2).build(RANGE, records);
    }

    private static MerkleRecordDigest digest(long token, String key, int digestByte) {
        return new MerkleRecordDigest(new Token(token), Key.utf8(key), new byte[] {(byte) digestByte});
    }
}
