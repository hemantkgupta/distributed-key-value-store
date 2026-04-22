package com.hkg.kv.partitioning;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TokenRangeTest {
    @Test
    void nonWrappingRangeIsStartExclusiveAndEndInclusive() {
        TokenRange range = new TokenRange(new Token(10L), new Token(20L));

        assertThat(range.contains(new Token(10L))).isFalse();
        assertThat(range.contains(new Token(11L))).isTrue();
        assertThat(range.contains(new Token(20L))).isTrue();
        assertThat(range.contains(new Token(21L))).isFalse();
    }

    @Test
    void wrappingRangeCoversTailAndHeadOfRing() {
        TokenRange range = new TokenRange(new Token(20L), new Token(10L));

        assertThat(range.contains(new Token(21L))).isTrue();
        assertThat(range.contains(new Token(Long.MAX_VALUE))).isTrue();
        assertThat(range.contains(new Token(Long.MIN_VALUE))).isTrue();
        assertThat(range.contains(new Token(10L))).isTrue();
        assertThat(range.contains(new Token(20L))).isFalse();
        assertThat(range.contains(new Token(15L))).isFalse();
    }

    @Test
    void fullRingCoversEveryToken() {
        TokenRange range = TokenRange.fullRing();

        assertThat(range.contains(new Token(0L))).isTrue();
        assertThat(range.contains(new Token(1L))).isTrue();
        assertThat(range.contains(new Token(Long.MIN_VALUE))).isTrue();
        assertThat(range.contains(new Token(-1L))).isTrue();
    }

    @Test
    void splitPreservesContiguousCoverage() {
        TokenRange range = new TokenRange(new Token(0L), new Token(8L));

        assertThat(range.split()).containsExactly(
                new TokenRange(new Token(0L), new Token(4L)),
                new TokenRange(new Token(4L), new Token(8L))
        );
    }

    @Test
    void splitHandlesFullRing() {
        TokenRange range = TokenRange.fullRing();

        assertThat(range.split()).containsExactly(
                new TokenRange(new Token(0L), new Token(Long.MIN_VALUE)),
                new TokenRange(new Token(Long.MIN_VALUE), new Token(0L))
        );
    }

    @Test
    void rejectsSplitWhenRangeHasNoInteriorMidpoint() {
        TokenRange range = new TokenRange(new Token(7L), new Token(8L));

        assertThat(range.canSplit()).isFalse();
        assertThatThrownBy(range::split).isInstanceOf(IllegalStateException.class);
    }
}
