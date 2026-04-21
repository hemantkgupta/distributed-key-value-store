package com.hkg.kv.replication;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hkg.kv.common.ConsistencyLevel;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ConsistencyWaitPolicyTest {
    @ParameterizedTest(name = "{0} with N={1} requires {2} acknowledgements")
    @MethodSource("replicaMathCases")
    void resolvesAcknowledgementThresholds(ConsistencyLevel level, int replicaCount, Integer localReplicaCount, int expectedAcks) {
        ConsistencyWaitPolicy policy = ConsistencyWaitPolicy.forLevel(level, replicaCount, localReplicaCount);

        assertThat(policy.acknowledgementsRequired()).isEqualTo(expectedAcks);
    }

    @Test
    void anyTreatsHintedHandoffAsAnAcknowledgementOption() {
        ConsistencyWaitPolicy policy = ConsistencyWaitPolicy.forLevel(ConsistencyLevel.ANY, 3, null);

        assertThat(policy.acknowledgementsRequired()).isEqualTo(1);
        assertThat(policy.hintMaySatisfyAcknowledgement()).isTrue();
    }

    @Test
    void rejectsNonPositiveReplicaCounts() {
        assertThatThrownBy(() -> ConsistencyWaitPolicy.forLevel(ConsistencyLevel.ONE, 0, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("replica count must be positive");
    }

    @Test
    void rejectsMissingLocalReplicaCountForLocalQuorum() {
        assertThatThrownBy(() -> ConsistencyWaitPolicy.forLevel(ConsistencyLevel.LOCAL_QUORUM, 3, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("local replica count is required for LOCAL_QUORUM");
    }

    @Test
    void rejectsLocalReplicaCountLargerThanReplicaCount() {
        assertThatThrownBy(() -> ConsistencyWaitPolicy.forLevel(ConsistencyLevel.LOCAL_QUORUM, 3, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("local replica count must not exceed replica count");
    }

    @Test
    void rejectsNonPositiveLocalReplicaCounts() {
        assertThatThrownBy(() -> ConsistencyWaitPolicy.forLevel(ConsistencyLevel.LOCAL_QUORUM, 3, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("local replica count must be positive");
    }

    private static Stream<Arguments> replicaMathCases() {
        return Stream.of(
                Arguments.of(ConsistencyLevel.ONE, 3, null, 1),
                Arguments.of(ConsistencyLevel.ONE, 5, null, 1),
                Arguments.of(ConsistencyLevel.QUORUM, 3, null, 2),
                Arguments.of(ConsistencyLevel.QUORUM, 5, null, 3),
                Arguments.of(ConsistencyLevel.ALL, 3, null, 3),
                Arguments.of(ConsistencyLevel.ALL, 5, null, 5),
                Arguments.of(ConsistencyLevel.ANY, 3, null, 1),
                Arguments.of(ConsistencyLevel.ANY, 5, null, 1),
                Arguments.of(ConsistencyLevel.LOCAL_QUORUM, 3, 3, 2),
                Arguments.of(ConsistencyLevel.LOCAL_QUORUM, 5, 5, 3)
        );
    }
}
