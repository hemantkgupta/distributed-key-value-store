package com.hkg.kv.replication;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ReplicationOptionsTest {
    @Test
    void defaultsToSingleAttemptWithoutLocalScope() {
        ReplicationOptions options = ReplicationOptions.defaults();

        assertThat(options.maxAttempts()).isEqualTo(1);
        assertThat(options.localReplicaCount()).isNull();
        assertThat(options.localDatacenter()).isNull();
    }

    @Test
    void normalizesLocalDatacenter() {
        ReplicationOptions options = ReplicationOptions.defaults().withLocalDatacenter(" us-east1 ");

        assertThat(options.localDatacenter()).isEqualTo("us-east1");
    }

    @Test
    void rejectsInvalidAttemptCount() {
        assertThatThrownBy(() -> ReplicationOptions.defaults().withMaxAttempts(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("max attempts must be positive");
    }
}
