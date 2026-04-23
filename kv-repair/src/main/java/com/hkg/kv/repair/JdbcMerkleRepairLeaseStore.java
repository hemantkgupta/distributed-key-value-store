package com.hkg.kv.repair;

import com.hkg.kv.common.NodeId;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import javax.sql.DataSource;

public final class JdbcMerkleRepairLeaseStore implements MerkleRepairLeaseStore {
    public static final String DEFAULT_TABLE_NAME = "kv_merkle_repair_leases";

    private static final int MAX_ACQUIRE_ATTEMPTS = 2;

    private final DataSource dataSource;
    private final String tableName;

    public JdbcMerkleRepairLeaseStore(DataSource dataSource) {
        this(dataSource, DEFAULT_TABLE_NAME);
    }

    public JdbcMerkleRepairLeaseStore(DataSource dataSource, String tableName) {
        if (dataSource == null) {
            throw new IllegalArgumentException("data source must not be null");
        }
        this.dataSource = dataSource;
        this.tableName = validateTableName(tableName);
    }

    public void initializeSchema() {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS %s (
                        task_id VARCHAR(512) PRIMARY KEY,
                        owner_id VARCHAR(256) NOT NULL,
                        fencing_token BIGINT NOT NULL,
                        acquired_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        active BOOLEAN NOT NULL
                    )
                    """.formatted(tableName));
        } catch (SQLException exception) {
            throw databaseFailure("initialize", exception);
        }
    }

    @Override
    public Optional<MerkleRepairLease> tryAcquire(
            String taskId,
            NodeId owner,
            Instant now,
            Duration leaseTtl
    ) {
        validate(taskId, owner, now, leaseTtl);

        for (int attempt = 1; attempt <= MAX_ACQUIRE_ATTEMPTS; attempt++) {
            try {
                return tryAcquireOnce(taskId, owner, now, leaseTtl);
            } catch (SQLException exception) {
                if (attempt < MAX_ACQUIRE_ATTEMPTS && isDuplicateKey(exception)) {
                    continue;
                }
                throw databaseFailure("acquire", exception);
            }
        }
        throw new IllegalStateException("unreachable acquire retry state");
    }

    @Override
    public boolean release(MerkleRepairLease lease) {
        if (lease == null) {
            throw new IllegalArgumentException("lease must not be null");
        }

        String sql = """
                UPDATE %s
                SET active = FALSE
                WHERE task_id = ?
                  AND owner_id = ?
                  AND fencing_token = ?
                  AND active = TRUE
                """.formatted(tableName);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, lease.taskId());
            statement.setString(2, lease.owner().value());
            statement.setLong(3, lease.fencingToken());
            return statement.executeUpdate() == 1;
        } catch (SQLException exception) {
            throw databaseFailure("release", exception);
        }
    }

    @Override
    public Optional<MerkleRepairLease> currentLease(String taskId) {
        validateTaskId(taskId);

        String sql = """
                SELECT task_id, owner_id, fencing_token, acquired_at, expires_at, active
                FROM %s
                WHERE task_id = ?
                """.formatted(tableName);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, taskId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                LeaseRow row = LeaseRow.from(resultSet);
                if (!row.active()) {
                    return Optional.empty();
                }
                return Optional.of(row.toLease());
            }
        } catch (SQLException exception) {
            throw databaseFailure("read current", exception);
        }
    }

    private Optional<MerkleRepairLease> tryAcquireOnce(
            String taskId,
            NodeId owner,
            Instant now,
            Duration leaseTtl
    ) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            boolean originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                Optional<LeaseRow> current = selectForUpdate(connection, taskId);
                Optional<MerkleRepairLease> acquired = acquireFromLockedRow(
                        connection,
                        current,
                        taskId,
                        owner,
                        now,
                        leaseTtl
                );
                connection.commit();
                return acquired;
            } catch (SQLException | RuntimeException exception) {
                rollback(connection);
                throw exception;
            } finally {
                connection.setAutoCommit(originalAutoCommit);
            }
        }
    }

    private Optional<MerkleRepairLease> acquireFromLockedRow(
            Connection connection,
            Optional<LeaseRow> current,
            String taskId,
            NodeId owner,
            Instant now,
            Duration leaseTtl
    ) throws SQLException {
        if (current.isEmpty()) {
            MerkleRepairLease lease = new MerkleRepairLease(taskId, owner, 1L, now, now.plus(leaseTtl));
            insertLease(connection, lease);
            return Optional.of(lease);
        }

        LeaseRow row = current.orElseThrow();
        if (row.active() && !row.toLease().isExpiredAt(now)) {
            return Optional.empty();
        }

        MerkleRepairLease lease = new MerkleRepairLease(
                taskId,
                owner,
                Math.addExact(row.fencingToken(), 1L),
                now,
                now.plus(leaseTtl)
        );
        updateLease(connection, lease);
        return Optional.of(lease);
    }

    private Optional<LeaseRow> selectForUpdate(Connection connection, String taskId) throws SQLException {
        String sql = """
                SELECT task_id, owner_id, fencing_token, acquired_at, expires_at, active
                FROM %s
                WHERE task_id = ?
                FOR UPDATE
                """.formatted(tableName);
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, taskId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }
                return Optional.of(LeaseRow.from(resultSet));
            }
        }
    }

    private void insertLease(Connection connection, MerkleRepairLease lease) throws SQLException {
        String sql = """
                INSERT INTO %s (task_id, owner_id, fencing_token, acquired_at, expires_at, active)
                VALUES (?, ?, ?, ?, ?, TRUE)
                """.formatted(tableName);
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bindLease(statement, lease);
            statement.executeUpdate();
        }
    }

    private void updateLease(Connection connection, MerkleRepairLease lease) throws SQLException {
        String sql = """
                UPDATE %s
                SET owner_id = ?,
                    fencing_token = ?,
                    acquired_at = ?,
                    expires_at = ?,
                    active = TRUE
                WHERE task_id = ?
                """.formatted(tableName);
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, lease.owner().value());
            statement.setLong(2, lease.fencingToken());
            statement.setObject(3, toOffsetDateTime(lease.acquiredAt()));
            statement.setObject(4, toOffsetDateTime(lease.expiresAt()));
            statement.setString(5, lease.taskId());
            statement.executeUpdate();
        }
    }

    private static void bindLease(PreparedStatement statement, MerkleRepairLease lease) throws SQLException {
        statement.setString(1, lease.taskId());
        statement.setString(2, lease.owner().value());
        statement.setLong(3, lease.fencingToken());
        statement.setObject(4, toOffsetDateTime(lease.acquiredAt()));
        statement.setObject(5, toOffsetDateTime(lease.expiresAt()));
    }

    private static void rollback(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException rollbackException) {
            throw databaseFailure("rollback", rollbackException);
        }
    }

    private static void validate(String taskId, NodeId owner, Instant now, Duration leaseTtl) {
        validateTaskId(taskId);
        if (owner == null) {
            throw new IllegalArgumentException("owner must not be null");
        }
        if (now == null) {
            throw new IllegalArgumentException("now must not be null");
        }
        if (leaseTtl == null || leaseTtl.isZero() || leaseTtl.isNegative()) {
            throw new IllegalArgumentException("lease ttl must be positive");
        }
    }

    private static void validateTaskId(String taskId) {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("task id must not be blank");
        }
    }

    private static String validateTableName(String tableName) {
        if (tableName == null || !tableName.matches("[A-Za-z][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("table name must be a simple SQL identifier");
        }
        return tableName;
    }

    private static boolean isDuplicateKey(SQLException exception) {
        if (exception instanceof SQLIntegrityConstraintViolationException) {
            return true;
        }
        String sqlState = exception.getSQLState();
        return "23505".equals(sqlState);
    }

    private static IllegalStateException databaseFailure(String operation, SQLException exception) {
        return new IllegalStateException("failed to " + operation + " Merkle repair lease", exception);
    }

    private static OffsetDateTime toOffsetDateTime(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private record LeaseRow(
            String taskId,
            String ownerId,
            long fencingToken,
            Instant acquiredAt,
            Instant expiresAt,
            boolean active
    ) {
        private static LeaseRow from(ResultSet resultSet) throws SQLException {
            return new LeaseRow(
                    resultSet.getString("task_id"),
                    resultSet.getString("owner_id"),
                    resultSet.getLong("fencing_token"),
                    resultSet.getObject("acquired_at", OffsetDateTime.class).toInstant(),
                    resultSet.getObject("expires_at", OffsetDateTime.class).toInstant(),
                    resultSet.getBoolean("active")
            );
        }

        private MerkleRepairLease toLease() {
            return new MerkleRepairLease(
                    taskId,
                    new NodeId(ownerId),
                    fencingToken,
                    acquiredAt,
                    expiresAt
            );
        }
    }
}
