package com.hkg.kv.node;

import com.hkg.kv.common.Key;
import com.hkg.kv.common.NodeId;
import com.hkg.kv.common.Value;
import com.hkg.kv.partitioning.Token;
import com.hkg.kv.partitioning.TokenRange;
import com.hkg.kv.replication.ReplicaReadResponse;
import com.hkg.kv.replication.ReplicaResponse;
import com.hkg.kv.storage.MutationRecord;
import com.hkg.kv.storage.StoredRecord;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class HttpReplicaTransportCodec {
    private HttpReplicaTransportCodec() {
    }

    static byte[] encodeMutationRecord(MutationRecord mutation) {
        if (mutation == null) {
            throw new IllegalArgumentException("mutation record must not be null");
        }
        return encode(out -> writeMutationRecord(out, mutation));
    }

    static MutationRecord decodeMutationRecord(byte[] bytes) {
        return decode(bytes, HttpReplicaTransportCodec::readMutationRecord);
    }

    static byte[] encodeKey(Key key) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        return encode(out -> writeKey(out, key));
    }

    static Key decodeKey(byte[] bytes) {
        return decode(bytes, HttpReplicaTransportCodec::readKey);
    }

    static byte[] encodeTokenRange(TokenRange range) {
        if (range == null) {
            throw new IllegalArgumentException("token range must not be null");
        }
        return encode(out -> writeTokenRange(out, range));
    }

    static TokenRange decodeTokenRange(byte[] bytes) {
        return decode(bytes, HttpReplicaTransportCodec::readTokenRange);
    }

    static byte[] encodeReplicaResponse(ReplicaResponse response) {
        if (response == null) {
            throw new IllegalArgumentException("replica response must not be null");
        }
        return encode(out -> writeReplicaResponse(out, response));
    }

    static ReplicaResponse decodeReplicaResponse(byte[] bytes) {
        return decode(bytes, HttpReplicaTransportCodec::readReplicaResponse);
    }

    static byte[] encodeReplicaReadResponse(ReplicaReadResponse response) {
        if (response == null) {
            throw new IllegalArgumentException("replica read response must not be null");
        }
        return encode(out -> writeReplicaReadResponse(out, response));
    }

    static ReplicaReadResponse decodeReplicaReadResponse(byte[] bytes) {
        return decode(bytes, HttpReplicaTransportCodec::readReplicaReadResponse);
    }

    static byte[] encodeStoredRecords(List<StoredRecord> records) {
        if (records == null) {
            throw new IllegalArgumentException("stored records must not be null");
        }
        return encode(out -> {
            out.writeInt(records.size());
            for (StoredRecord record : records) {
                writeStoredRecord(out, record);
            }
        });
    }

    static List<StoredRecord> decodeStoredRecords(byte[] bytes) {
        return decode(bytes, in -> {
            int size = in.readInt();
            if (size < 0) {
                throw new IllegalArgumentException("record list size must not be negative");
            }
            ArrayList<StoredRecord> records = new ArrayList<>(size);
            for (int index = 0; index < size; index++) {
                records.add(readStoredRecord(in));
            }
            return List.copyOf(records);
        });
    }

    private static void writeMutationRecord(DataOutputStream out, MutationRecord mutation) throws IOException {
        writeKey(out, mutation.key());
        writeOptionalValue(out, mutation.value());
        out.writeBoolean(mutation.tombstone());
        writeInstant(out, mutation.timestamp());
        writeOptionalDuration(out, mutation.ttl());
        writeString(out, mutation.mutationId());
    }

    private static MutationRecord readMutationRecord(DataInputStream in) throws IOException {
        return new MutationRecord(
                readKey(in),
                readOptionalValue(in),
                in.readBoolean(),
                readInstant(in),
                readOptionalDuration(in),
                readString(in)
        );
    }

    private static void writeStoredRecord(DataOutputStream out, StoredRecord record) throws IOException {
        if (record == null) {
            throw new IllegalArgumentException("stored record must not be null");
        }
        writeKey(out, record.key());
        writeOptionalValue(out, record.value());
        out.writeBoolean(record.tombstone());
        writeInstant(out, record.timestamp());
        writeOptionalInstant(out, record.expiresAt());
        writeString(out, record.mutationId());
    }

    private static StoredRecord readStoredRecord(DataInputStream in) throws IOException {
        return new StoredRecord(
                readKey(in),
                readOptionalValue(in),
                in.readBoolean(),
                readInstant(in),
                readOptionalInstant(in),
                readString(in)
        );
    }

    private static void writeReplicaResponse(DataOutputStream out, ReplicaResponse response) throws IOException {
        writeNodeId(out, response.nodeId());
        out.writeBoolean(response.success());
        writeString(out, response.detail());
        out.writeInt(response.attempts());
    }

    private static ReplicaResponse readReplicaResponse(DataInputStream in) throws IOException {
        return new ReplicaResponse(
                readNodeId(in),
                in.readBoolean(),
                readString(in),
                in.readInt()
        );
    }

    private static void writeReplicaReadResponse(DataOutputStream out, ReplicaReadResponse response) throws IOException {
        writeNodeId(out, response.nodeId());
        out.writeBoolean(response.success());
        writeString(out, response.detail());
        if (response.success()) {
            writeOptionalStoredRecord(out, response.record());
            writeByteArray(out, response.digest());
        }
    }

    private static ReplicaReadResponse readReplicaReadResponse(DataInputStream in) throws IOException {
        NodeId nodeId = readNodeId(in);
        boolean success = in.readBoolean();
        String detail = readString(in);
        if (!success) {
            return ReplicaReadResponse.failure(nodeId, detail);
        }
        return new ReplicaReadResponse(
                nodeId,
                true,
                readOptionalStoredRecord(in),
                readByteArray(in),
                detail
        );
    }

    private static void writeTokenRange(DataOutputStream out, TokenRange range) throws IOException {
        out.writeLong(range.startExclusive().value());
        out.writeLong(range.endInclusive().value());
    }

    private static TokenRange readTokenRange(DataInputStream in) throws IOException {
        return new TokenRange(new Token(in.readLong()), new Token(in.readLong()));
    }

    private static void writeNodeId(DataOutputStream out, NodeId nodeId) throws IOException {
        if (nodeId == null) {
            throw new IllegalArgumentException("node id must not be null");
        }
        writeString(out, nodeId.value());
    }

    private static NodeId readNodeId(DataInputStream in) throws IOException {
        return new NodeId(readString(in));
    }

    private static void writeKey(DataOutputStream out, Key key) throws IOException {
        writeByteArray(out, key.bytes());
    }

    private static Key readKey(DataInputStream in) throws IOException {
        return new Key(readByteArray(in));
    }

    private static void writeOptionalStoredRecord(DataOutputStream out, Optional<StoredRecord> record) throws IOException {
        if (record == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(record.isPresent());
        if (record.isPresent()) {
            writeStoredRecord(out, record.orElseThrow());
        }
    }

    private static Optional<StoredRecord> readOptionalStoredRecord(DataInputStream in) throws IOException {
        if (!in.readBoolean()) {
            return Optional.empty();
        }
        return Optional.of(readStoredRecord(in));
    }

    private static void writeOptionalValue(DataOutputStream out, Optional<Value> value) throws IOException {
        if (value == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(value.isPresent());
        if (value.isPresent()) {
            writeByteArray(out, value.orElseThrow().bytes());
        }
    }

    private static Optional<Value> readOptionalValue(DataInputStream in) throws IOException {
        if (!in.readBoolean()) {
            return Optional.empty();
        }
        return Optional.of(new Value(readByteArray(in)));
    }

    private static void writeOptionalDuration(DataOutputStream out, Optional<Duration> duration) throws IOException {
        if (duration == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(duration.isPresent());
        if (duration.isPresent()) {
            writeDuration(out, duration.orElseThrow());
        }
    }

    private static Optional<Duration> readOptionalDuration(DataInputStream in) throws IOException {
        if (!in.readBoolean()) {
            return Optional.empty();
        }
        return Optional.of(readDuration(in));
    }

    private static void writeOptionalInstant(DataOutputStream out, Optional<Instant> instant) throws IOException {
        if (instant == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(instant.isPresent());
        if (instant.isPresent()) {
            writeInstant(out, instant.orElseThrow());
        }
    }

    private static Optional<Instant> readOptionalInstant(DataInputStream in) throws IOException {
        if (!in.readBoolean()) {
            return Optional.empty();
        }
        return Optional.of(readInstant(in));
    }

    private static void writeInstant(DataOutputStream out, Instant instant) throws IOException {
        out.writeLong(instant.getEpochSecond());
        out.writeInt(instant.getNano());
    }

    private static Instant readInstant(DataInputStream in) throws IOException {
        return Instant.ofEpochSecond(in.readLong(), in.readInt());
    }

    private static void writeDuration(DataOutputStream out, Duration duration) throws IOException {
        out.writeLong(duration.getSeconds());
        out.writeInt(duration.getNano());
    }

    private static Duration readDuration(DataInputStream in) throws IOException {
        return Duration.ofSeconds(in.readLong(), in.readInt());
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        if (value == null) {
            value = "";
        }
        writeByteArray(out, value.getBytes(StandardCharsets.UTF_8));
    }

    private static String readString(DataInputStream in) throws IOException {
        return new String(readByteArray(in), StandardCharsets.UTF_8);
    }

    private static void writeByteArray(DataOutputStream out, byte[] bytes) throws IOException {
        if (bytes == null) {
            throw new IllegalArgumentException("byte array must not be null");
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static byte[] readByteArray(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0) {
            throw new IllegalArgumentException("byte array length must not be negative");
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    private static byte[] encode(Encoder encoder) {
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            try (DataOutputStream dataOutput = new DataOutputStream(output)) {
                encoder.encode(dataOutput);
            }
            return output.toByteArray();
        } catch (IOException exception) {
            throw new IllegalStateException("failed to encode transport payload", exception);
        }
    }

    private static <T> T decode(byte[] bytes, Decoder<T> decoder) {
        if (bytes == null) {
            throw new IllegalArgumentException("transport payload must not be null");
        }
        try (DataInputStream dataInput = new DataInputStream(new ByteArrayInputStream(bytes))) {
            T value = decoder.decode(dataInput);
            if (dataInput.available() > 0) {
                throw new IllegalArgumentException("transport payload had trailing bytes");
            }
            return value;
        } catch (IOException exception) {
            throw new IllegalArgumentException("failed to decode transport payload", exception);
        }
    }

    @FunctionalInterface
    private interface Encoder {
        void encode(DataOutputStream out) throws IOException;
    }

    @FunctionalInterface
    private interface Decoder<T> {
        T decode(DataInputStream in) throws IOException;
    }
}
