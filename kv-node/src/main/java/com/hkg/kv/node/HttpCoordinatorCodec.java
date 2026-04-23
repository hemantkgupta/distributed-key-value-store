package com.hkg.kv.node;

import com.hkg.kv.common.ConsistencyLevel;
import com.hkg.kv.common.Key;
import com.hkg.kv.storage.StoredRecord;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class HttpCoordinatorCodec {
    private HttpCoordinatorCodec() {
    }

    static byte[] encodeWriteRequest(CoordinatorWriteRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("coordinator write request must not be null");
        }
        return encode(out -> {
            writeByteArray(out, HttpReplicaTransportCodec.encodeMutationRecord(request.mutation()));
            writeConsistencyLevel(out, request.consistencyLevel());
        });
    }

    static CoordinatorWriteRequest decodeWriteRequest(byte[] bytes) {
        return decode(bytes, in -> new CoordinatorWriteRequest(
                HttpReplicaTransportCodec.decodeMutationRecord(readByteArray(in)),
                readConsistencyLevel(in)
        ));
    }

    static byte[] encodeWriteResponse(CoordinatorWriteResponse response) {
        if (response == null) {
            throw new IllegalArgumentException("coordinator write response must not be null");
        }
        return encode(out -> {
            out.writeBoolean(response.success());
            out.writeInt(response.acknowledgements());
            out.writeInt(response.acknowledgementsRequired());
            out.writeInt(response.totalAttempts());
            writeStringList(out, response.failedReplicaDetails());
        });
    }

    static CoordinatorWriteResponse decodeWriteResponse(byte[] bytes) {
        return decode(bytes, in -> new CoordinatorWriteResponse(
                in.readBoolean(),
                in.readInt(),
                in.readInt(),
                in.readInt(),
                readStringList(in)
        ));
    }

    static byte[] encodeReadRequest(CoordinatorReadRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("coordinator read request must not be null");
        }
        return encode(out -> {
            writeByteArray(out, HttpReplicaTransportCodec.encodeKey(request.key()));
            writeConsistencyLevel(out, request.consistencyLevel());
        });
    }

    static CoordinatorReadRequest decodeReadRequest(byte[] bytes) {
        return decode(bytes, in -> new CoordinatorReadRequest(
                HttpReplicaTransportCodec.decodeKey(readByteArray(in)),
                readConsistencyLevel(in)
        ));
    }

    static byte[] encodeReadResponse(CoordinatorReadResponse response) {
        if (response == null) {
            throw new IllegalArgumentException("coordinator read response must not be null");
        }
        return encode(out -> {
            out.writeBoolean(response.success());
            writeOptionalStoredRecord(out, response.record());
            out.writeInt(response.successfulResponses());
            out.writeInt(response.responsesRequired());
            out.writeBoolean(response.digestsAgree());
            out.writeInt(response.successfulRepairs());
            out.writeInt(response.failedRepairs());
            writeStringList(out, response.failedReplicaDetails());
            writeString(out, response.detail());
        });
    }

    static CoordinatorReadResponse decodeReadResponse(byte[] bytes) {
        return decode(bytes, in -> new CoordinatorReadResponse(
                in.readBoolean(),
                readOptionalStoredRecord(in),
                in.readInt(),
                in.readInt(),
                in.readBoolean(),
                in.readInt(),
                in.readInt(),
                readStringList(in),
                readString(in)
        ));
    }

    private static void writeOptionalStoredRecord(DataOutputStream out, Optional<StoredRecord> record) throws IOException {
        List<StoredRecord> records = record == null || record.isEmpty() ? List.of() : List.of(record.orElseThrow());
        writeByteArray(out, HttpReplicaTransportCodec.encodeStoredRecords(records));
    }

    private static Optional<StoredRecord> readOptionalStoredRecord(DataInputStream in) throws IOException {
        List<StoredRecord> records = HttpReplicaTransportCodec.decodeStoredRecords(readByteArray(in));
        if (records.isEmpty()) {
            return Optional.empty();
        }
        if (records.size() > 1) {
            throw new IllegalArgumentException("coordinator read response must carry at most one stored record");
        }
        return Optional.of(records.get(0));
    }

    private static void writeConsistencyLevel(DataOutputStream out, ConsistencyLevel consistencyLevel) throws IOException {
        writeString(out, consistencyLevel.name());
    }

    private static ConsistencyLevel readConsistencyLevel(DataInputStream in) throws IOException {
        return ConsistencyLevel.valueOf(readString(in));
    }

    private static void writeStringList(DataOutputStream out, List<String> values) throws IOException {
        if (values == null) {
            throw new IllegalArgumentException("string list must not be null");
        }
        out.writeInt(values.size());
        for (String value : values) {
            writeString(out, value);
        }
    }

    private static List<String> readStringList(DataInputStream in) throws IOException {
        int size = in.readInt();
        if (size < 0) {
            throw new IllegalArgumentException("string list size must not be negative");
        }
        ArrayList<String> values = new ArrayList<>(size);
        for (int index = 0; index < size; index++) {
            values.add(readString(in));
        }
        return List.copyOf(values);
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
        byte[] bytes = in.readNBytes(length);
        if (bytes.length != length) {
            throw new IllegalArgumentException("unexpected end of binary payload");
        }
        return bytes;
    }

    private static byte[] encode(Encoder encoder) {
        try {
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(byteStream)) {
                encoder.encode(out);
            }
            return byteStream.toByteArray();
        } catch (IOException exception) {
            throw new IllegalStateException("failed to encode coordinator payload", exception);
        }
    }

    private static <T> T decode(byte[] bytes, Decoder<T> decoder) {
        if (bytes == null) {
            throw new IllegalArgumentException("payload bytes must not be null");
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            T value = decoder.decode(in);
            if (in.available() > 0) {
                throw new IllegalArgumentException("unexpected trailing bytes in coordinator payload");
            }
            return value;
        } catch (IOException exception) {
            throw new IllegalStateException("failed to decode coordinator payload", exception);
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
