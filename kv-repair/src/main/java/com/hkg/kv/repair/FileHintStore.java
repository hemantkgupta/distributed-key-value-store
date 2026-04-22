package com.hkg.kv.repair;

import com.hkg.kv.common.Key;
import com.hkg.kv.common.NodeId;
import com.hkg.kv.common.Value;
import com.hkg.kv.storage.MutationRecord;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

public final class FileHintStore implements HintStore {
    private static final String VERSION = "v1";
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    private final Path file;

    public FileHintStore(Path file) {
        if (file == null) {
            throw new IllegalArgumentException("hint file must not be null");
        }
        this.file = file;
        initialize();
    }

    @Override
    public synchronized void append(HintRecord hint) {
        if (hint == null) {
            throw new IllegalArgumentException("hint must not be null");
        }
        try {
            Files.writeString(
                    file,
                    serialize(hint) + System.lineSeparator(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
        } catch (IOException exception) {
            throw new IllegalStateException("failed to append hint", exception);
        }
    }

    @Override
    public synchronized List<HintRecord> loadAll() {
        try {
            List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            ArrayList<HintRecord> hints = new ArrayList<>();
            for (String line : lines) {
                if (!line.isBlank()) {
                    hints.add(deserialize(line));
                }
            }
            return List.copyOf(hints);
        } catch (IOException exception) {
            throw new IllegalStateException("failed to load hints", exception);
        }
    }

    @Override
    public synchronized void remove(String hintId) {
        if (hintId == null || hintId.isBlank()) {
            throw new IllegalArgumentException("hint id must not be blank");
        }
        List<HintRecord> remaining = loadAll().stream()
                .filter(hint -> !hint.hintId().equals(hintId))
                .toList();
        rewrite(remaining);
    }

    public Path file() {
        return file;
    }

    private void initialize() {
        try {
            Path parent = file.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            if (!Files.exists(file)) {
                Files.createFile(file);
            }
        } catch (IOException exception) {
            throw new IllegalStateException("failed to initialize hint store", exception);
        }
    }

    private void rewrite(List<HintRecord> hints) {
        try {
            Path parent = file.getParent();
            Path tempFile = parent == null
                    ? Files.createTempFile(file.getFileName().toString(), ".tmp")
                    : Files.createTempFile(parent, file.getFileName().toString(), ".tmp");
            ArrayList<String> lines = new ArrayList<>(hints.size());
            for (HintRecord hint : hints) {
                lines.add(serialize(hint));
            }
            Files.write(tempFile, lines, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);
            try {
                Files.move(tempFile, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException exception) {
                Files.move(tempFile, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException exception) {
            throw new IllegalStateException("failed to rewrite hints", exception);
        }
    }

    private static String serialize(HintRecord hint) {
        MutationRecord mutation = hint.mutation();
        return String.join(
                "|",
                VERSION,
                encode(hint.hintId()),
                encode(hint.targetNodeId().value()),
                encode(mutation.key().bytes()),
                Boolean.toString(mutation.value().isPresent()),
                mutation.value().map(value -> encode(value.bytes())).orElse(""),
                Boolean.toString(mutation.tombstone()),
                mutation.timestamp().toString(),
                mutation.ttl().map(Duration::toMillis).map(String::valueOf).orElse(""),
                encode(mutation.mutationId()),
                hint.createdAt().toString(),
                Integer.toString(hint.deliveryAttempts()),
                hint.nextAttemptAt().map(Instant::toString).orElse("")
        );
    }

    private static HintRecord deserialize(String line) {
        String[] parts = line.split("\\|", -1);
        if (parts.length != 13 || !VERSION.equals(parts[0])) {
            throw new IllegalStateException("unsupported hint record format");
        }

        Optional<Value> value = Boolean.parseBoolean(parts[4])
                ? Optional.of(new Value(decodeBytes(parts[5])))
                : Optional.empty();
        Optional<Duration> ttl = parts[8].isBlank()
                ? Optional.empty()
                : Optional.of(Duration.ofMillis(Long.parseLong(parts[8])));
        Optional<Instant> nextAttemptAt = parts[12].isBlank()
                ? Optional.empty()
                : Optional.of(Instant.parse(parts[12]));

        MutationRecord mutation = new MutationRecord(
                new Key(decodeBytes(parts[3])),
                value,
                Boolean.parseBoolean(parts[6]),
                Instant.parse(parts[7]),
                ttl,
                decodeString(parts[9])
        );

        return new HintRecord(
                decodeString(parts[1]),
                new NodeId(decodeString(parts[2])),
                mutation,
                Instant.parse(parts[10]),
                Integer.parseInt(parts[11]),
                nextAttemptAt
        );
    }

    private static String encode(String value) {
        return encode(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String encode(byte[] bytes) {
        return ENCODER.encodeToString(bytes);
    }

    private static String decodeString(String value) {
        return new String(decodeBytes(value), StandardCharsets.UTF_8);
    }

    private static byte[] decodeBytes(String value) {
        return DECODER.decode(value);
    }
}
