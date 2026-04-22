package com.hkg.kv.repair;

import com.hkg.kv.common.NodeId;
import com.hkg.kv.storage.MutationRecord;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

public final class HintedHandoffService {
    private final HintStore hintStore;
    private final Supplier<String> hintIdSupplier;
    private final Clock clock;

    public HintedHandoffService(HintStore hintStore) {
        this(hintStore, () -> UUID.randomUUID().toString(), Clock.systemUTC());
    }

    public HintedHandoffService(HintStore hintStore, Supplier<String> hintIdSupplier, Clock clock) {
        if (hintStore == null) {
            throw new IllegalArgumentException("hint store must not be null");
        }
        if (hintIdSupplier == null) {
            throw new IllegalArgumentException("hint id supplier must not be null");
        }
        if (clock == null) {
            throw new IllegalArgumentException("clock must not be null");
        }
        this.hintStore = hintStore;
        this.hintIdSupplier = hintIdSupplier;
        this.clock = clock;
    }

    public HintRecord recordHint(NodeId targetNodeId, MutationRecord mutation) {
        HintRecord hint = new HintRecord(
                hintIdSupplier.get(),
                targetNodeId,
                mutation,
                Instant.now(clock),
                0,
                Optional.empty()
        );
        hintStore.append(hint);
        return hint;
    }

    public List<HintRecord> pendingHints() {
        return hintStore.loadAll();
    }

    public void markDelivered(String hintId) {
        hintStore.remove(hintId);
    }
}
