package com.hkg.kv.repair;

import java.util.List;

public interface HintStore {
    void append(HintRecord hint);

    List<HintRecord> loadAll();

    void remove(String hintId);

    default void replace(HintRecord hint) {
        remove(hint.hintId());
        append(hint);
    }
}
