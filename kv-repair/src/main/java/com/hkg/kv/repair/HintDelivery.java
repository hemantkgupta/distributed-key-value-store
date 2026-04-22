package com.hkg.kv.repair;

@FunctionalInterface
public interface HintDelivery {
    boolean deliver(HintRecord hint);
}
