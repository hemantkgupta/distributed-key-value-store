package com.hkg.kv.repair;

public record HintReplaySummary(int scanned, int attempted, int delivered, int failed, int skipped) {
    public HintReplaySummary {
        if (scanned < 0 || attempted < 0 || delivered < 0 || failed < 0 || skipped < 0) {
            throw new IllegalArgumentException("summary counts must not be negative");
        }
        if (attempted != delivered + failed) {
            throw new IllegalArgumentException("attempted must equal delivered plus failed");
        }
        if (scanned != attempted + skipped) {
            throw new IllegalArgumentException("scanned must equal attempted plus skipped");
        }
    }
}
