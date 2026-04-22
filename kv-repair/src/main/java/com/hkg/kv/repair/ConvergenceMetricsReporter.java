package com.hkg.kv.repair;

public final class ConvergenceMetricsReporter {
    private final ConvergenceMetricsExporter exporter;

    public ConvergenceMetricsReporter(ConvergenceMetricsExporter exporter) {
        if (exporter == null) {
            throw new IllegalArgumentException("metrics exporter must not be null");
        }
        this.exporter = exporter;
    }

    public void report(ConvergenceMetricsSnapshot snapshot) {
        if (snapshot == null) {
            throw new IllegalArgumentException("metrics snapshot must not be null");
        }
        exporter.export(snapshot.toMetrics());
    }
}
