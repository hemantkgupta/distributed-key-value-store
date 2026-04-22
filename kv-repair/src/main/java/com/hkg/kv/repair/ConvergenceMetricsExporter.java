package com.hkg.kv.repair;

import java.util.List;

@FunctionalInterface
public interface ConvergenceMetricsExporter {
    void export(List<ConvergenceMetric> metrics);
}
