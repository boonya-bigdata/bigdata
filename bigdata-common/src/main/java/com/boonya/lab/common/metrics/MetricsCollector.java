package com.boonya.lab.common.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 简易指标收集器 — 为各模块提供统一的指标采集能力
 * 在生产环境中可替换为 Micrometer/Prometheus 集成
 */
public class MetricsCollector {

    private static final MetricsCollector INSTANCE = new MetricsCollector();

    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();

    private MetricsCollector() {}

    public static MetricsCollector getInstance() { return INSTANCE; }

    public void increment(String metric) {
        counters.computeIfAbsent(metric, k -> new AtomicLong()).incrementAndGet();
    }

    public void add(String metric, long value) {
        counters.computeIfAbsent(metric, k -> new AtomicLong()).addAndGet(value);
    }

    public long get(String metric) {
        AtomicLong counter = counters.get(metric);
        return counter != null ? counter.get() : 0;
    }

    public Map<String, Long> snapshot() {
        Map<String, Long> snapshot = new ConcurrentHashMap<>();
        counters.forEach((k, v) -> snapshot.put(k, v.get()));
        return snapshot;
    }

    // 常用指标名
    public static final String RECORDS_IN      = "records.in.total";
    public static final String RECORDS_OUT     = "records.out.total";
    public static final String RECORDS_ERROR   = "records.error.total";
    public static final String RECORDS_INVALID = "records.invalid.total";
    public static final String RECORDS_SKEWED  = "records.skewed.total";
}
