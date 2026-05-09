package com.boonya.lab.flink.example.production;

/**
 * 故障排查指南 —— 常见问题的诊断和建议。
 */
public class TroubleshootingGuide {

    /**
     * Checkpoint 超时排查建议。
     */
    public static String checkpointTimeout() {
        return String.join("\n",
                "=== Checkpoint Timeout ===",
                "1. 增大 checkpointTimeout: cpConfig.setCheckpointTimeout(600_000)",
                "2. 增大 minPauseBetweenCheckpoints 避免重叠",
                "3. 启用 RocksDB 增量 checkpoint: new EmbeddedRocksDBStateBackend(true)",
                "4. 减小状态数据量，设置 State TTL",
                "5. 检查网络带宽和 HDFS 写入性能"
        );
    }

    /**
     * 背压排查建议。
     */
    public static String backpressure() {
        return String.join("\n",
                "=== Backpressure ===",
                "1. 增大瓶颈算子的并行度",
                "2. 优化算子逻辑（减少 IO、避免长尾计算）",
                "3. 增加 TaskManager 内存和 CPU 资源",
                "4. 优化 Sink 写入性能（批量写入、异步 IO）",
                "5. 检查数据倾斜，使用 rebalance / 两阶段聚合"
        );
    }

    /**
     * 内存溢出排查建议。
     */
    public static String outOfMemory() {
        return String.join("\n",
                "=== OutOfMemoryError ===",
                "1. 增加 taskmanager.memory.process.size 或 managed.size",
                "2. 设置 State TTL 清理过期状态",
                "3. 使用 RocksDB 状态后端代替堆内存",
                "4. 减少滑动窗口数量或窗口大小",
                "5. 使用 MAT/JProfiler 分析内存泄漏"
        );
    }

    /**
     * Kafka 消费延迟排查建议。
     */
    public static String kafkaconsumerLag() {
        return String.join("\n",
                "=== Kafka Consumer Lag ===",
                "1. 增大 Kafka Source 并行度",
                "2. 增加 Kafka 分区数",
                "3. 优化下游算子处理能力",
                "4. 检查网络和数据格式解析开销",
                "5. 使用批量反序列化减少解析次数"
        );
    }

    public static void main(String[] args) {
        System.out.println(checkpointTimeout());
        System.out.println(backpressure());
        System.out.println(outOfMemory());
        System.out.println(kafkaconsumerLag());
    }
}
