package com.boonya.lab.flink.example.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Checkpoint 与 Savepoint 配置 —— 演示 EXACTLY_ONCE 语义下的生产级配置。
 */
public class CheckpointConfiguration {

    /**
     * 配置 Checkpoint 参数。
     */
    public static void configureCheckpoints(StreamExecutionEnvironment env) {
        env.enableCheckpointing(60_000L, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig cpConfig = env.getCheckpointConfig();

        // 两次 checkpoint 之间的最小暂停时间
        cpConfig.setMinPauseBetweenCheckpoints(30_000L);
        // checkpoint 超时
        cpConfig.setCheckpointTimeout(600_000L);
        // 最大并发 checkpoint 数量
        cpConfig.setMaxConcurrentCheckpoints(1);

        // 取消作业时保留 checkpoint（用于外部恢复）
        cpConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 允许的 checkpoint 失败次数
        cpConfig.setTolerableCheckpointFailureNumber(3);
    }

    /**
     * 使用 FsStateBackend (HashMap)。存储在 HDFS。
     */
    public static void useFsStateBackend(StreamExecutionEnvironment env) {
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://namenode:9000/flink/checkpoints");
    }

    /**
     * 使用 RocksDBStateBackend —— 推荐用于生产环境的大状态场景。
     */
    public static void useRocksDBStateBackend(StreamExecutionEnvironment env) throws Exception {
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.getCheckpointConfig().setCheckpointStorage("hdfs://namenode:9000/flink/checkpoints");
    }

    /**
     * 使用 MemoryStateBackend —— 仅用于开发和简单作业。
     */
    public static void useMemoryStateBackend(StreamExecutionEnvironment env) {
        // Flink 1.18 默认使用 HashMapStateBackend
        env.setStateBackend(new HashMapStateBackend());
    }

    /**
     * 配置固定延迟重启策略。
     */
    public static void configureRestartStrategy(StreamExecutionEnvironment env) {
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10_000L));
    }
}
