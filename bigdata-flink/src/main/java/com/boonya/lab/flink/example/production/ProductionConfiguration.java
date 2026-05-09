package com.boonya.lab.flink.example.production;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 生产环境配置 —— 内存、网络、Checkpoint 和状态后端的最佳实践。
 */
public class ProductionConfiguration {

    /**
     * 创建预配置好生产级参数的执行环境。
     */
    public static StreamExecutionEnvironment createProductionEnvironment() {
        org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
        config.setString("taskmanager.memory.process.size", "4g");
        config.setString("taskmanager.memory.managed.size", "1g");
        config.setInteger("taskmanager.numberOfTaskSlots", 4);
        config.setInteger("taskmanager.network.numberOfBuffers", 2048);
        config.setFloat("taskmanager.network.memory.fraction", 0.1f);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(8);
        env.getConfig().enableObjectReuse();

        // Checkpoint 配置
        env.enableCheckpointing(300_000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cpConfig = env.getCheckpointConfig();
        cpConfig.setMinPauseBetweenCheckpoints(120_000L);
        cpConfig.setCheckpointTimeout(600_000L);
        cpConfig.setMaxConcurrentCheckpoints(1);
        cpConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // Failure Rate 重启策略：5 分钟窗口内最多 3 次，每次延迟 30 秒
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                org.apache.flink.api.common.time.Time.minutes(5),
                org.apache.flink.api.common.time.Time.seconds(30)
        ));

        // RocksDB 状态后端（生产推荐）
        try {
            EmbeddedRocksDBStateBackend rocksDB = new EmbeddedRocksDBStateBackend(true);
            rocksDB.setDbStoragePath("hdfs://namenode:9000/flink/rocksdb");
            env.setStateBackend(rocksDB);
        } catch (Exception e) {
            System.err.println("Failed to set RocksDB state backend: " + e.getMessage());
        }

        return env;
    }
}
