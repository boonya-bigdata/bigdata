package com.boonya.lab.flink.example.config;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 环境搭建与配置 —— 演示两种创建 Flink 执行环境的方式。
 */
public class EnvironmentSetup {

    /**
     * 方式一：使用默认配置创建本地/集群执行环境。
     */
    public static StreamExecutionEnvironment createDefaultEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        return env;
    }

    /**
     * 方式二：使用自定义 Configuration 创建执行环境。
     * 设置内存、slot 数量，以及重启策略和 Checkpoint。
     */
    public static StreamExecutionEnvironment createCustomEnvironment() {
        org.apache.flink.configuration.Configuration config =
                new org.apache.flink.configuration.Configuration();
        config.setString("taskmanager.memory.process.size", "2g");
        config.setInteger("taskmanager.numberOfTaskSlots", 4);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(4);

        // 固定延迟重启策略：3 次重试，间隔 10 秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10_000L));

        // Checkpoint 配置
        env.enableCheckpointing(60_000L, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig cpConfig = env.getCheckpointConfig();
        cpConfig.setMinPauseBetweenCheckpoints(30_000L);
        cpConfig.setCheckpointTimeout(600_000L);
        cpConfig.setMaxConcurrentCheckpoints(1);
        cpConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        return env;
    }
}
