package com.boonya.lab.flink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink Application 入口 —— 汇总博客《Apache Flink 实战：从入门到生产》所有示例。
 *
 * <p>模块包含以下示例包：
 * <ul>
 *   <li>{@code example.datastream}  — DataStream API 实战（行为分析、告警、去重）</li>
 *   <li>{@code example.sql}         — Flink SQL &amp; Table API（窗口聚合、维表 JOIN）</li>
 *   <li>{@code example.state}       — 状态管理与 Checkpoint 配置</li>
 *   <li>{@code example.window}      — Tumbling / Sliding / Session 窗口</li>
 *   <li>{@code example.cdc}         — Flink CDC 数据同步</li>
 *   <li>{@code example.production}  — 生产优化（倾斜处理、Metrics、故障排查）</li>
 *   <li>{@code example.config}      — 环境搭建与参数配置</li>
 * </ul>
 */
public class FlinkApplication {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkApplication.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink Application with production-grade configuration...");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureEnvironment(env);

        // 入口示例：WordCount
        // 其他完整示例请参见 example.* 包下的各独立 main 方法

        env.execute("Flink Application");
        LOG.info("Flink Application completed.");
    }

    /** 生产环境参数配置。 */
    private static void configureEnvironment(StreamExecutionEnvironment env) {
        env.setParallelism(4);

        // 重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                org.apache.flink.api.common.time.Time.minutes(5),
                org.apache.flink.api.common.time.Time.seconds(30)
        ));

        // Checkpoint
        env.enableCheckpointing(60_000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cpConfig = env.getCheckpointConfig();
        cpConfig.setMinPauseBetweenCheckpoints(30_000L);
        cpConfig.setCheckpointTimeout(600_000L);
        cpConfig.setMaxConcurrentCheckpoints(1);
        cpConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }
}
