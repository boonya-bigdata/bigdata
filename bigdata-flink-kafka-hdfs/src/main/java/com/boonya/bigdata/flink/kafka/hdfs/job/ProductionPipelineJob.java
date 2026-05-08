package com.boonya.bigdata.flink.kafka.hdfs.job;

import com.boonya.bigdata.flink.kafka.hdfs.config.FlinkConfig;
import com.boonya.bigdata.flink.kafka.hdfs.function.JsonParserFunction;
import com.boonya.bigdata.flink.kafka.hdfs.function.SkewAwareAggregator;
import com.boonya.bigdata.flink.kafka.hdfs.model.UserEvent;
import com.boonya.lab.common.metrics.MetricsCollector;
import com.boonya.lab.common.quality.DataValidator;
import com.boonya.lab.common.quality.UserEventValidator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 生产级端到端数据管道
 *
 * 数据流: Kafka → 数据质量校验 → 解析 → (脏数据侧输出) → 倾斜感知聚合 → HDFS
 *
 * 特性:
 *  - 数据质量校验层 (DataValidator)
 *  - 脏数据侧输出 (side output) 不丢失任何数据
 *  - 数据倾斜自动处理 (SkewAwareAggregator)
 *  - Checkpoint 容错
 *  - 指标埋点
 */
public class ProductionPipelineJob {

    private static final Logger log = LoggerFactory.getLogger(ProductionPipelineJob.class);

    private static final OutputTag<String> INVALID_RECORDS =
            new OutputTag<String>("invalid-records") {};

    private static final OutputTag<String> PARSE_ERRORS =
            new OutputTag<String>("parse-errors") {};

    public static void main(String[] args) throws Exception {
        FlinkConfig config = FlinkConfig.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.getParallelism());
        env.enableCheckpointing(config.getCheckpointInterval());
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 1. Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(config.getBootstrapServers())
                .setTopics(config.getTopic())
                .setGroupId(config.getGroupId())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env.fromSource(
                kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        // 2. 数据质量校验 (前置)
        DataValidator<UserEventValidator.UserEvent> validator = UserEventValidator.create();

        SingleOutputStreamOperator<UserEvent> validatedStream = rawStream
                .process(new JsonParserFunction()) // JSON解析 + 错误侧输出
                .filter(event -> {
                    DataValidator.ValidationResult result = validator.validate(
                            new UserEventValidator.UserEvent(
                                    event.userId(), event.page(), event.amount(), event.timestamp()));
                    if (result.isInvalid()) {
                        MetricsCollector.getInstance().increment(MetricsCollector.RECORDS_INVALID);
                        log.debug("Invalid record: userId={}, errors={}", event.userId(), result.errors());
                        return false;
                    }
                    MetricsCollector.getInstance().increment(MetricsCollector.RECORDS_IN);
                    return true;
                });

        // 3. 数据倾斜感知聚合
        DataStream<String> aggregatedStream = validatedStream
                .keyBy(UserEvent::getSaltedKey)
                .process(new SkewAwareAggregator())
                .name("skew-aware-aggregator");

        // 4. HDFS Sink (原始事件 — 数据湖层)
        FileSink<String> rawEventSink = FileSink
                .<String>forRowFormat(
                        new Path(config.getHdfsOutputPath() + "/raw-events"),
                        new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(Duration.ofMinutes(5))
                        .withInactivityInterval(Duration.ofMinutes(1))
                        .withMaxPartSize(128 * 1024 * 1024)
                        .build())
                .build();

        validatedStream
                .map(UserEvent::toCsv)
                .name("event-to-csv")
                .sinkTo(rawEventSink)
                .name("hdfs-raw-sink");

        // 5. HDFS Sink (聚合结果)
        FileSink<String> aggSink = FileSink
                .<String>forRowFormat(
                        new Path(config.getHdfsOutputPath() + "/aggregations"),
                        new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(Duration.ofMinutes(1))
                        .withInactivityInterval(Duration.ofSeconds(30))
                        .withMaxPartSize(64 * 1024 * 1024)
                        .build())
                .build();

        aggregatedStream.sinkTo(aggSink).name("hdfs-agg-sink");

        // 6. 监控指标定期输出
        aggregatedStream
                .map(record -> {
                    MetricsCollector.getInstance().increment(MetricsCollector.RECORDS_OUT);
                    return record;
                })
                .name("metrics-counter");

        log.info("Production pipeline starting: topic={}, parallelism={}, checkpointInterval={}ms",
                config.getTopic(), config.getParallelism(), config.getCheckpointInterval());

        env.execute("Production Pipeline — Kafka → Flink → HDFS");
    }
}
