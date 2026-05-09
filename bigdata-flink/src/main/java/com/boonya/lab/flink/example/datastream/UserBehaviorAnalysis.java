package com.boonya.lab.flink.example.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Properties;

/**
 * 实时用户行为日志分析 —— 从 Kafka 读取日志，按 5 分钟滚动窗口统计每位用户的点击/浏览/购买次数。
 */
public class UserBehaviorAnalysis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("user-behavior-log")
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<UserBehavior> behaviors = env.fromSource(source,
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> {
                                    String[] fields = event.split(",");
                                    return Long.parseLong(fields[3]);
                                }),
                        "Kafka Source")
                .map(new LogParser());

        DataStream<UserStatistics> stats = behaviors
                .keyBy(b -> b.userId)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .reduce(new BehaviorReduceFunction())
                .map(b -> new UserStatistics(b.userId, b.clickCount, b.viewCount,
                        b.purchaseCount, b.timestamp, 0, 0));

        stats.print().name("Console Sink");

        env.execute("User Behavior Analysis Job");
    }

    // --- Model ---

    public static class UserBehavior {
        public String userId;
        public String action;
        public String itemId;
        public long timestamp;
        public int clickCount;
        public int viewCount;
        public int purchaseCount;

        public UserBehavior() {}

        public UserBehavior(String userId, String action, String itemId, long timestamp) {
            this.userId = userId;
            this.action = action;
            this.itemId = itemId;
            this.timestamp = timestamp;
            this.clickCount = "click".equals(action) ? 1 : 0;
            this.viewCount = "view".equals(action) ? 1 : 0;
            this.purchaseCount = "purchase".equals(action) ? 1 : 0;
        }
    }

    public static class UserStatistics {
        public String userId;
        public int clickCount;
        public int viewCount;
        public int purchaseCount;
        public long lastAccessTime;
        public long windowStart;
        public long windowEnd;

        public UserStatistics() {}

        public UserStatistics(String userId, int clickCount, int viewCount,
                              int purchaseCount, long lastAccessTime, long windowStart, long windowEnd) {
            this.userId = userId;
            this.clickCount = clickCount;
            this.viewCount = viewCount;
            this.purchaseCount = purchaseCount;
            this.lastAccessTime = lastAccessTime;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return String.format("UserStatistics{userId='%s', click=%d, view=%d, purchase=%d, lastAccess=%d}",
                    userId, clickCount, viewCount, purchaseCount, lastAccessTime);
        }
    }

    // --- Functions ---

    /** 将逗号分隔的日志行解析为 UserBehavior。 */
    public static class LogParser implements MapFunction<String, UserBehavior> {
        @Override
        public UserBehavior map(String line) {
            String[] fields = line.split(",");
            return new UserBehavior(fields[0], fields[1], fields[2], Long.parseLong(fields[3]));
        }
    }

    /** 合并同一窗口内同一用户的多条行为记录。 */
    public static class BehaviorReduceFunction implements ReduceFunction<UserBehavior> {
        @Override
        public UserBehavior reduce(UserBehavior a, UserBehavior b) {
            UserBehavior merged = new UserBehavior();
            merged.userId = a.userId;
            merged.clickCount = a.clickCount + b.clickCount;
            merged.viewCount = a.viewCount + b.viewCount;
            merged.purchaseCount = a.purchaseCount + b.purchaseCount;
            merged.timestamp = Math.max(a.timestamp, b.timestamp);
            return merged;
        }
    }
}
