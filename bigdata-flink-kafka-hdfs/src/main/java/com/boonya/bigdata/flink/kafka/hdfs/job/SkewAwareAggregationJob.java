package com.boonya.bigdata.flink.kafka.hdfs.job;

import com.boonya.bigdata.flink.kafka.hdfs.model.UserEvent;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SkewAwareAggregationJob {

    private static final Logger LOG = LoggerFactory.getLogger(SkewAwareAggregationJob.class);
    private static final OutputTag<String> ERROR_TAG = new OutputTag<>("errors") {};

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setString("rest.bind-port", "8081-8090");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(4);

        env.enableCheckpointing(60000);

        String bootstrapServers = getParam(args, "kafka.bootstrap.servers", "localhost:9092");
        String topic = getParam(args, "kafka.topic", "user-events");
        String groupId = getParam(args, "kafka.group.id", "flink-skew-group");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(kafkaSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<UserEvent> eventStream = kafkaStream
                .process(new JsonParserFunction())
                .name("JSON Parser");

        eventStream.getSideOutput(ERROR_TAG)
                .print("ERROR DATA");

        DataStream<String> aggregatedStream = eventStream
                .keyBy(UserEvent::userId)
                .process(new SkewAwareAggregator())
                .name("Skew-Aware Aggregator");

        aggregatedStream.print("倾斜处理结果");

        String hdfsPath = getParam(args, "hdfs.path", "hdfs://localhost:9000/user/flink/events-skew");
        FileSink<String> hdfsSink = FileSink
                .<String>forRowFormat(new Path(hdfsPath), new SimpleStringEncoder<>("UTF-8"))
                .build();

        eventStream
                .map(UserEvent::toCsv)
                .sinkTo(hdfsSink);

        LOG.info("启动倾斜感知Flink任务");
        env.execute("Flink Skew-Aware Aggregation Job");
    }

    private static class JsonParserFunction extends ProcessFunction<String, UserEvent> {
        @Override
        public void processElement(String json, Context ctx, Collector<UserEvent> out) {
            try {
                UserEvent event = UserEvent.fromJson(json);
                if (event.userId() != null && event.userId() > 0) {
                    out.collect(event);
                } else {
                    ctx.output(ERROR_TAG, "Invalid user_id: " + json);
                }
            } catch (Exception e) {
                ctx.output(ERROR_TAG, "Parse error: " + json + ", " + e.getMessage());
            }
        }
    }

    private static class SkewAwareAggregator extends org.apache.flink.streaming.api.functions.KeyedProcessFunction<Integer, UserEvent, String> {
        private java.util.Map<Integer, Double> amountMap = new java.util.HashMap<>();

        @Override
        public void processElement(UserEvent event, Context ctx, Collector<String> out) {
            Integer userId = event.userId();
            Double currentAmount = amountMap.getOrDefault(userId, 0.0);
            Double newAmount = currentAmount + event.amount();
            amountMap.put(userId, newAmount);

            out.collect(String.format("用户[%d]累计金额: %.2f", userId, newAmount));
        }
    }

    private static String getParam(String[] args, String key, String defaultValue) {
        for (String arg : args) {
            if (arg.startsWith("--" + key + "=")) {
                return arg.substring(("--" + key + "=").length());
            }
        }
        return System.getProperty(key, defaultValue);
    }
}
