package com.boonya.lab.flink.example.production;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 自定义 Metrics 监控 —— Counter、Meter、Histogram 的注册和使用。
 */
public class CustomMetrics {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<Event> events = env.socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] f = line.split(",");
                    return new Event(f[0], f[1], Long.parseLong(f[2]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((e, ts) -> e.timestamp));

        events.process(new MetricsProcessFunction()).print();

        env.execute("Custom Metrics Job");
    }

    // --- Model ---

    public static class Event {
        public String key;
        public String data;
        public long timestamp;

        public Event() {}

        public Event(String key, String data, long timestamp) {
            this.key = key;
            this.data = data;
            this.timestamp = timestamp;
        }
    }

    // --- ProcessFunction with Metrics ---

    /** 注册 Counter、Meter、Histogram 指标并在每个事件处理时更新。 */
    public static class MetricsProcessFunction extends ProcessFunction<Event, Event> {

        private transient Counter eventCounter;
        private transient Meter eventMeter;
        private transient Histogram latencyHistogram;

        @Override
        public void open(Configuration parameters) {
            eventCounter = getRuntimeContext().getMetricGroup()
                    .counter("events_processed");

            eventMeter = getRuntimeContext().getMetricGroup()
                    .meter("events_per_second", new MeterView(60));

            // SimpleHistogram 可能已被弃用
            latencyHistogram = getRuntimeContext().getMetricGroup()
                    .histogram("latency", new DescriptiveStatisticsHistogram(3));
        }

        @Override
        public void processElement(Event event, Context ctx, Collector<Event> out) {
            long start = System.currentTimeMillis();

            eventCounter.inc();
            eventMeter.markEvent();

            // 模拟业务处理
            out.collect(event);

            long latency = System.currentTimeMillis() - start;
            latencyHistogram.update(latency);
        }
    }
}
