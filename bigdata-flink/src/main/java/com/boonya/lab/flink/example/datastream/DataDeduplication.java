package com.boonya.lab.flink.example.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 订单去重 —— 基于 orderId 的去重，使用带 TTL 的 ValueState 自动过期。
 */
public class DataDeduplication {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<Order> orders = env.socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] f = line.split(",");
                    return new Order(f[0], f[1], Double.parseDouble(f[2]), Long.parseLong(f[3]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((o, ts) -> o.timestamp));

        orders.keyBy(o -> o.orderId)
                .process(new DeduplicationFunction(3600)) // 1 hour TTL
                .print();

        env.execute("Order Deduplication Job");
    }

    // --- Model ---

    public static class Order {
        public String orderId;
        public String userId;
        public double amount;
        public long timestamp;

        public Order() {}

        public Order(String orderId, String userId, double amount, long timestamp) {
            this.orderId = orderId;
            this.userId = userId;
            this.amount = amount;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("Order{id='%s', userId='%s', amount=%.2f}", orderId, userId, amount);
        }
    }

    // --- KeyedProcessFunction ---

    /** 通过键控状态判断订单是否已处理，TTL 自动清理过期状态。 */
    public static class DeduplicationFunction extends KeyedProcessFunction<String, Order, Order> {
        private final int ttlSeconds;
        private ValueState<Boolean> seenState;

        public DeduplicationFunction(int ttlSeconds) {
            this.ttlSeconds = ttlSeconds;
        }

        @Override
        public void open(Configuration parameters) {
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(ttlSeconds))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen", Boolean.class);
            descriptor.enableTimeToLive(ttlConfig);
            seenState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Order order, Context ctx, Collector<Order> out) throws Exception {
            Boolean seen = seenState.value();
            if (seen == null || !seen) {
                seenState.update(true);
                out.collect(order);
            }
        }
    }
}
