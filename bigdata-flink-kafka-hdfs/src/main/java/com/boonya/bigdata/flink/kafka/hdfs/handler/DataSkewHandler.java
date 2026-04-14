package com.boonya.bigdata.flink.kafka.hdfs.handler;

import com.boonya.bigdata.flink.kafka.hdfs.model.UserEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Random;

public class DataSkewHandler {

    // 方法1：加盐打散（两阶段聚合）
    public static DataStream<UserEvent> addSaltAndAggregate(DataStream<UserEvent> input) {
        return input
                // 第一阶段：加盐后局部聚合
                .map(new MapFunction<UserEvent, SaltedEvent>() {
                    private final Random random = new Random();

                    @Override
                    public SaltedEvent map(UserEvent event) throws Exception {
                        int salt = random.nextInt(10);  // 0-9随机盐
                        return new SaltedEvent(event.userId(), salt, event.amount());
                    }
                })
                .keyBy(e -> e.userId + "_" + e.salt)
                .process(new PartialAggregator())

                // 第二阶段：去盐后全局聚合
                .map(e -> new UserEvent(e.userId, null, e.amount, System.currentTimeMillis()))
                .keyBy(UserEvent::userId)
                .process(new GlobalAggregator());
    }

    // 方法2：动态检测数据倾斜并自动处理
    public static class SkewDetector extends KeyedProcessFunction<Integer, UserEvent, UserEvent> {
        private transient MapState<Long, Long> countState;

        //@Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            MapStateDescriptor<Long, Long> descriptor =
                    new MapStateDescriptor<>("counts", Long.class, Long.class);
            countState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(UserEvent event, Context ctx, Collector<UserEvent> out) throws Exception {
            long currentWindow = ctx.timerService().currentProcessingTime() / 60000;
            Long count = countState.get(currentWindow);
            count = count == null ? 1 : count + 1;
            countState.put(currentWindow, count);

            // 检测倾斜：单个key超过阈值
            if (count > 10000) {
                // 触发动态加盐逻辑
                ctx.output(new OutputTag<UserEvent>("skew") {}, event);
            } else {
                out.collect(event);
            }

            ctx.timerService().registerProcessingTimeTimer((currentWindow + 1) * 60000);
        }
    }

    // 辅助类
    static class SaltedEvent {
        int userId;
        int salt;
        double amount;

        SaltedEvent(int userId, int salt, double amount) {
            this.userId = userId;
            this.salt = salt;
            this.amount = amount;
        }
    }

    static class PartialAggregator extends KeyedProcessFunction<String, SaltedEvent, SaltedEvent> {
        private double sum = 0;

        @Override
        public void processElement(SaltedEvent event, Context ctx, Collector<SaltedEvent> out) throws Exception {
            sum += event.amount;
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SaltedEvent> out) throws Exception {
            String key = ctx.getCurrentKey();
            String[] parts = key.split("_");
            out.collect(new SaltedEvent(Integer.parseInt(parts[0]),
                    Integer.parseInt(parts[1]), sum));
        }
    }

    static class GlobalAggregator extends KeyedProcessFunction<Integer, UserEvent, UserEvent> {
        private double total = 0;

        @Override
        public void processElement(UserEvent event, Context ctx, Collector<UserEvent> out) throws Exception {
            total += event.amount();
            out.collect(new UserEvent(ctx.getCurrentKey(), null, total, System.currentTimeMillis()));
        }
    }
}
