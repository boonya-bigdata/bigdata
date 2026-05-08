package com.boonya.bigdata.flink.kafka.hdfs.function;

import com.boonya.bigdata.flink.kafka.hdfs.model.UserEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * 数据倾斜感知聚合器（两阶段聚合）
 *
 * 生产级数据倾斜处理方案:
 * 阶段1 — 加盐打散: 热点Key加随机盐(0-9), 将负载分散到SALT_BUCKETS个桶
 * 阶段2 — 去盐合并: 定时器触发全局合并, 汇总所有盐桶的局部聚合结果
 *
 * 触发条件: 单Key每分钟超过100条事件时启用加盐
 * 状态清理: 定时器触发后自动清理, 防止RocksDB无限增长
 */
public class SkewAwareAggregator extends KeyedProcessFunction<String, UserEvent, String> {

    private static final int SALT_BUCKETS = 10;
    private static final int HOT_KEY_THRESHOLD = 100;

    private transient MapState<Integer, Double> saltedAmountState;
    private transient MapState<Integer, Long> keyCountState;
    private final Random random = new Random();

    @Override
    public void open(Configuration parameters) throws Exception {
        saltedAmountState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("saltedAmount", Integer.class, Double.class));
        keyCountState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("keyCount", Integer.class, Long.class));
    }

    @Override
    public void processElement(UserEvent event, Context ctx, Collector<String> out) throws Exception {
        Integer userId = event.userId();
        long currentMinute = ctx.timerService().currentProcessingTime() / 60000;

        Long count = keyCountState.get(userId);
        long newCount = (count == null ? 0 : count) + 1;
        keyCountState.put(userId, newCount);

        if (newCount > HOT_KEY_THRESHOLD) {
            processWithSalt(event, out);
        } else {
            processWithoutSalt(event, out);
        }

        ctx.timerService().registerProcessingTimeTimer((currentMinute + 1) * 60000);
    }

    private void processWithSalt(UserEvent event, Collector<String> out) throws Exception {
        int salt = random.nextInt(SALT_BUCKETS);
        Double current = saltedAmountState.get(salt);
        saltedAmountState.put(salt, (current == null ? 0.0 : current) + event.amount());

        if (random.nextInt(100) < 5) {
            out.collect(String.format("[局部聚合] userId=%d salt[%d]=%.2f",
                    event.userId(), salt, saltedAmountState.get(salt)));
        }
    }

    private void processWithoutSalt(UserEvent event, Collector<String> out) throws Exception {
        Double current = saltedAmountState.get(0);
        saltedAmountState.put(0, (current == null ? 0.0 : current) + event.amount());

        out.collect(String.format("用户[%d]累计金额: %.2f", event.userId(), saltedAmountState.get(0)));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        double total = 0.0;
        for (Double v : saltedAmountState.values()) {
            total += (v != null ? v : 0);
        }
        out.collect(String.format("[全局聚合] 时间窗口总金额: %.2f", total));

        saltedAmountState.clear();
        keyCountState.clear();
    }
}