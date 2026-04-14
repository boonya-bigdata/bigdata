package com.boonya.bigdata.flink.kafka.hdfs.function;

import com.boonya.bigdata.flink.kafka.hdfs.model.UserEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * 数据倾斜处理器（两阶段聚合）
 *
 * 原理：
 * 1. 第一阶段：加盐打散，局部聚合
 * 2. 第二阶段：去盐合并，全局聚合
 *
 * 适用场景：热点用户（如userId=1）产生大量数据
 */
public class SkewAwareAggregator extends KeyedProcessFunction<String, UserEvent, String> {

    private static final int SALT_BUCKETS = 10;  // 10个盐桶
    private transient MapState<Integer, Double> saltedAmountState;
    private final Random random = new Random();

    // 用于检测热点Key的计数器
    private final Map<Integer, Long> hotKeyDetector = new HashMap<>();
    private static final int HOT_KEY_THRESHOLD = 100;  // 100条/分钟视为热点

    //@Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<Integer, Double> descriptor = new MapStateDescriptor<>(
                "saltedAmount",
                TypeInformation.of(new TypeHint<Integer>() {}),
                TypeInformation.of(new TypeHint<Double>() {})
        );
        saltedAmountState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(UserEvent event, Context ctx, Collector<String> out) throws Exception {
        Integer userId = event.userId();

        // 热点检测
        long currentMinute = ctx.timerService().currentProcessingTime() / 60000;
        String detectorKey = userId + "_" + currentMinute;

        // 如果是热点用户，启用加盐处理
        if (isHotKey(userId)) {
            processWithSalt(event, ctx, out);
        } else {
            // 普通用户，直接聚合
            processWithoutSalt(event, out);
        }

        // 注册定时器，每分钟输出一次聚合结果
        ctx.timerService().registerProcessingTimeTimer((currentMinute + 1) * 60000);
    }

    /**
     * 检测是否是热点Key
     */
    private boolean isHotKey(Integer userId) {
        long count = hotKeyDetector.getOrDefault(userId, 0L);
        if (count > HOT_KEY_THRESHOLD) {
            System.out.printf("⚠️ 检测到热点用户: userId=%d, 频率=%d/分钟%n", userId, count);
            return true;
        }
        return false;
    }

    /**
     * 加盐处理（两阶段聚合）
     */
    private void processWithSalt(UserEvent event, Context ctx, Collector<String> out) throws Exception {
        int salt = random.nextInt(SALT_BUCKETS);
        UserEvent saltedEvent = event.withSalt(salt);
        String saltedKey = saltedEvent.getSaltedKey();

        // 这里实际应该按saltedKey重新分区，简化示例中直接累加
        // 完整实现需要：keyBy(saltedKey) -> 局部聚合 -> 再keyBy(userId) -> 全局聚合

        Double current = saltedAmountState.get(salt);
        if (current == null) current = 0.0;
        saltedAmountState.put(salt, current + event.amount());

        // 定期输出局部聚合结果（简化）
        if (random.nextInt(100) < 10) {
            out.collect(String.format("[局部聚合] 用户[%d]盐桶[%d]累计: %.2f",
                    event.userId(), salt, saltedAmountState.get(salt)));
        }
    }

    /**
     * 无盐处理（普通聚合）
     */
    private void processWithoutSalt(UserEvent event, Collector<String> out) throws Exception {
        Double current = saltedAmountState.get(0);
        if (current == null) current = 0.0;
        saltedAmountState.put(0, current + event.amount());

        out.collect(String.format("用户[%d]累计金额: %.2f", event.userId(), saltedAmountState.get(0)));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 每分钟输出一次全局聚合结果
        for (Map.Entry<Integer, Double> entry : saltedAmountState.entries()) {
            out.collect(String.format("[全局聚合] 盐桶[%d]总金额: %.2f", entry.getKey(), entry.getValue()));
        }
        // 注意：生产环境需要清理状态，避免无限增长
    }
}