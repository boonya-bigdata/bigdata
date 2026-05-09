package com.boonya.lab.flink.example.production;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 数据倾斜处理 —— 三种常用方案：自定义分区器、两阶段聚合、rebalance。
 */
public class DataSkewHandling {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("popular_item", 1),
                Tuple2.of("normal_1", 1),
                Tuple2.of("popular_item", 2),
                Tuple2.of("normal_2", 1),
                Tuple2.of("popular_item", 1),
                Tuple2.of("normal_3", 2)
        );

        // 方案 1：自定义分区器为热 key 加随机后缀
        input.partitionCustom(new CustomPartitioner(), t -> t.f0)
                .keyBy(t -> t.f0)
                .sum(1)
                .print("CustomPartitioner");

        // 方案 2：两阶段聚合
        twoPhaseAggregation(input).print("TwoPhase");

        // 方案 3：rebalance 后再 keyBy
        useRebalance(input).print("Rebalance");

        env.execute("Data Skew Handling Job");
    }

    /** 自定义分区器 —— 热 key "popular_item" 加 0-9 随机后缀分散到不同分区。 */
    public static class CustomPartitioner implements Partitioner<String> {
        @Override
        public int partition(String key, int numPartitions) {
            if ("popular_item".equals(key)) {
                int suffix = ThreadLocalRandom.current().nextInt(10);
                return Math.abs((key + "_" + suffix).hashCode()) % numPartitions;
            }
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    /** 两阶段聚合 —— 第一阶段加盐分散，第二阶段去盐汇总。 */
    public static DataStream<Tuple2<String, Integer>> twoPhaseAggregation(
            DataStream<Tuple2<String, Integer>> input) {
        // Phase 1: 加盐聚合
        DataStream<Tuple2<String, Integer>> phase1 = input
                .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) {
                        int salt = ThreadLocalRandom.current().nextInt(10);
                        return Tuple2.of(value.f0 + "_" + salt, value.f1);
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1);

        // Phase 2: 去盐聚合
        return phase1
                .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) {
                        String originalKey = value.f0.split("_")[0];
                        return Tuple2.of(originalKey, value.f1);
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1);
    }

    /** 使用 rebalance 轮询分发后再 keyBy，缓解热点倾斜。 */
    public static DataStream<Tuple2<String, Integer>> useRebalance(
            DataStream<Tuple2<String, Integer>> input) {
        return input.rebalance()
                .keyBy(t -> t.f0)
                .sum(1);
    }
}
