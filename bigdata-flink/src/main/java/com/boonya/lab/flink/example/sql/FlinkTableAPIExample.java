package com.boonya.lab.flink.example.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Flink Table API 实战 —— DataStream 转 Table，执行分组聚合和过滤，再转回流。
 */
public class FlinkTableAPIExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 模拟 DataStream 输入
        DataStream<Tuple2<String, Integer>> input = env.fromElements(
                new Tuple2<>("Alice", 100),
                new Tuple2<>("Bob", 200),
                new Tuple2<>("Alice", 150),
                new Tuple2<>("Charlie", 300),
                new Tuple2<>("Bob", 50),
                new Tuple2<>("Alice", 80)
        );

        // DataStream -> Table
        Table table = tableEnv.fromDataStream(input, $("name"), $("amount"));

        // Table API 聚合
        Table resultTable = table
                .groupBy($("name"))
                .select(
                        $("name"),
                        $("amount").sum().as("total_amount"),
                        $("amount").avg().as("avg_amount"),
                        $("amount").count().as("count")
                )
                .filter($("total_amount").isGreater(200));

        // Table -> DataStream (retract 流)
        tableEnv.toRetractStream(resultTable, Row.class)
                .print();

        env.execute("Table API Example Job");
    }
}
