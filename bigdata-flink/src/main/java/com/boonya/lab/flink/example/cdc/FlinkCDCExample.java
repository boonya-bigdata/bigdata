package com.boonya.lab.flink.example.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink CDC 实践 —— MySQL Binlog 变化数据捕获与解析。
 *
 * 使用 DataStream API 手动构建 MySqlSource（需要 flink-connector-mysql-cdc 依赖）。
 */
public class FlinkCDCExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // CDC 通常以读 Binlog 为主，单并行度即可

        // MySqlSource 构建（需要 mysql-cdc 连接器依赖）
        // MySqlSource<String> source = MySqlSource.<String>builder()
        //         .hostname("localhost")
        //         .port(3306)
        //         .databaseList("my_database")
        //         .tableList("my_database.users", "my_database.orders")
        //         .username("root")
        //         .password("root")
        //         .startupOptions(StartupOptions.initial())
        //         .deserializer(new JsonDebeziumDeserializationSchema())
        //         .build();

        // 以 socket 模拟 CDC 数据用于演示
        DataStream<String> cdcStream = env.socketTextStream("localhost", 9999)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forMonotonousTimestamps()
                        .withTimestampAssigner((line, ts) -> System.currentTimeMillis()));

        DataStream<ChangeEvent> changes = cdcStream.map(new CDCParser());

        changes.filter(e -> "INSERT".equals(e.operation) || "UPDATE".equals(e.operation))
                .print("Upsert");

        changes.filter(e -> "DELETE".equals(e.operation))
                .print("Delete");

        env.execute("Flink CDC Example Job");
    }

    // --- Model ---

    /** Debezium 格式的变更事件。 */
    public static class ChangeEvent {
        public String database;
        public String table;
        public String operation;  // INSERT, UPDATE, DELETE, READ
        public JSONObject before;
        public JSONObject after;
        public long timestamp;

        public ChangeEvent() {}

        @Override
        public String toString() {
            return String.format("ChangeEvent[%s.%s] op=%s, before=%s, after=%s",
                    database, table, operation, before, after);
        }
    }

    // --- MapFunction ---

    /** 解析 Debezium JSON 格式的 CDC 消息。 */
    public static class CDCParser implements MapFunction<String, ChangeEvent> {

        @Override
        public ChangeEvent map(String line) throws Exception {
            JSONObject root = JSON.parseObject(line);
            JSONObject source = root.getJSONObject("source");

            ChangeEvent event = new ChangeEvent();
            event.database = source.getString("db");
            event.table = source.getString("table");
            event.timestamp = source.getLongValue("ts_ms");

            String op = root.getString("op");
            switch (op) {
                case "c": event.operation = "INSERT"; break;
                case "u": event.operation = "UPDATE"; break;
                case "d": event.operation = "DELETE"; break;
                case "r": event.operation = "READ";   break;
                default:  event.operation = op;       break;
            }

            event.before = root.getJSONObject("before");
            event.after = root.getJSONObject("after");
            return event;
        }
    }
}
