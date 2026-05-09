package com.boonya.lab.flink.example.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink SQL 实战 —— 通过 DDL 创建 Kafka 源表和 JDBC 结果表，使用 TUMBLE 窗口进行聚合。
 */
public class FlinkSQLExample {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // --- Kafka 源表 DDL ---
        tableEnv.executeSql("CREATE TABLE user_behavior (\n"
                + "  user_id     STRING,\n"
                + "  item_id     STRING,\n"
                + "  category_id STRING,\n"
                + "  behavior    STRING,\n"
                + "  ts          BIGINT,\n"
                + "  event_time  AS TO_TIMESTAMP(FROM_UNIXTIME(ts)),\n"
                + "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND\n"
                + ") WITH (\n"
                + "  'connector' = 'kafka',\n"
                + "  'topic'     = 'user_behavior',\n"
                + "  'properties.bootstrap.servers' = 'localhost:9092',\n"
                + "  'properties.group.id' = 'flink-sql-consumer',\n"
                + "  'scan.startup.mode' = 'latest-offset',\n"
                + "  'format'     = 'json'\n"
                + ")");

        // --- JDBC 结果表 (MySQL) ---
        tableEnv.executeSql("CREATE TABLE user_behavior_count (\n"
                + "  user_id      STRING,\n"
                + "  behavior     STRING,\n"
                + "  cnt          BIGINT,\n"
                + "  window_start TIMESTAMP(3),\n"
                + "  window_end   TIMESTAMP(3),\n"
                + "  PRIMARY KEY (user_id, behavior, window_start) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector'  = 'jdbc',\n"
                + "  'url'        = 'jdbc:mysql://localhost:3306/flink_db',\n"
                + "  'table-name' = 'user_behavior_count',\n"
                + "  'username'   = 'root',\n"
                + "  'password'   = 'root'\n"
                + ")");

        // --- TUMBLE 窗口聚合查询 ---
        tableEnv.executeSql("INSERT INTO user_behavior_count\n"
                + "SELECT\n"
                + "  user_id,\n"
                + "  behavior,\n"
                + "  COUNT(*) AS cnt,\n"
                + "  TUMBLE_START(event_time, INTERVAL '5' MINUTE) AS window_start,\n"
                + "  TUMBLE_END(event_time, INTERVAL '5' MINUTE)   AS window_end\n"
                + "FROM user_behavior\n"
                + "GROUP BY user_id, behavior, TUMBLE(event_time, INTERVAL '5' MINUTE)");

        System.out.println("Flink SQL job submitted: user_behavior -> user_behavior_count");
    }
}
