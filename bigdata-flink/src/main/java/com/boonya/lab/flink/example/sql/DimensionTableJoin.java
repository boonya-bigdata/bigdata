package com.boonya.lab.flink.example.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 实时维表 JOIN —— Kafka 事实表 + MySQL 维表，使用 FOR SYSTEM_TIME AS OF 实现 lookup join。
 */
public class DimensionTableJoin {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // --- Kafka 事实表 (订单) ---
        tableEnv.executeSql("CREATE TABLE orders (\n"
                + "  order_id    STRING,\n"
                + "  user_id     STRING,\n"
                + "  product_id  STRING,\n"
                + "  amount      DECIMAL(10,2),\n"
                + "  order_time  TIMESTAMP(3),\n"
                + "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND\n"
                + ") WITH (\n"
                + "  'connector' = 'kafka',\n"
                + "  'topic'     = 'orders',\n"
                + "  'properties.bootstrap.servers' = 'localhost:9092',\n"
                + "  'scan.startup.mode' = 'latest-offset',\n"
                + "  'format'     = 'json'\n"
                + ")");

        // --- MySQL 维表 (用户) ---
        tableEnv.executeSql("CREATE TABLE user_dim (\n"
                + "  user_id   STRING,\n"
                + "  user_name STRING,\n"
                + "  age       INT,\n"
                + "  city      STRING,\n"
                + "  level     STRING,\n"
                + "  PRIMARY KEY (user_id) NOT ENFORCED\n"
                + ") WITH (\n"
                + "  'connector'      = 'jdbc',\n"
                + "  'url'            = 'jdbc:mysql://localhost:3306/flink_db',\n"
                + "  'table-name'     = 'user_dim',\n"
                + "  'username'       = 'root',\n"
                + "  'password'       = 'root',\n"
                + "  'lookup.cache.max-rows' = '5000',\n"
                + "  'lookup.cache.ttl'      = '10min'\n"
                + ")");

        // --- Lookup Join 查询 ---
        tableEnv.executeSql("SELECT\n"
                + "  o.order_id,\n"
                + "  o.user_id,\n"
                + "  u.user_name,\n"
                + "  u.city,\n"
                + "  u.level,\n"
                + "  o.product_id,\n"
                + "  o.amount,\n"
                + "  o.order_time\n"
                + "FROM orders AS o\n"
                + "LEFT JOIN user_dim FOR SYSTEM_TIME AS OF o.order_time AS u\n"
                + "  ON o.user_id = u.user_id");

        System.out.println("Dimension table join job submitted.");
    }
}
