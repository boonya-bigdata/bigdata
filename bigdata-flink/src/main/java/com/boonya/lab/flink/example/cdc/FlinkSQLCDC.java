package com.boonya.lab.flink.example.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink SQL CDC —— 使用 mysql-cdc 连接器将 MySQL 表实时同步到 Elasticsearch。
 */
public class FlinkSQLCDC {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // --- MySQL CDC 源表 ---
        tableEnv.executeSql("CREATE TABLE mysql_users (\n"
                + "  id          BIGINT PRIMARY KEY NOT ENFORCED,\n"
                + "  name        STRING,\n"
                + "  age         INT,\n"
                + "  email       STRING,\n"
                + "  create_time TIMESTAMP(3),\n"
                + "  update_time TIMESTAMP(3)\n"
                + ") WITH (\n"
                + "  'connector'  = 'mysql-cdc',\n"
                + "  'hostname'   = 'localhost',\n"
                + "  'port'       = '3306',\n"
                + "  'username'   = 'root',\n"
                + "  'password'   = 'root',\n"
                + "  'database-name' = 'my_database',\n"
                + "  'table-name'    = 'users',\n"
                + "  'scan.startup.mode' = 'initial'\n"
                + ")");

        // --- Elasticsearch 结果表 ---
        tableEnv.executeSql("CREATE TABLE es_users (\n"
                + "  id          BIGINT PRIMARY KEY NOT ENFORCED,\n"
                + "  name        STRING,\n"
                + "  age         INT,\n"
                + "  email       STRING,\n"
                + "  create_time TIMESTAMP(3),\n"
                + "  update_time TIMESTAMP(3)\n"
                + ") WITH (\n"
                + "  'connector'  = 'elasticsearch-7',\n"
                + "  'hosts'      = 'http://localhost:9200',\n"
                + "  'index'      = 'users'\n"
                + ")");

        // --- 实时同步 ---
        tableEnv.executeSql("INSERT INTO es_users SELECT * FROM mysql_users");

        System.out.println("Flink SQL CDC job submitted: mysql_users -> es_users");
    }
}
