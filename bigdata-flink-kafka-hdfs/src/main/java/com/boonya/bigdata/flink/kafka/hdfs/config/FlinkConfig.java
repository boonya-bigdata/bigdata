package com.boonya.bigdata.flink.kafka.hdfs.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * Flink配置类
 * 从 application.yml 读取配置
 */
@Configuration
public class FlinkConfig {

    @Value("${flink.kafka.bootstrap-servers:localhost:9092}")
    private String kafkaBootstrapServers;

    @Value("${flink.kafka.topic:user-events}")
    private String kafkaTopic;

    @Value("${flink.kafka.group-id:flink-consumer-group}")
    private String kafkaGroupId;

    @Value("${flink.hdfs.path:hdfs://localhost:9000/user/flink/events}")
    private String hdfsPath;

    @Value("${flink.checkpoint.interval:60000}")
    private Long checkpointInterval;

    @Value("${flink.parallelism:1}")
    private Integer parallelism;

    // Getters
    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public String getKafkaTopic() { return kafkaTopic; }
    public String getKafkaGroupId() { return kafkaGroupId; }
    public String getHdfsPath() { return hdfsPath; }
    public Long getCheckpointInterval() { return checkpointInterval; }
    public Integer getParallelism() { return parallelism; }
}