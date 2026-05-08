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

    // Getters (Spring + CLI 兼容)
    public String getBootstrapServers() { return kafkaBootstrapServers; }
    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public String getTopic() { return kafkaTopic; }
    public String getKafkaTopic() { return kafkaTopic; }
    public String getGroupId() { return kafkaGroupId; }
    public String getKafkaGroupId() { return kafkaGroupId; }
    public String getHdfsOutputPath() { return hdfsPath; }
    public String getHdfsPath() { return hdfsPath; }
    public Long getCheckpointInterval() { return checkpointInterval; }
    public Integer getParallelism() { return parallelism; }

    /**
     * 从命令行参数创建配置（用于非Spring环境）
     */
    public static FlinkConfig fromArgs(String[] args) {
        FlinkConfig config = new FlinkConfig();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--bootstrap-servers": config.kafkaBootstrapServers = args[++i]; break;
                case "--topic": config.kafkaTopic = args[++i]; break;
                case "--group-id": config.kafkaGroupId = args[++i]; break;
                case "--hdfs-path": config.hdfsPath = args[++i]; break;
                case "--parallelism": config.parallelism = Integer.parseInt(args[++i]); break;
                case "--checkpoint-interval": config.checkpointInterval = Long.parseLong(args[++i]); break;
            }
        }
        return config;
    }
}