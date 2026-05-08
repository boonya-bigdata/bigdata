package com.boonya.lab.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "bigdata.kafka")
public class KafkaConfig {

    /** Kafka 集群地址 */
    private String bootstrapServers = "kafka:9092";

    /** 消费者组 ID */
    private String groupId = "bigdata-consumer-group";

    /** 默认 Topic */
    private String topic = "user-events";

    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }
    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }
    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
}
