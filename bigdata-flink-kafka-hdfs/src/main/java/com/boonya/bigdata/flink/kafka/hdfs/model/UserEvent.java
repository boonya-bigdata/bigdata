package com.boonya.bigdata.flink.kafka.hdfs.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.time.Instant;

/**
 * 用户事件 Record（JDK 17）
 * 同时用于：数据生成、Kafka传输、Flink处理、HDFS存储
 */
public record UserEvent(
        @JsonProperty("user_id") Integer userId,
        @JsonProperty("page") String page,
        @JsonProperty("amount") Double amount,
        @JsonProperty("timestamp") Long timestamp,
        @JsonProperty("salt") Integer salt          // 用于数据倾斜处理
) implements Serializable {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    // 构造函数（无salt版本，兼容旧数据）
    public UserEvent(Integer userId, String page, Double amount, Long timestamp) {
        this(userId, page, amount, timestamp, null);
    }

    // 转换为 CSV 格式（HDFS存储）
    public String toCsv() {
        return String.format("%d,%s,%.2f,%d", userId, page, amount, timestamp);
    }

    // 从 CSV 解析
    public static UserEvent fromCsv(String csv) {
        String[] parts = csv.split(",");
        return new UserEvent(
                Integer.parseInt(parts[0]),
                parts[1],
                Double.parseDouble(parts[2]),
                Long.parseLong(parts[3])
        );
    }

    // 转换为 JSON（Kafka传输）
    public String toJson() {
        try {
            return objectMapper.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize to JSON", e);
        }
    }

    // 从 JSON 解析
    public static UserEvent fromJson(String json) {
        try {
            return objectMapper.readValue(json, UserEvent.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JSON: " + json, e);
        }
    }

    // 加盐（用于数据倾斜处理）
    public UserEvent withSalt(int salt) {
        return new UserEvent(this.userId, this.page, this.amount, this.timestamp, salt);
    }

    // 获取加盐后的Key
    public String getSaltedKey() {
        if (salt == null) {
            return String.valueOf(userId);
        }
        return userId + "_" + salt;
    }

    @Override
    public String toString() {
        return String.format("UserEvent{userId=%d, page='%s', amount=%.2f, timestamp=%d, salt=%s}",
                userId, page, amount, timestamp, salt);
    }
}