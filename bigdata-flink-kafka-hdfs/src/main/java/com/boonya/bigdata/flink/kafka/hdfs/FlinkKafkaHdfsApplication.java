package com.boonya.bigdata.flink.kafka.hdfs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Spring Boot 启动类
 *
 * 提供多种启动方式：
 * 1. 直接运行此main方法启动Spring Boot
 * 2. 通过HTTP接口触发Flink任务
 * 3. 独立运行Flink任务（无需Spring Boot）
 */
@SpringBootApplication
@EnableScheduling
public class FlinkKafkaHdfsApplication {

    public static void main(String[] args) {
        SpringApplication.run(FlinkKafkaHdfsApplication.class, args);
        System.out.println("========================================");
        System.out.println("Flink Kafka HDFS Demo 启动成功");
        System.out.println("========================================");
        System.out.println("独立运行Flink任务:");
        System.out.println("  mvn clean package");
        System.out.println("  flink run -c com.boonya.bigdata.flink.kafka.hdfs.job.RealTimeAggregationJob target/xxx.jar");
        System.out.println("========================================");
    }
}