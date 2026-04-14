package com.boonya.bigdata.flink.kafka.hdfs.generator;

import com.boonya.bigdata.flink.kafka.hdfs.model.UserEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 独立数据生成器
 * 运行方式: java -cp xxx.jar com.boonya.bigdata.flink.kafka.hdfs.generator.DataGenerator
 * 作用: 模拟用户行为数据，发送到Kafka
 */
public class DataGenerator {

    private static final String[] PAGES = {"/home", "/pay", "/cart", "/product/123", "/search", "/user/profile"};
    private static final Random random = new Random();

    public static void main(String[] args) {
        String bootstrapServers = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
        String topic = System.getProperty("kafka.topic", "user-events");
        int sleepMs = Integer.getInteger("generate.sleep.ms", 200);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            int eventId = 0;
            System.out.println("========================================");
            System.out.println("数据生成器启动");
            System.out.println("Kafka: " + bootstrapServers);
            System.out.println("Topic: " + topic);
            System.out.println("========================================");

            while (true) {
                // 模拟数据倾斜：用户ID 1 产生30%的数据
                int userId;
                if (random.nextInt(100) < 30) {
                    userId = 1;  // 热点用户
                } else {
                    userId = random.nextInt(100) + 2;  // 2-101
                }

                UserEvent event = new UserEvent(
                        userId,
                        PAGES[random.nextInt(PAGES.length)],
                        Math.round(random.nextDouble() * 1000 * 100.0) / 100.0,
                        System.currentTimeMillis()
                );

                String json = event.toJson();
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, String.valueOf(eventId++), json);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("发送失败: " + exception.getMessage());
                    } else if (metadata != null) {
                        System.out.printf("✓ 发送 [用户:%d, 金额:%.2f] partition=%d offset=%d%n",
                                event.userId(), event.amount(), metadata.partition(), metadata.offset());
                    }
                });

                TimeUnit.MILLISECONDS.sleep(random.nextInt(sleepMs * 2) + 10);
            }
        } catch (InterruptedException e) {
            System.out.println("数据生成器已停止");
            Thread.currentThread().interrupt();
        }
    }
}