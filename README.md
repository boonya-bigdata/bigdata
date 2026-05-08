# Big Data 生产实战项目

全链路大数据实战平台，覆盖数据采集、实时流处理、离线批处理、数据仓库、推荐系统、机器学习全场景。

## 架构概览

```
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│  数据采集    │   │  实时处理    │   │  离线分析    │
│  Kafka      │ → │  Flink      │ → │  Spark/MapReduce│
└─────────────┘   └──────┬──────┘   └──────┬──────┘
                         │                  │
                         ▼                  ▼
                    ┌─────────┐      ┌──────────┐
                    │  HDFS   │      │  Hive    │
                    └────┬────┘      └────┬─────┘
                         │               │
                         ▼               ▼
                    ┌─────────┐     ┌──────────┐
                    │  HBase  │     │  MySQL   │
                    └────┬────┘     └────┬─────┘
                         │               │
                         ▼               ▼
                    ┌─────────────────────────┐
                    │   Redis (实时查询)       │
                    └─────────────────────────┘
```

## 模块说明

| 模块 | 说明 | 技术栈 |
|------|------|--------|
| bigdata-common | 共享配置模块 | Spring Boot Config |
| bigdata-flink | Flink 流处理基础 | Flink 1.18 |
| bigdata-flink-kafka-hdfs | 实时ETL管道 | Flink + Kafka + HDFS |
| bigdata-spark | Spark 批处理 | Spark 3.5.2 |
| bigdata-hadoop | Hadoop 生态合集 | Hadoop 3.4.0 |
| ├─ hdfs | HDFS 基础操作 | HDFS API |
| ├─ mapreduce | MapReduce 经典案例 | MapReduce |
| ├─ hbase | HBase 客户端操作 | HBase 3.0 |
| ├─ hive | Hive JDBC + UDF | Hive JDBC |
| ├─ spark | Spark on YARN | Spark |
| ├─ logAnalysis | Web日志分析 | MapReduce |
| ├─ itemCF | 协同过滤推荐 | MapReduce |
| └─ spamDetermination | 贝叶斯垃圾检测 | MR + Redis + RPC |
| bigdata-interview | 大数据算法面试题 | Java |

## 快速开始

### 1. 启动基础设施
```bash
docker-compose up -d
```

服务端口映射:
- HDFS NameNode Web UI: http://localhost:9870
- YARN ResourceManager: http://localhost:8088
- Flink Web UI: http://localhost:8081
- Kafka: localhost:9092
- HBase Master: http://localhost:16010
- Redis: localhost:6379 (password: admin)
- MySQL: localhost:3306 (root/root123)
- Grafana: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9090

### 2. 构建项目
```bash
mvn clean package -DskipTests
```

### 3. 运行示例

**Flink 实时聚合:**
```bash
flink run -c com.boonya.bigdata.flink.kafka.hdfs.job.RealTimeAggregationJob \
  bigdata-flink-kafka-hdfs/target/bigdata-flink-kafka-hdfs-1.0.0-SNAPSHOT.jar
```

**MapReduce WordCount:**
```bash
hadoop jar bigdata-hadoop/bigdata-hadoop-3.x/bigdata-hadoop-3.x-mapreduce/target/*.jar \
  com.boonya.lab.hadoop.mapreduce.WordCount /input /output
```

## 环境变量配置

所有配置统一在 `application-shared.yml` 中管理，支持通过环境变量覆盖:

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| BIGDATA_HADOOP_FS_DEFAULT_FS | hdfs://namenode:9000 | HDFS 地址 |
| BIGDATA_KAFKA_BOOTSTRAP_SERVERS | kafka:9092 | Kafka 集群 |
| BIGDATA_REDIS_HOST | redis | Redis 地址 |
| BIGDATA_FLINK_PARALLELISM | 2 | Flink 并行度 |

## 技术要求

- Java 17+
- Maven 3.8+
- Docker & Docker Compose
- 8GB+ 可用内存
