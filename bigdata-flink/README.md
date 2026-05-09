# bigdata-flink — Apache Flink 实战：从入门到生产

> 基于 Apache Flink 1.18.0，覆盖 DataStream API、Table API / SQL、状态管理、窗口、CDC、生产优化等核心主题。

## 模块结构

```
com.boonya.lab.flink
├── FlinkApplication.java              # 应用入口（生产级配置）
└── example/
    ├── config/
    │   └── EnvironmentSetup.java       # 环境搭建（默认 / 自定义配置）
    ├── datastream/
    │   ├── UserBehaviorAnalysis.java   # 实时用户行为日志分析
    │   ├── RealTimeAlertSystem.java    # 实时登录失败告警
    │   └── DataDeduplication.java      # 订单去重（State TTL）
    ├── sql/
    │   ├── FlinkSQLExample.java        # Flink SQL DDL + TUMBLE 窗口聚合
    │   ├── FlinkTableAPIExample.java   # Table API 分组聚合与过滤
    │   └── DimensionTableJoin.java     # 实时维表 Lookup Join
    ├── state/
    │   ├── StateManagementExample.java # 三种 Keyed State 实战
    │   └── CheckpointConfiguration.java# Checkpoint / Savepoint 配置
    ├── window/
    │   └── WindowExample.java          # Tumbling / Sliding / Session 窗口
    ├── cdc/
    │   ├── FlinkCDCExample.java        # MySQL CDC 数据捕获（DataStream）
    │   └── FlinkSQLCDC.java            # Flink SQL CDC（MySQL → ES）
    └── production/
        ├── ProductionConfiguration.java# 生产级环境配置
        ├── DataSkewHandling.java       # 数据倾斜处理
        ├── CustomMetrics.java          # 自定义 Metrics 监控
        └── TroubleshootingGuide.java   # 故障排查指南
```

## 技术栈

| 技术 | 版本 |
|------|------|
| Apache Flink | 1.18.0 |
| Flink Kafka Connector | 1.17.1 |
| Flink JDBC Connector | 1.18.0 |
| Hadoop | 3.3.4 |
| FastJSON | 2.0.43 |
| MySQL Connector | 8.0.33 |
| Java | 17 |

## 依赖说明

核心 Flink API 依赖使用 `provided` scope（集群提供，不打包进 Fat JAR）。连接器依赖默认 `compile` scope（打包进 JAR）：

```xml
<!-- Flink 核心 — provided（集群运行时提供） -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java</artifactId>
    <version>1.18.0</version>
    <scope>provided</scope>
</dependency>

<!-- 连接器 — compile（打包进应用 JAR） -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka</artifactId>
    <version>1.17.1</version>
</dependency>
```

---

## Docker 部署（推荐）

### 架构

```
docker-compose services:
┌──────────────┐   ┌──────────────┐   ┌───────────┐   ┌──────────┐
│  jobmanager  │   │  taskmanager  │   │   kafka   │   │  mysql   │
│  :8081 (UI)  │◄──│  (worker)    │   │  :9092    │   │  :3306   │
└──────────────┘   └──────────────┘   └───────────┘   └──────────┘
       │                                        │
       └───── job-submitter ────────────────────┘
             (jar 挂载到 /opt/flink/usrlib)
```

### 1. 构建 Fat JAR

```bash
# 在项目根目录执行
mvn clean package -pl bigdata-flink -DskipTests
```

### 2. 启动集群

```bash
cd bigdata-flink
docker compose up -d
```

启动后验证：

```bash
# 检查服务状态
docker compose ps

# Flink Web UI: http://localhost:8081
# Kafka: localhost:9092
# MySQL: localhost:3306 (root/root, database: flink_db)
```

### 3. 创建 Kafka Topic（如需要）

```bash
# 创建用户行为日志 topic
docker compose exec kafka kafka-topics --create \
  --topic user-behavior-log \
  --bootstrap-server kafka:29092 \
  --partitions 4 \
  --replication-factor 1
  
# 或者 windows
docker compose exec kafka kafka-topics --create   --topic user-behavior-log   --bootstrap-server kafka:29092   --partitions 4   --replication-factor 1

# 查看所有 topic
docker compose exec kafka kafka-topics --list --bootstrap-server kafka:29092
```

### 4. 初始化 MySQL 表（如需要）

```bash
# 进入 MySQL
docker compose exec mysql mysql -uroot -proot flink_db

# 建表（SQL / JDBC 示例用）
CREATE TABLE user_behavior_count (
  user_id VARCHAR(64),
  behavior VARCHAR(32),
  cnt BIGINT,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  PRIMARY KEY (user_id, behavior, window_start)
);

CREATE TABLE user_dim (
  user_id VARCHAR(64) PRIMARY KEY,
  user_name VARCHAR(64),
  age INT,
  city VARCHAR(64),
  level VARCHAR(32)
);
```

### 5. 提交作业到集群

**方式一：docker compose（自动提交）**

```bash
docker compose run --rm job-submitter
```

**方式二：通过 Flink CLI**

```bash
docker compose exec jobmanager flink run -d \
  -c com.boonya.lab.flink.FlinkApplication \
  /opt/flink/usrlib/bigdata-flink-1.0.0-SNAPSHOT.jar
```

**方式三：通过 REST API**

```bash
# 上传并运行 jar
curl -X POST http://localhost:8081/jars/bigdata-flink-1.0.0-SNAPSHOT.jar/run \
  -H "Content-Type: application/json" \
  -d '{"entryClass": "com.boonya.lab.flink.FlinkApplication", "parallelism": 4}'
```

**方式四：指定其他示例作为入口**

```bash
# 行为分析
docker compose exec jobmanager flink run -d \
  -c com.boonya.lab.flink.example.datastream.UserBehaviorAnalysis \
  /opt/flink/usrlib/bigdata-flink-1.0.0-SNAPSHOT.jar

# 数据倾斜处理
docker compose exec jobmanager flink run -d \
  -c com.boonya.lab.flink.example.production.DataSkewHandling \
  /opt/flink/usrlib/bigdata-flink-1.0.0-SNAPSHOT.jar
```

### 6. 查看与管理作业

```bash
# 查看运行中的作业
docker compose exec jobmanager flink list

# 取消作业
docker compose exec jobmanager flink cancel <job-id>

# 带 savepoint 停止
docker compose exec jobmanager flink stop --savepointPath /tmp/savepoint <job-id>

# Flink Web UI
open http://localhost:8081
```

### 7. 停止集群

```bash
docker compose down

# 清除数据卷（可选）
docker compose down -v
```

---

## 本地 IDE 运行（开发调试）

部分无需外部依赖的示例可直接运行 `main()`：

| 示例 | 外部依赖 | IDE 可运行 |
|------|---------|-----------|
| `DataSkewHandling` | 无 | 是 |
| `FlinkTableAPIExample` | 无 | 是 |
| `TroubleshootingGuide` | 无 | 是 |
| `StateManagementExample` | socket | 是 (配合 `nc -lk 9999`) |
| `UserBehaviorAnalysis` | Kafka | 需要 Docker Kafka |
| `FlinkSQLExample` | Kafka + MySQL | 需要 Docker |

---

## 示例详解

### 1. 环境搭建（EnvironmentSetup）

两种创建执行环境的方式：`getExecutionEnvironment()` 自动检测，或通过 `Configuration` 设置 TM 内存（2g）、Slot（4）、重启策略（固定延迟 3 次/10s）、Checkpoint（60s / EXACTLY_ONCE）。

### 2. 实时用户行为分析（UserBehaviorAnalysis）

从 Kafka 读取日志，5 分钟滚动窗口统计用户点击/浏览/购买次数。核心：`KafkaSource` → `WatermarkStrategy.forBoundedOutOfOrderness(5s)` → `TumblingEventTimeWindows.of(5min)` → `ReduceFunction`。

### 3. 实时登录失败告警（RealTimeAlertSystem）

`KeyedProcessFunction` + `ValueState` 实现有状态检测。用户 5 分钟内登录失败 ≥3 次触发告警。使用 Event-Time 定时器自动清理过期状态。

### 4. 订单去重（DataDeduplication）

`StateTtlConfig` 设置 1 小时 TTL，`OnCreateAndWrite` 更新策略，`NeverReturnExpired` 可见性。`ValueState<Boolean>` 标记已处理。

### 5. Flink SQL 聚合（FlinkSQLExample）

DDL 创建 Kafka 源表（含计算列 `event_time AS TO_TIMESTAMP(FROM_UNIXTIME(ts))` 和 Watermark）→ TUMBLE 窗口 → JDBC 写入 MySQL。

### 6. Table API（FlinkTableAPIExample）

`DataStream → fromDataStream() → groupBy().select().filter() → toRetractStream()`。

### 7. 维表 Lookup Join（DimensionTableJoin）

Kafka 事实表 + MySQL 维表，`FOR SYSTEM_TIME AS OF` 时态 Join，`lookup.cache.max-rows=5000`。

### 8. 状态管理（StateManagementExample）

三种 Keyed State：`ValueState<Double>`（累计消费）、`ListState<UserAction>`（最近 10 条操作）、`MapState<String, Integer>`（品类计数）。

### 9. Checkpoint 配置（CheckpointConfiguration）

FsStateBackend / RocksDBStateBackend / MemoryStateBackend 对比与生产配置。

### 10. 窗口类型（WindowExample）

Tumbling (1min) / Sliding (5min,1min) / Session (30min gap)，自定义 `AggregateFunction`（增量聚合）和 `ProcessWindowFunction`（全量窗口）。

### 11. Flink CDC（FlinkCDCExample）

MySqlSource 捕获 Binlog → Debezium JSON 解析（FastJSON）→ 按 INSERT/UPDATE/DELETE 分流。

### 12. Flink SQL CDC（FlinkSQLCDC）

`mysql-cdc` connector 源 → `elasticsearch-7` connector 汇，纯 SQL 实时同步。

### 13. 生产环境配置（ProductionConfiguration）

TM 内存 4g、RocksDB 增量、Failure Rate 重启（3次/5min/30s）、Object Reuse 开启。

### 14. 数据倾斜处理（DataSkewHandling）

自定义 Partitioner（热 key 加随机后缀）、两阶段聚合（加盐→去盐）、rebalance。

### 15. 自定义 Metrics（CustomMetrics）

`Counter`、`Meter`（60s 滑动窗口速率）、`Histogram`（`DescriptiveStatisticsHistogram`）监控。

### 16. 故障排查（TroubleshootingGuide）

Checkpoint 超时、背压、OOM、Kafka Lag 的诊断建议。

---

## 参考

- [Apache Flink 官方文档](https://nightlies.apache.org/flink/flink-docs-release-2.3/)
- [Apache Flink 实战博客](https://www.cnblogs.com/clnchanpin/p/19463234)
- [Flink DataStream API](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/datastream/overview/)
- [Flink SQL & Table API](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/table/overview/)
- [Flink CDC 连接器](https://github.com/ververica/flink-cdc-connectors)
