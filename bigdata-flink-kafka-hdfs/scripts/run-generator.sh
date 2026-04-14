#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

# 查找 JAR 文件
JAR_FILE=$(find target -name "*.jar" -not -name "*-sources.jar" -not -name "*-javadoc.jar" 2>/dev/null | head -1)

if [ -z "$JAR_FILE" ]; then
    echo "❌ 未找到 JAR 文件，请先运行: mvn clean package"
    exit 1
fi

echo "========================================"
echo "启动数据生成器"
echo "========================================"
echo "JAR: $JAR_FILE"
echo "Kafka: localhost:9092"
echo "Topic: user-events"
echo ""
echo "按 Ctrl+C 停止"
echo "========================================"

java -cp "$JAR_FILE" \
    com.boonya.bigdata.flink.kafka.hdfs.generator.DataGenerator