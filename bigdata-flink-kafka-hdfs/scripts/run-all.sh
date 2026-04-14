#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "========================================"
echo "Flink + Kafka + HDFS 完整 Demo"
echo "========================================"

# 1. 编译项目
echo ""
echo "[1/5] 编译项目..."
mvn clean package -DskipTests -q

# 2. 启动环境
echo ""
echo "[2/5] 启动 Docker 环境..."
"$SCRIPT_DIR/start-env.sh"

# 3. 测试连接
echo ""
echo "[3/5] 测试连接..."
"$SCRIPT_DIR/test-env.sh"

# 4. 启动数据生成器（后台）
echo ""
echo "[4/5] 启动数据生成器..."
mkdir -p logs
java -cp target/*.jar com.boonya.bigdata.flink.kafka.hdfs.generator.DataGenerator > logs/generator.log 2>&1 &
GENERATOR_PID=$!
echo "数据生成器 PID: $GENERATOR_PID"
echo "日志文件: logs/generator.log"

# 5. 启动 Flink 任务（后台）
echo ""
echo "[5/5] 启动 Flink 任务..."
java -cp target/*.jar com.boonya.bigdata.flink.kafka.hdfs.job.RealTimeAggregationJob > logs/flink.log 2>&1 &
FLINK_PID=$!
echo "Flink 任务 PID: $FLINK_PID"
echo "日志文件: logs/flink.log"

echo ""
echo "========================================"
echo "✅ Demo 运行中"
echo "========================================"
echo ""
echo "查看日志:"
echo "  数据生成器: tail -f logs/generator.log"
echo "  Flink任务:  tail -f logs/flink.log"
echo ""
echo "停止 Demo:"
echo "  kill $GENERATOR_PID $FLINK_PID"
echo "  ./scripts/stop-env.sh"
echo "========================================"

# 等待用户中断
wait