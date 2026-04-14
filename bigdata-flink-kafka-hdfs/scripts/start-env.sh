#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "========================================"
echo "启动大数据环境"
echo "========================================"

# 启动容器
docker-compose up -d

# 等待服务就绪
echo "等待服务启动 (30秒)..."
sleep 30

# 创建 Kafka Topic
echo ""
echo "创建 Kafka Topic..."
docker exec kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create \
    --topic user-events \
    --partitions 3 \
    --replication-factor 1 2>/dev/null && echo "✓ Topic 'user-events' 创建成功" || echo "⚠️ Topic 已存在"

# 初始化 HDFS 目录
echo ""
echo "初始化 HDFS 目录..."
docker exec namenode hdfs dfs -mkdir -p /user/flink/events 2>/dev/null || true
docker exec namenode hdfs dfs -chmod 777 /user/flink/events 2>/dev/null || true
echo "✓ HDFS 目录创建成功"

echo ""
echo "========================================"
echo "✅ 环境启动完成！"
echo "========================================"
echo ""
echo "服务地址:"
echo "  Kafka:     localhost:9092 (宿主机) / kafka:29092 (容器)"
echo "  HDFS Web:  http://localhost:9870"
echo "  Flink Web: http://localhost:8081"
echo ""
echo "常用命令:"
echo "  查看日志: docker-compose logs -f"
echo "  停止环境: ./scripts/stop-env.sh"
echo "  测试连接: ./scripts/test-env.sh"
echo "  运行Demo: ./scripts/run-all.sh"
echo "========================================"