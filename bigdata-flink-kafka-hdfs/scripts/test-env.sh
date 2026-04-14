#!/bin/bash

echo "========================================"
echo "测试服务连接"
echo "========================================"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 测试 Kafka
echo -n "Kafka: "
if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
    echo -e "${GREEN}✓ 正常${NC}"
else
    echo -e "${RED}✗ 异常${NC}"
fi

# 测试 HDFS
echo -n "HDFS: "
if docker exec namenode hdfs dfs -ls / > /dev/null 2>&1; then
    echo -e "${GREEN}✓ 正常${NC}"
else
    echo -e "${RED}✗ 异常${NC}"
fi

# 测试 Flink
echo -n "Flink: "
if curl -s http://localhost:8081/config > /dev/null 2>&1; then
    echo -e "${GREEN}✓ 正常${NC}"
else
    echo -e "${RED}✗ 异常${NC}"
fi

# 检查 Topic
echo -n "Topic(user-events): "
if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic user-events > /dev/null 2>&1; then
    echo -e "${GREEN}✓ 存在${NC}"
else
    echo -e "${YELLOW}⚠ 不存在${NC}"
fi

# 检查 HDFS 目录
echo -n "HDFS目录(/user/flink/events): "
if docker exec namenode hdfs dfs -test -d /user/flink/events 2>/dev/null; then
    echo -e "${GREEN}✓ 存在${NC}"
else
    echo -e "${YELLOW}⚠ 不存在${NC}"
fi

echo "========================================"