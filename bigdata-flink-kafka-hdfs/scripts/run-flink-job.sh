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

# 选择运行模式
echo "========================================"
echo "选择 Flink 任务"
echo "========================================"
echo "1) 基础聚合任务 (RealTimeAggregationJob)"
echo "2) 数据倾斜处理任务 (SkewAwareAggregationJob)"
echo "========================================"
read -p "请选择 [1-2]: " choice

case $choice in
    1)
        JOB_CLASS="com.boonya.bigdata.flink.kafka.hdfs.job.RealTimeAggregationJob"
        JOB_NAME="基础聚合任务"
        ;;
    2)
        JOB_CLASS="com.boonya.bigdata.flink.kafka.hdfs.job.SkewAwareAggregationJob"
        JOB_NAME="数据倾斜处理任务"
        ;;
    *)
        echo "无效选择，使用默认: 基础聚合任务"
        JOB_CLASS="com.boonya.bigdata.flink.kafka.hdfs.job.RealTimeAggregationJob"
        JOB_NAME="基础聚合任务"
        ;;
esac

echo ""
echo "========================================"
echo "启动 $JOB_NAME"
echo "========================================"

# 方式1：本地运行（IDE风格）
echo "运行模式: 本地 JVM"
java -cp "$JAR_FILE" "$JOB_CLASS"

# 方式2：提交到 Flink 集群（可选）
# echo "运行模式: Flink 集群"
# docker exec flink-jobmanager flink run -c "$JOB_CLASS" /opt/flink/usrlib/$(basename "$JAR_FILE")