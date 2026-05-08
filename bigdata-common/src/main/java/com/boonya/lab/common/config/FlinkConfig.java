package com.boonya.lab.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "bigdata.flink")
public class FlinkConfig {

    /** 默认并行度 */
    private int parallelism = 1;

    /** Checkpoint 间隔 (毫秒) */
    private long checkpointInterval = 60000;

    /** HDFS 输出路径 */
    private String hdfsOutputPath = "hdfs://namenode:9000/flink/output";

    public int getParallelism() { return parallelism; }
    public void setParallelism(int parallelism) { this.parallelism = parallelism; }
    public long getCheckpointInterval() { return checkpointInterval; }
    public void setCheckpointInterval(long checkpointInterval) { this.checkpointInterval = checkpointInterval; }
    public String getHdfsOutputPath() { return hdfsOutputPath; }
    public void setHdfsOutputPath(String hdfsOutputPath) { this.hdfsOutputPath = hdfsOutputPath; }
}
