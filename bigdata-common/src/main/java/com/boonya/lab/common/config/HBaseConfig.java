package com.boonya.lab.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "bigdata.hbase")
public class HBaseConfig {

    /** ZooKeeper Quorum */
    private String zkQuorum = "zookeeper:2181";

    /** HBase 在 HDFS 上的根目录 */
    private String rootDir = "hdfs://namenode:9000/hbase";

    public String getZkQuorum() { return zkQuorum; }
    public void setZkQuorum(String zkQuorum) { this.zkQuorum = zkQuorum; }
    public String getRootDir() { return rootDir; }
    public void setRootDir(String rootDir) { this.rootDir = rootDir; }
}
