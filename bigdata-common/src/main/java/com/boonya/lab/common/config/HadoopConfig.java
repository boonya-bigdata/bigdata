package com.boonya.lab.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "bigdata.hadoop")
public class HadoopConfig {

    /** HDFS NameNode 地址，默认 namenode:9000 */
    private String fsDefaultFs = "hdfs://namenode:9000";

    /** Hadoop 用户 */
    private String userName = "root";

    /** Hadoop 安装目录 */
    private String homeDir = "/opt/hadoop";

    public String getFsDefaultFs() { return fsDefaultFs; }
    public void setFsDefaultFs(String fsDefaultFs) { this.fsDefaultFs = fsDefaultFs; }
    public String getUserName() { return userName; }
    public void setUserName(String userName) { this.userName = userName; }
    public String getHomeDir() { return homeDir; }
    public void setHomeDir(String homeDir) { this.homeDir = homeDir; }
}
