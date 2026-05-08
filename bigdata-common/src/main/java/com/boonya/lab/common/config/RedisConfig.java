package com.boonya.lab.common.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "bigdata.redis")
public class RedisConfig {

    /** Redis 地址 */
    private String host = "redis";

    /** Redis 端口 */
    private int port = 6379;

    /** Redis 密码 */
    private String password = "admin";

    /** 最大连接数 */
    private int maxTotal = 20;

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }
    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }
    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }
    public int getMaxTotal() { return maxTotal; }
    public void setMaxTotal(int maxTotal) { this.maxTotal = maxTotal; }
}
