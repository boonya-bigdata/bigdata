\# Skill: 技术栈约束



\## Purpose

确保所有代码生成和调试操作严格遵循项目技术栈约束。



\## Trigger Conditions

激活条件：生成代码、调试 Docker/环境问题、修改构建配置。



\## Constraints



\### JDK 版本

\- 始终使用 Java 17

\- 永远不要尝试降级到 Java 11 或升级到 Java 21

\- 如果遇到 JDK 相关报错，优先检查 JAVA\_HOME 环境变量，不要动版本



\### 构建工具

\- 使用 Maven 3.9+ 或 Gradle 8.5+

\- 不切换构建工具类型



\### 依赖管理

\- Spring Boot 使用 3.x 系列

\- Flink 使用 1.18+，确认与 Java 17 兼容



\## Do This / Not This



\### Do This:

当 Flink 组件起不来时，检查：网络配置、端口映射、内存限制、日志权限。



\### Not This:

❌ 不要执行 `apt-get install openjdk-11-jdk`

❌ 不要修改 `Dockerfile` 中的 `FROM openjdk:11`（我们的镜像是 17）

❌ 不要在 `pom.xml` 中降级 `<java.version>`

