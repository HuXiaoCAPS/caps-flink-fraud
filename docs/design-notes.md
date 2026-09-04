# 技术难点与后续展望

## 一、核心难点与解决方案

### 1. CEP Pattern 无法运行时重建
Flink CEP 的 `Pattern` 在作业构建期编译成 NFA，运行期不可改。
**方案**：CEP 只做"宽匹配"（任意连续两笔交易，`within` 设 120s 上限），
`DynamicRuleProcessor`（`KeyedBroadcastProcessFunction`）从广播状态读取最新规则，
在 `processElement` 中按阈值 / 窗口 / 启停开关判定。阈值、窗口、开关全部可热更新。

### 2. 空闲分区卡死 Watermark
单分区 topic + 并行度>1 时，空闲 subtask 的 watermark 通道永不前进，
下游 CEP 的 watermark = min(所有通道) 被卡住，事件时间模式下**永远不发射匹配**。
**方案**：`WatermarkStrategy.withIdleness(5s)` 将空闲分区标记为 idle。

### 3. 乱序数据处理
模拟器按 30% 概率将交易延迟 3~10 秒发送制造乱序，
作业用 `forBoundedOutOfOrderness(10s)` 容忍乱序。
代价是告警端到端延迟约等于 watermark 滞后（见 README 性能指标）。

### 4. Doris 内存限制
Doris 2.1 镜像入口脚本向 be.conf 追加 `priority_networks` 时未换行，
会把 `mem_limit = 2G` 拼成坏行导致内存限制失效。
**方案**：be.conf 末尾补换行 + 限制 `mem_limit = 2G`（26G 内存环境）。

### 5. JDBC 连接器 API 变动（早期 JDBC 方案遗留，现已被 Stream Load 替代）
flink-connector-jdbc 3.4.0 旧版 `JdbcConnectionOptions.builder()` 已移除，
新版用 `JdbcSink.builder()` + `new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()`。

### 6. Doris Stream Load 的网络可达性
Stream Load 的 FE 会返回 307 重定向，要求客户端**直连 BE 内网地址**
（如 `172.28.0.11:8040`）。host 与 docker 桥接网络无路由时必然超时。
**方案**：作业容器化，运行在同一 compose 网络内（Stream Load 直连 BE 可达）。
由此还引出：Kafka 需配置**双监听**（`localhost:9092` 供 host 客户端 + `kafka:9094` 供容器内作业），
否则容器内作业拿到的广告地址是 `localhost` 会连到自己。

### 7. Doris 唯一键表 + Stream Load 的隐藏列
Doris 表用 `UNIQUE KEY(rule_id, user_id, window_start)` 实现幂等去重；
flink-doris-connector 对 UNIQUE 表默认追加 `__DORIS_DELETE__` 隐藏列
（`executionOptions.getDeletable()` 默认 true），需 `setDeletable(false)`，
否则 CSV 列数(11) 与预期(12) 不符导致整批被过滤。
序列化：`SimpleStringSerializer` 走 CSV，按表列序输出 tab 分隔行。

### 8. pymysql 的 `%` 转义坑
pymysql 会对 SQL 做 `%` 格式化，`DATE_FORMAT('%H:%i')` 会报错。
**方案**：时间分桶改在 Python 侧完成。

### 9. 独立集群的槽位与时区
TaskManager 默认 1 槽位，而作业并行度 2 → `NoResourceAvailableException`，
作业反复重启。**方案**：`taskmanager.numberOfTaskSlots=2`。
另：容器 JVM 默认 UTC 时区，告警时间戳差 8 小时 → 设置 `TZ=Asia/Shanghai`。

## 二、后续展望（简历可写方向）

- 开启 flink-doris-connector 的 **2PC**（两阶段提交），从"唯一键幂等"升级为事务级 exactly-once
- 规则版本管理与灰度（`version` 字段已预留）
- Flink on YARN/K8s 部署 + 高可用（HA、多 JobManager）
- CEP 超时部分匹配告警（`within` 到期未成对）
- Checkpoint 持久化到文件系统，支持作业从 checkpoint 恢复