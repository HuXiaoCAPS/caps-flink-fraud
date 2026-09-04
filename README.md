# 基于 Flink CEP 的动态可配置实时风控系统

实时从 Kafka 消费模拟交易数据，基于 **Flink CEP**（Java 核心）做复杂事件匹配，
通过 **Broadcast State 广播流**实现**风控规则热更新（改规则不重启作业）**，
告警结果写入 **Apache Doris**，由 **Flask + ECharts** 大屏实时展示。

```
Kafka ──交易流──▶ Java Flink 作业 ──告警──▶ Doris ──▶ Flask + ECharts 大屏
            │         │ CEP 宽匹配 + Broadcast State 规则判定
            └─规则流──┘  (rule_topic 热更新)
```

## 技术栈

| 层 | 技术 | 说明 |
|---|---|---|
| 流计算核心 | **Java + Flink 1.20 + Flink CEP** | DataStream API，容器化部署（compose 网络） |
| 消息队列 | Apache Kafka 3.7 (KRaft) | Docker 部署，双监听（host 9092 + 容器内 9094） |
| 数据存储 | Apache Doris 2.1.11 (FE+BE) | 告警表 `risk.dws_risk_result`（UNIQUE KEY 幂等去重） |
| Doris 写入 | 官方 `flink-doris-connector`（Stream Load） | 替换 JDBC，生产级导入路径 |
| 数据模拟 | Python Faker + kafka-python | 异常注入 + 乱序注入 |
| 展示层 | Flask + ECharts 5 | 实时告警大屏，支持网页端规则热更新 |

> 说明：PyFlink 的 Python API **不提供 CEP 绑定**（1.17~1.20 均无 `pyflink.cep` 模块），
> 因此核心作业采用 Java 实现，模拟/规则注入/展示等边缘环节用 Python。

## 目录结构

```
├── docker-compose.yml          # Kafka + Doris (FE/BE)
├── conf/                       # Doris FE/BE 配置（含 mem_limit 内存限制）
├── core/                       # Java 核心模块（Flink 作业）
│   ├── pom.xml
│   ├── Dockerfile              # 作业容器镜像（compose 网络内运行）
│   └── src/main/java/com/fraud/
│       ├── FraudDetectionJob.java   # 主入口：交易流 + 规则流 + CEP + 广播状态
│       ├── DynamicRuleProcessor.java # KeyedBroadcastProcessFunction 规则判定
│       ├── Transaction.java          # 交易 POJO
│       ├── Rule.java                 # 规则 POJO
│       ├── RiskAlert.java            # 告警 POJO
│       └── CepTest.java              # CEP 隔离测试
├── tools/                      # Python 辅助工具
│   ├── mock_producer.py        # Faker 模拟交易数据 -> order_topic
│   ├── rule_producer.py        # 发送规则更新 -> rule_topic（热更新入口）
│   ├── simple_consumer.py      # 连通性测试消费端
│   └── setup_doris.py          # Doris 初始化 + 建表
└── dashboard/                  # Flask + ECharts 大屏
    ├── app.py                  # 查询 Doris 的 JSON API
    ├── templates/index.html
    └── static/                 # app.js / style.css / echarts.min.js(本地化)
```

## 快速开始

### 1. 环境准备

```bash
# Docker: Kafka + Doris
docker compose up -d
python3 tools/setup_doris.py            # 等 BE 注册 + 建库表

# Python 工具（Python 3.11）
python3 -m venv .venv
.venv/bin/pip install -r tools/requirements.txt

# Java 构建（JDK 8 + Maven）
mvn -f core/pom.xml package
```

### 2. 启动数据模拟器

```bash
.venv/bin/python tools/mock_producer.py --rate 3
```

### 3. 构建并启动 Flink 作业（容器化，与 Kafka/Doris 同网络）

```bash
mvn -f core/pom.xml package
docker build -f core/Dockerfile -t caps-fraud-job core/
docker run -d --name fraud-job \
    --network pyflink-cep-fraud-detection_default \
    caps-fraud-job
```

> 作业运行在 compose 网络内，可通过服务名访问 Kafka(`kafka:9094`)/Doris(`doris-fe:8030`)。
> 本机调试也可用 `java -jar core/target/...jar`（环境变量默认走 localhost）。

### 4. 下发初始规则（广播状态为空时不会有告警）

```bash
.venv/bin/python tools/rule_producer.py            # R001: threshold=900, 窗口30s
```

### 5. 启动大屏

```bash
.venv/bin/python dashboard/app.py                  # http://localhost:5001
```

## 规则热更新演示（项目核心亮点）

规则存在 Kafka `rule_topic`，通过 Broadcast State 广播到作业所有并行实例。
修改规则**无需重启作业**：

```bash
# 提高阈值（更严）
.venv/bin/python tools/rule_producer.py --threshold 3000 --name "连续大额交易(严)"
# 停用规则（立即停止告警）
.venv/bin/python tools/rule_producer.py --disable
# 恢复启用
.venv/bin/python tools/rule_producer.py
```

规则 JSON 契约：

```json
{
  "rule_id": "R001", "rule_name": "连续大额交易",
  "rule_type": "CONSECUTIVE_HIGH_AMOUNT",
  "threshold": 900.0, "window_ms": 30000,
  "enabled": true, "version": 2
}
```

## 核心难点与解决方案

1. **CEP Pattern 无法运行时重建**
   Flink CEP 的 `Pattern` 在作业构建期编译成 NFA，运行期不可改。
   **方案**：CEP 只做"宽匹配"（任意连续两笔交易，`within` 设 120s 上限），
   `DynamicRuleProcessor`（`KeyedBroadcastProcessFunction`）从广播状态读取最新规则，
   在 `processElement` 中按阈值 / 窗口 / 启停开关判定。阈值、窗口、开关全部可热更新。

2. **空闲分区卡死 Watermark**
   单分区 topic + 并行度>1 时，空闲 subtask 的 watermark 通道永不前进，
   下游 CEP 的 watermark = min(所有通道) 被卡住，事件时间模式下**永远不发射匹配**。
   **方案**：`WatermarkStrategy.withIdleness(5s)` 将空闲分区标记为 idle。

3. **乱序数据处理**
   模拟器按 30% 概率将交易延迟 3~10 秒发送制造乱序，
   作业用 `forBoundedOutOfOrderness(10s)` 容忍乱序。
   代价是告警端到端延迟约等于 watermark 滞后（见性能指标）。

4. **Doris 内存限制**
   Doris 2.1 镜像入口脚本向 be.conf 追加 `priority_networks` 时未换行，
   会把 `mem_limit = 2G` 拼成坏行导致内存限制失效。
   **方案**：be.conf 末尾补换行 + 限制 `mem_limit = 2G`（26G 内存环境）。

5. **JDBC 连接器 API 变动**（早期 JDBC 方案遗留，现已被 Stream Load 替代）
   flink-connector-jdbc 3.4.0 旧版 `JdbcConnectionOptions.builder()` 已移除，
   新版用 `JdbcSink.builder()` + `new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()`。

6. **Doris Stream Load 的网络可达性**
   Stream Load 的 FE 会返回 307 重定向，要求客户端**直连 BE 内网地址**
   （如 `172.28.0.11:8040`）。host 与 docker 桥接网络无路由时必然超时。
   **方案**：作业容器化，运行在同一 compose 网络内（Stream Load 直连 BE 可达）。
   由此还引出：Kafka 需配置**双监听**（`localhost:9092` 供 host 客户端 + `kafka:9094` 供容器内作业），
   否则容器内作业拿到的广告地址是 `localhost` 会连到自己。

7. **Doris 唯一键表 + Stream Load 的隐藏列**
   Doris 表用 `UNIQUE KEY(rule_id, user_id, window_start)` 实现幂等去重；
   flink-doris-connector 对 UNIQUE 表默认追加 `__DORIS_DELETE__` 隐藏列
   （`executionOptions.getDeletable()` 默认 true），需 `setDeletable(false)`，
   否则 CSV 列数(11) 与预期(12) 不符导致整批被过滤。
   序列化：`SimpleStringSerializer` 走 CSV，按表列序输出 tab 分隔行。

8. **pymysql 的 `%` 转义坑**
   pymysql 会对 SQL 做 `%` 格式化，`DATE_FORMAT('%H:%i')` 会报错。
   **方案**：时间分桶改在 Python 侧完成。

## 性能指标（本地 mini-cluster 实测）

| 指标 | 数值 | 说明 |
|---|---|---|
| 告警吞吐 | ~4.2 条/分钟 | 数据源 3 条交易/秒，含 VELOCITY_BURST 注入 |
| 端到端延迟均值 | 10.4s | ≈ watermark 乱序容忍(10s)，符合预期 |
| 端到端延迟 P95 | 11.0s | |
| Checkpoint | ~170KB / 3ms | 5s 间隔，开销可忽略 |
| 单告警状态 | ~34KB | 12 核环境，2 并行度 |

> 延迟主要由 `forBoundedOutOfOrderness(10s)` 决定：要容忍 10s 乱序，
> watermark 就滞后 10s，告警至少要等 watermark 越过匹配末尾才发射。
> 实际生产中可按业务容忍度调整乱序窗口。

## 后续展望（简历可写方向）

- 开启 flink-doris-connector 的 **2PC**（两阶段提交），从"唯一键幂等"升级为事务级 exactly-once
- 多规则支持：同一类型规则可配置多条阈值梯度
- 规则版本管理与灰度（`version` 字段已预留）
- 独立 Flink 集群（Flink on YARN/K8s）替换容器内 mini-cluster，挂载 Flink WebUI
- CEP 超时部分匹配告警（`within` 到期未成对）
- Checkpoint 持久化到文件系统，支持作业从 checkpoint 恢复