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
| 流计算核心 | **Java + Flink 1.20 + Flink CEP** | DataStream API，独立 Standalone 集群（compose 网络，WebUI:8081） |
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

### 3. 构建并启动 Flink Standalone 集群，提交作业

```bash
# 启动集群（JobManager + TaskManager，WebUI: http://localhost:8081）
docker compose up -d flink-jobmanager flink-taskmanager

# 构建作业 jar 并向集群提交
mvn -f core/pom.xml package
docker run --rm --network pyflink-cep-fraud-detection_default \
    -v $(pwd)/core/target:/app \
    -e KAFKA_BOOTSTRAP=kafka:9094 -e DORIS_FE_NODES=doris-fe:8030 \
    flink:1.20.0 \
    /opt/flink/bin/flink run -d -m flink-jobmanager:8081 \
        -c com.fraud.FraudDetectionJob /app/flink-fraud-detection-core-1.0.0.jar
```

> 作业运行在独立集群上，Flink WebUI 可查看 DAG / watermark / 反压 / checkpoint。
> 本机调试也可用 `java -jar core/target/...jar` 起本地 mini-cluster（环境变量默认走 localhost）。

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

**内置 5 类规则模式**（CEP 宽匹配 + 广播状态判定）：

| 规则类型 | CEP 模式 | 判定逻辑 |
|---|---|---|
| `SINGLE_HIGH_AMOUNT` | 单笔候选流 | 单笔超阈值 |
| `CONSECUTIVE_HIGH_AMOUNT` | 连续两笔 | 两笔都超阈值且窗口内 |
| `SEQUENCE_SMALL_LARGE` | 连续两笔 | 首笔小额试探 + 次笔大额 |
| `HIGH_FREQUENCY` | 连续三笔 (`next×3`) | 窗口内高频交易 |
| `IP_CHANGE` | 迭代条件比较 IP | 窗口内 IP/城市切换 |
| `TIMEOUT_ALERT` | `within` 超时侧输出 | 单笔大额无后续交易(超时) |

规则 JSON 契约：

```json
{
  "rule_id": "R004", "rule_name": "高频交易",
  "rule_type": "HIGH_FREQUENCY",
  "threshold": 0, "window_ms": 30000,
  "count": 3, "small_threshold": 0,
  "enabled": true, "version": 2, "weight": 0.7
}
```

## 性能指标（独立集群实测）

| 指标 | 数值 | 说明 |
|---|---|---|
| 告警吞吐 | ~10.1 条/分钟 | 数据源 3 条交易/秒，3 条规则（R001/R002/R003）同时生效 |
| 端到端延迟均值 | 7.6s | ≈ watermark 乱序容忍(10s)，符合预期 |
| 端到端延迟 P50 / P95 / P99 | 8.0 / 11.0 / 11.0s | |
| Checkpoint | ~104KB / 55ms | 5s 间隔，开销可忽略 |

> 延迟主要由 `forBoundedOutOfOrderness(10s)` 决定：要容忍 10s 乱序，
> watermark 就滞后 10s，告警至少要等 watermark 越过匹配末尾才发射。
> 实际生产中可按业务容忍度调整乱序窗口。

## 监控与调试

- **Flink WebUI**：`http://localhost:8081`（作业 DAG、watermark、反压、checkpoint 历史）
- **告警大屏**：`http://localhost:5001`（实时告警、趋势、城市分布、规则排行、延迟指标、网页端规则热更新）
- **规则热更新**：大屏"规则管理"面板或 `tools/rule_producer.py`