# 简历话术参考

## 一句话项目简介

基于 **Flink CEP + Kafka + Doris** 的实时风控系统：从 Kafka 实时消费交易流，
用 Flink CEP 做复杂事件匹配识别欺诈行为，通过 **Broadcast State 广播流实现规则热更新
（改规则不重启作业）**，告警结果入 Doris，前端 Flask + ECharts 大屏实时展示。

## STAR 拆解

- **S（背景）**：构建实时风控平台，支持规则动态调整。核心诉求是"运营调整规则不需要重启流任务"。
- **T（任务）**：实现交易实时匹配、结果落库、可视化大屏，并解决规则热更新这一行业难点。
- **A（行动）**：
  1. 调研发现 **PyFlink Python API 无 CEP 绑定**（1.17~1.20 均无 `pyflink.cep` 模块），
     果断调整架构：核心作业用 **Java + Flink CEP**，模拟/展示层保留 Python，职责清晰。
  2. 用 CEP 宽匹配 + `KeyedBroadcastProcessFunction` 读广播状态做**阈值/窗口/开关热更新**，
     绕开"CEP Pattern 编译期固化、无法运行时重建"的限制。
  3. 处理乱序数据：`forBoundedOutOfOrderness(10s)` + `withIdleness`，
     解决"单分区 topic + 多并行度导致 watermark 卡死、CEP 永不发射"的经典问题。
  4. 调通 Kafka→Flink→Doris 全链路，本地 mini-cluster 运行，不依赖大集群。
- **R（结果）**：
  - 规则热更新全程**无需重启作业**，实测阈值 900→3000 后告警量明显收敛、停用后立即归零；
  - 端到端告警延迟均值 10.4s（≈乱序容忍窗口，符合预期）、吞吐 4.2 条/分钟；
  - 全链路：Kafka(KRaft) + Doris(FE/BE) + Flink CEP + Flask 大屏四层贯通。

## 可被追问的深度点（面试准备）

1. **为什么用广播状态做规则判定，而不是重建 CEP Pattern？**
   → CEP 的 Pattern 在作业构建期编译为 NFA 状态机，运行期不可改；
     广播状态天然适合"运行期参数更新"，二者结合：CEP 定结构、广播状态定参数。

2. **Broadcast State 与普通状态的区别？**
   → 广播状态在**所有并行实例间完整复制**，每条广播元素都会进到每个实例；
     只能广播 MapStateDescriptor 描述的状态；进程侧只读（ReadOnlyContext）。

3. **watermark 卡死的根因？**
   → 下游 operator 的 watermark 取所有输入通道的最小值；单分区 topic 的第二个
     源 subtask 空闲，其通道永不发 watermark，导致下游永不推进；
     `withIdleness` 让空闲通道一段时间后被视为 idle 从而排除。

4. **为什么端到端延迟≈10s？**
   → `forBoundedOutOfOrderness(10s)` 让 watermark 滞后最大事件时间 10s，
     CEP 要等 watermark 越过匹配末尾才发射；这是"容忍乱序"与"低延迟"的权衡。

5. **事务性/幂等？**
   → 当前 JDBC sink 为 at-least-once，Doris 表主键 id 为自增，未做去重；
     生产可换 `flink-doris-connector`（Stream Load）+ 唯一键去重实现 exactly-once。

6. **为什么不用 PyFlink？**
   → PyFlink 无 Python CEP API；且业界 Flink 生产作业以 Java 为主。
     但数据模拟、规则注入、可视化用 Python 开发效率高，形成"Java 核心 + Python 边缘"分工。

## 关键词

`Flink` `Flink CEP` `DataStream` `Broadcast State` `规则热更新` `Kafka` `Doris`
`Watermark` `乱序处理` `KeyedBroadcastProcessFunction` `ECharts 大屏` `Java`