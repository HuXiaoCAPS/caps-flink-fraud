package com.fraud;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 风控作业主入口（阶段三：动态规则 + Broadcast State）
 *
 * 数据链路：
 *   [交易流] Kafka(order_topic) -> JSON 解析 -> Watermark -> keyBy(user_id)
 *           -> CEP 宽匹配(任意连续两笔，上限 120s) -> 候选告警
 *   [规则流] Kafka(rule_topic) -> JSON 解析 -> Broadcast State
 *            └── connect 后由 DynamicRuleProcessor 按最新规则判定
 *   -> 命中的告警写 Doris
 *
 * 关键设计（对应简历难点）：
 *   1. CEP Pattern 在作业构建期编译，无法运行时重建 —— 因此用"宽匹配 + 广播规则判定"，
 *      阈值/窗口/开关全部可通过 rule_topic 热更新，无需重启作业。
 *   2. withIdleness：单分区 topic + 并行度>1 时，空闲 subtask 的水mark 通道会卡死
 *      下游事件时间推进，需将空闲分区标记为 idle。
 */
public class FraudDetectionJob {

    /** 支持双模式部署：容器内(compose 网络)用服务名，本机调试用 localhost */
    private static final String KAFKA_BOOTSTRAP =
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
    private static final String ORDER_TOPIC = "order_topic";
    private static final String RULE_TOPIC = "rule_topic";
    private static final String ORDER_GROUP = "fraud-detection";
    private static final String RULE_GROUP = "fraud-rules";

    /** CEP 宽匹配窗口上限：所有规则的 window_ms 不得超过该值 */
    private static final long MAX_WINDOW_SECONDS = 120;

    private static final String DORIS_FE_NODES =
            System.getenv().getOrDefault("DORIS_FE_NODES", "127.0.0.1:8030");
    private static final String DORIS_USER = "root";
    private static final String DORIS_PASSWORD = "";
    private static final String DORIS_TABLE = "risk.dws_risk_result";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(5000);

        // ============ 1. 交易流：Kafka -> JSON -> Watermark ============
        KafkaSource<String> orderSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(ORDER_TOPIC)
                .setGroupId(ORDER_GROUP)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<Transaction> raw = env.fromSource(
                        orderSource, WatermarkStrategy.noWatermarks(), "kafka-orders")
                .map(json -> MAPPER.readValue(json, Transaction.class))
                .returns(TypeInformation.of(Transaction.class));

        // 容忍 10 秒乱序；withIdleness 处理空闲分区（见类注释）
        WatermarkStrategy<Transaction> watermarkStrategy = WatermarkStrategy
                .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((tx, ts) -> tx.getEventTs())
                .withIdleness(Duration.ofSeconds(5));

        DataStream<Transaction> timed = raw.assignTimestampsAndWatermarks(watermarkStrategy);
        KeyedStream<Transaction, String> keyed = timed.keyBy(Transaction::getUserId);

        // ============ 2. CEP 宽匹配：任意连续两笔交易（结构固定） ============
        Pattern<Transaction, Transaction> pattern = Pattern
                .<Transaction>begin("first", AfterMatchSkipStrategy.skipPastLastEvent())
                .next("second")
                .within(Time.seconds(MAX_WINDOW_SECONDS));

        PatternStream<Transaction> patternStream = CEP.pattern(keyed, pattern);

        DataStream<RiskAlert> pairCandidates = patternStream.select(
                new PatternSelectFunction<Transaction, RiskAlert>() {
                    @Override
                    public RiskAlert select(Map<String, List<Transaction>> match) {
                        Transaction first = match.get("first").get(0);
                        Transaction second = match.get("second").get(0);

                        RiskAlert candidate = new RiskAlert();
                        candidate.setUserId(first.getUserId());
                        candidate.setOrderIds(first.getOrderId() + "," + second.getOrderId());
                        candidate.setTotalAmount(first.getAmount() + second.getAmount());
                        candidate.setCity(first.getCity());
                        candidate.setRiskType("PAIR");
                        candidate.setFirstAmount(first.getAmount());
                        candidate.setSecondAmount(second.getAmount());
                        candidate.setWindowStartTs(first.getEventTs());
                        candidate.setWindowEndTs(second.getEventTs());
                        return candidate;
                    }
                });

        // 连续三笔 CEP（HIGH_FREQUENCY 规则用）
        Pattern<Transaction, Transaction> triplePattern = Pattern
                .<Transaction>begin("t1", AfterMatchSkipStrategy.skipPastLastEvent())
                .next("t2")
                .next("t3")
                .within(Time.seconds(MAX_WINDOW_SECONDS));

        DataStream<RiskAlert> tripleCandidates = CEP.pattern(keyed, triplePattern).select(
                new PatternSelectFunction<Transaction, RiskAlert>() {
                    @Override
                    public RiskAlert select(Map<String, List<Transaction>> match) {
                        Transaction t1 = match.get("t1").get(0);
                        Transaction t2 = match.get("t2").get(0);
                        Transaction t3 = match.get("t3").get(0);

                        RiskAlert candidate = new RiskAlert();
                        candidate.setUserId(t1.getUserId());
                        candidate.setOrderIds(t1.getOrderId() + "," + t2.getOrderId() + "," + t3.getOrderId());
                        candidate.setTotalAmount(t1.getAmount() + t2.getAmount() + t3.getAmount());
                        candidate.setCity(t1.getCity());
                        candidate.setRiskType("TRIPLE");
                        candidate.setFirstAmount(t1.getAmount());
                        candidate.setSecondAmount(t2.getAmount());
                        candidate.setThirdAmount(t3.getAmount());
                        candidate.setWindowStartTs(t1.getEventTs());
                        candidate.setWindowEndTs(t3.getEventTs());
                        return candidate;
                    }
                });

        // IP 切换 CEP（同用户两笔交易、IP 不同）
        Pattern<Transaction, Transaction> ipPattern = Pattern
                .<Transaction>begin("first", AfterMatchSkipStrategy.skipPastLastEvent())
                .next("second")
                .where(new IterativeCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction second,
                                          Context<Transaction> ctx) throws Exception {
                        Iterable<Transaction> firsts = ctx.getEventsForPattern("first");
                        java.util.Iterator<Transaction> it = firsts.iterator();
                        return it.hasNext()
                                && !it.next().getIp().equals(second.getIp());
                    }
                })
                .within(Time.seconds(MAX_WINDOW_SECONDS));

        DataStream<RiskAlert> ipCandidates = CEP.pattern(keyed, ipPattern).select(
                new PatternSelectFunction<Transaction, RiskAlert>() {
                    @Override
                    public RiskAlert select(Map<String, List<Transaction>> match) {
                        Transaction first = match.get("first").get(0);
                        Transaction second = match.get("second").get(0);

                        RiskAlert candidate = new RiskAlert();
                        candidate.setUserId(first.getUserId());
                        candidate.setOrderIds(first.getOrderId() + "," + second.getOrderId());
                        candidate.setTotalAmount(first.getAmount() + second.getAmount());
                        candidate.setCity(first.getCity());
                        candidate.setRiskType("IP_CHANGE");
                        candidate.setFirstAmount(first.getAmount());
                        candidate.setSecondAmount(second.getAmount());
                        candidate.setFirstIp(first.getIp());
                        candidate.setSecondIp(second.getIp());
                        candidate.setWindowStartTs(first.getEventTs());
                        candidate.setWindowEndTs(second.getEventTs());
                        return candidate;
                    }
                });

        // 单笔交易候选流：供 SINGLE_HIGH_AMOUNT 类规则判定
        DataStream<RiskAlert> singleCandidates = timed.map(tx -> {
            RiskAlert candidate = new RiskAlert();
            candidate.setUserId(tx.getUserId());
            candidate.setOrderIds(tx.getOrderId());
            candidate.setTotalAmount(tx.getAmount());
            candidate.setCity(tx.getCity());
            candidate.setRiskType("SINGLE");
            candidate.setFirstAmount(tx.getAmount());
            candidate.setSecondAmount(0);
            candidate.setWindowStartTs(tx.getEventTs());
            candidate.setWindowEndTs(tx.getEventTs());
            return candidate;
        });

        // 超时告警 CEP：单笔交易无后续交易，within 到期走超时侧输出
        OutputTag<RiskAlert> timeoutTag = new OutputTag<RiskAlert>("timeout-alerts") {
        };
        Pattern<Transaction, Transaction> timeoutPattern = Pattern
                .<Transaction>begin("first")
                .next("second")
                .within(Time.seconds(MAX_WINDOW_SECONDS));

        SingleOutputStreamOperator<RiskAlert> timeoutMain = CEP.pattern(keyed, timeoutPattern)
                .flatSelect(timeoutTag,
                        new PatternFlatTimeoutFunction<Transaction, RiskAlert>() {
                            @Override
                            public void timeout(Map<String, List<Transaction>> partial,
                                                long timestamp,
                                                Collector<RiskAlert> out) throws Exception {
                                List<Transaction> firsts = partial.get("first");
                                if (firsts == null || firsts.isEmpty()) {
                                    return;
                                }
                                Transaction first = firsts.get(0);
                                RiskAlert candidate = new RiskAlert();
                                candidate.setUserId(first.getUserId());
                                candidate.setOrderIds(first.getOrderId());
                                candidate.setTotalAmount(first.getAmount());
                                candidate.setCity(first.getCity());
                                candidate.setRiskType("TIMEOUT");
                                candidate.setFirstAmount(first.getAmount());
                                candidate.setSecondAmount(0);
                                candidate.setWindowStartTs(first.getEventTs());
                                candidate.setWindowEndTs(first.getEventTs()
                                        + MAX_WINDOW_SECONDS * 1000);
                                out.collect(candidate);
                            }
                        },
                        new PatternFlatSelectFunction<Transaction, RiskAlert>() {
                            @Override
                            public void flatSelect(Map<String, List<Transaction>> match,
                                                   Collector<RiskAlert> out) {
                                // 有后续交易的完整匹配由其他模式处理，此处不输出
                            }
                        });

        DataStream<RiskAlert> timeoutCandidates = timeoutMain.getSideOutput(timeoutTag);

        DataStream<RiskAlert> candidates = pairCandidates
                .union(tripleCandidates, ipCandidates, singleCandidates, timeoutCandidates);

        // ============ 3. 规则流：Kafka(rule_topic) -> Broadcast State ============
        KafkaSource<String> ruleSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(RULE_TOPIC)
                .setGroupId(RULE_GROUP)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        BroadcastStream<Rule> ruleBroadcast = env.fromSource(
                        ruleSource, WatermarkStrategy.noWatermarks(), "kafka-rules")
                .map(json -> MAPPER.readValue(json, Rule.class))
                .returns(TypeInformation.of(Rule.class))
                .broadcast(DynamicRuleProcessor.RULE_STATE_DESC);

        // ============ 4. 规则判定：候选告警流 connect 规则广播流 ============
        DataStream<RiskAlert> alerts = candidates
                .keyBy(RiskAlert::getUserId)
                .connect(ruleBroadcast)
                .process(new DynamicRuleProcessor());

        alerts.print();

        // ============ 5. 命中的告警写入 Doris（Stream Load + 唯一键幂等去重） ============
        // SimpleStringSerializer 走 CSV 格式，按表列序输出 tab 分隔行
        DataStream<String> alertCsv = alerts.map(alert -> String.join("\t",
                alert.getRuleId(),
                alert.getUserId(),
                alert.getWindowStart(),
                alert.getRuleName(),
                alert.getOrderIds(),
                String.valueOf(alert.getTotalAmount()),
                alert.getCity(),
                alert.getRiskType(),
                String.valueOf(alert.getRiskScore()),
                alert.getWindowEnd(),
                alert.getTriggerTime(),
                alert.getDetail()));

        alertCsv.sinkTo(DorisSink.<String>builder()
                .setDorisOptions(DorisOptions.builder()
                        .setFenodes(DORIS_FE_NODES)
                        .setUsername(DORIS_USER)
                        .setPassword(DORIS_PASSWORD)
                        .setTableIdentifier(DORIS_TABLE)
                        .build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder()
                        .setDeletable(false)  // 不追加 __DORIS_DELETE__ 隐藏列
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build());

        env.execute("Fraud Detection Job");
    }
}