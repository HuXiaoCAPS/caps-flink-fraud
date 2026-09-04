package com.fraud;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 隔离测试：验证 CEP 模式（同用户连续两笔 >900 元，30 秒内）能否匹配。
 */
public class CepTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Transaction e1 = make("o1", "u1", 1000.0, 1_000_000);
        Transaction e2 = make("o2", "u1", 1200.0, 1_001_000);
        Transaction e3 = make("o3", "u2", 500.0, 1_002_000);

        DataStream<Transaction> timed = env.fromElements(e1, e2, e3)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((tx, ts) -> tx.getEventTs()));

        Pattern<Transaction, Transaction> pattern = Pattern
                .<Transaction>begin("first", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return tx.getAmount() > 900.0;
                    }
                })
                .next("second")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return tx.getAmount() > 900.0;
                    }
                })
                .within(Time.seconds(30));

        PatternStream<Transaction> ps = CEP.pattern(timed.keyBy(Transaction::getUserId), pattern);

        ps.select(new PatternSelectFunction<Transaction, String>() {
                    @Override
                    public String select(Map<String, List<Transaction>> match) {
                        return "MATCH: " + match.get("first").get(0).getOrderId()
                                + "+" + match.get("second").get(0).getOrderId();
                    }
                })
                .print();

        env.execute("CepTest");
    }

    private static Transaction make(String o, String u, double a, long ts) {
        Transaction t = new Transaction();
        t.setOrderId(o);
        t.setUserId(u);
        t.setAmount(a);
        t.setEventTs(ts);
        return t;
    }
}