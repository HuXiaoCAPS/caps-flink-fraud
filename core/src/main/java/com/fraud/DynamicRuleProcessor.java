package com.fraud;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 动态规则处理器（阶段三核心）
 *
 * CEP 负责"宽匹配"（结构固定，运行期不可改），本处理器负责"规则判定"：
 * 从 Broadcast State 读取最新规则，按阈值、时间窗口、启停开关决定是否命中。
 * 因此修改 rule_topic 中的规则即可热更新，无需重启作业。
 */
public class DynamicRuleProcessor
        extends KeyedBroadcastProcessFunction<String, RiskAlert, Rule, RiskAlert> {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicRuleProcessor.class);

    /** 广播状态：rule_id -> Rule */
    public static final MapStateDescriptor<String, Rule> RULE_STATE_DESC =
            new MapStateDescriptor<>("risk-rules", Types.STRING, Types.POJO(Rule.class));

    private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void processElement(RiskAlert candidate, ReadOnlyContext ctx,
                               Collector<RiskAlert> out) throws Exception {
        // 遍历广播状态中的全部规则（支持同类型多规则）
        for (java.util.Map.Entry<String, Rule> entry
                : ctx.getBroadcastState(RULE_STATE_DESC).immutableEntries()) {
            Rule rule = entry.getValue();
            if (apply(rule, candidate, out)) {
                return; // 命中一条规则即输出一条告警
            }
        }
    }

    private boolean apply(Rule rule, RiskAlert candidate, Collector<RiskAlert> out) {
        if (!rule.isEnabled()) {
            return false;
        }
        if (!rule.getRuleType().equals(candidate.getRiskType())) {
            return false;
        }
        // 阈值校验：两笔交易都必须超过阈值
        if (candidate.getFirstAmount() <= rule.getThreshold()) {
            return false;
        }
        if (candidate.getSecondAmount() <= rule.getThreshold()) {
            return false;
        }
        // 时间窗口校验：两笔交易时间差须在规则窗口内
        if (candidate.getWindowEndTs() - candidate.getWindowStartTs() > rule.getWindowMs()) {
            return false;
        }

        RiskAlert alert = new RiskAlert();
        alert.setRuleId(rule.getRuleId());
        alert.setRuleName(rule.getRuleName());
        alert.setUserId(candidate.getUserId());
        alert.setOrderIds(candidate.getOrderIds());
        alert.setTotalAmount(candidate.getTotalAmount());
        alert.setCity(candidate.getCity());
        alert.setRiskType(rule.getRuleType());
        alert.setWindowStart(formatTs(candidate.getWindowStartTs()));
        alert.setWindowEnd(formatTs(candidate.getWindowEndTs()));
        alert.setTriggerTime(formatTs(System.currentTimeMillis()));
        alert.setDetail((rule.getWindowMs() / 1000)
                + "秒内连续两笔超过" + rule.getThreshold() + "元交易: "
                + candidate.getFirstAmount() + " + " + candidate.getSecondAmount());
        out.collect(alert);
        return true;
    }

    @Override
    public void processBroadcastElement(Rule rule, Context ctx,
                                        Collector<RiskAlert> out) throws Exception {
        ctx.getBroadcastState(RULE_STATE_DESC).put(rule.getRuleId(), rule);
        LOG.info("规则热更新: ruleId={} ruleName={} ruleType={} threshold={} "
                        + "windowMs={}ms enabled={} version={}",
                rule.getRuleId(), rule.getRuleName(), rule.getRuleType(),
                rule.getThreshold(), rule.getWindowMs(), rule.isEnabled(), rule.getVersion());
    }

    private static String formatTs(long ts) {
        return SDF.format(new Date(ts));
    }
}