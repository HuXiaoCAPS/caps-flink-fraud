package com.fraud;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 动态规则处理器（阶段三核心）
 *
 * CEP 负责"宽匹配"（结构固定，运行期不可改），本处理器负责"规则判定"：
 * 遍历 Broadcast State 中的全部规则，按 类型 / 阈值 / 窗口 / 启停开关 匹配，
 * 命中的规则权重累加得到风险评分；评分 > 0 才输出告警。
 * 因此修改 rule_topic 中的规则即可热更新，无需重启作业。
 *
 * 候选流按"形态"标记 candidate.getRiskType()：
 *   - SINGLE: 单笔交易
 *   - PAIR: 连续两笔（来自 2-consecutive CEP）
 *   - TRIPLE: 连续三笔（来自 3-consecutive CEP，HIGH_FREQUENCY 用）
 *   - IP_CHANGE: 两笔 IP 不同的交易（来自 IP 切换 CEP）
 *
 * 规则类型 -> 匹配的候选形态：
 *   - SINGLE_HIGH_AMOUNT   -> SINGLE   (单笔超阈值)
 *   - CONSECUTIVE_HIGH_AMOUNT -> PAIR   (两笔都超阈值且在窗口内)
 *   - SEQUENCE_SMALL_LARGE -> PAIR     (首笔小额试探、次笔大额)
 *   - HIGH_FREQUENCY       -> TRIPLE   (连续三笔在窗口内)
 *   - IP_CHANGE            -> IP_CHANGE(窗口内 IP 切换)
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
        double score = 0.0;
        List<String> matchedNames = new ArrayList<>();
        Rule firstMatched = null;

        for (Map.Entry<String, Rule> entry
                : ctx.getBroadcastState(RULE_STATE_DESC).immutableEntries()) {
            Rule rule = entry.getValue();
            if (matches(rule, candidate)) {
                score += rule.getWeight();
                matchedNames.add(rule.getRuleName());
                if (firstMatched == null) {
                    firstMatched = rule;
                }
            }
        }

        if (score > 0 && firstMatched != null) {
            out.collect(buildAlert(firstMatched, matchedNames, score, candidate));
        }
    }

    private long span(RiskAlert c) {
        return c.getWindowEndTs() - c.getWindowStartTs();
    }

    private boolean matches(Rule rule, RiskAlert candidate) {
        if (!rule.isEnabled()) {
            return false;
        }
        switch (rule.getRuleType()) {
            case "SINGLE_HIGH_AMOUNT":
                return "SINGLE".equals(candidate.getRiskType())
                        && candidate.getFirstAmount() > rule.getThreshold();
            case "CONSECUTIVE_HIGH_AMOUNT":
                return "PAIR".equals(candidate.getRiskType())
                        && candidate.getFirstAmount() > rule.getThreshold()
                        && candidate.getSecondAmount() > rule.getThreshold()
                        && span(candidate) <= rule.getWindowMs();
            case "SEQUENCE_SMALL_LARGE":
                return "PAIR".equals(candidate.getRiskType())
                        && candidate.getFirstAmount() < rule.getSmallThreshold()
                        && candidate.getSecondAmount() > rule.getThreshold()
                        && span(candidate) <= rule.getWindowMs();
            case "HIGH_FREQUENCY":
                return "TRIPLE".equals(candidate.getRiskType())
                        && span(candidate) <= rule.getWindowMs();
            case "IP_CHANGE":
                return "IP_CHANGE".equals(candidate.getRiskType())
                        && span(candidate) <= rule.getWindowMs();
            case "TIMEOUT_ALERT":
                // 单笔大额交易无后续交易，within 到期超时（窗口由 CEP 结构上限决定）
                return "TIMEOUT".equals(candidate.getRiskType())
                        && candidate.getFirstAmount() > rule.getThreshold();
            default:
                return false;
        }
    }

    private RiskAlert buildAlert(Rule rule, List<String> matchedNames,
                                 double score, RiskAlert candidate) {
        RiskAlert alert = new RiskAlert();
        alert.setRuleId(rule.getRuleId());
        alert.setRuleName(String.join("、", matchedNames));
        alert.setUserId(candidate.getUserId());
        alert.setOrderIds(candidate.getOrderIds());
        alert.setTotalAmount(candidate.getTotalAmount());
        alert.setCity(candidate.getCity());
        alert.setRiskType(rule.getRuleType());
        alert.setRiskScore(score);
        alert.setWindowStart(formatTs(candidate.getWindowStartTs()));
        alert.setWindowEnd(formatTs(candidate.getWindowEndTs()));
        alert.setTriggerTime(formatTs(System.currentTimeMillis()));

        String amounts;
        switch (candidate.getRiskType()) {
            case "SINGLE":
                amounts = "金额=" + candidate.getFirstAmount();
                break;
            case "TIMEOUT":
                amounts = "金额=" + candidate.getFirstAmount() + " 无后续交易(超时)";
                break;
            case "TRIPLE":
                amounts = "金额=" + candidate.getFirstAmount() + "+"
                        + candidate.getSecondAmount() + "+" + candidate.getThirdAmount();
                break;
            case "IP_CHANGE":
                amounts = "IP切换 " + candidate.getFirstIp() + "->" + candidate.getSecondIp()
                        + " 金额=" + candidate.getFirstAmount() + "+" + candidate.getSecondAmount();
                break;
            default: // PAIR
                amounts = "金额=" + candidate.getFirstAmount() + "+" + candidate.getSecondAmount();
        }
        alert.setDetail("命中规则[" + String.join("、", matchedNames) + "] 风险分="
                + Math.round(score * 100) / 100.0 + " " + amounts);
        return alert;
    }

    @Override
    public void processBroadcastElement(Rule rule, Context ctx,
                                        Collector<RiskAlert> out) throws Exception {
        ctx.getBroadcastState(RULE_STATE_DESC).put(rule.getRuleId(), rule);
        LOG.info("规则热更新: ruleId={} ruleName={} ruleType={} threshold={} "
                        + "smallThreshold={} count={} windowMs={}ms enabled={} version={} weight={}",
                rule.getRuleId(), rule.getRuleName(), rule.getRuleType(),
                rule.getThreshold(), rule.getSmallThreshold(), rule.getCount(),
                rule.getWindowMs(), rule.isEnabled(), rule.getVersion(), rule.getWeight());
    }

    private static String formatTs(long ts) {
        return SDF.format(new Date(ts));
    }
}