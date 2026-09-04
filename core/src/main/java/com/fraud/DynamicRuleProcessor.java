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
 * 遍历 Broadcast State 中的全部规则，按类型 / 阈值 / 窗口 / 启停开关匹配，
 * 命中的规则权重累加得到风险评分；评分 > 0 才输出告警。
 * 因此修改 rule_topic 中的规则即可热更新，无需重启作业。
 *
 * 支持的规则类型（与 candidate.getRiskType() 对应）：
 *   - CONSECUTIVE_HIGH_AMOUNT: 连续两笔都超过阈值且时间差在窗口内
 *   - SINGLE_HIGH_AMOUNT: 单笔超过阈值
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

    private boolean matches(Rule rule, RiskAlert candidate) {
        if (!rule.isEnabled()) {
            return false;
        }
        if (!rule.getRuleType().equals(candidate.getRiskType())) {
            return false;
        }
        if ("SINGLE_HIGH_AMOUNT".equals(candidate.getRiskType())) {
            return candidate.getFirstAmount() > rule.getThreshold();
        }
        // 默认 CONSECUTIVE_HIGH_AMOUNT
        return candidate.getFirstAmount() > rule.getThreshold()
                && candidate.getSecondAmount() > rule.getThreshold()
                && (candidate.getWindowEndTs() - candidate.getWindowStartTs()) <= rule.getWindowMs();
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
        alert.setRiskType(candidate.getRiskType());
        alert.setRiskScore(score);
        alert.setWindowStart(formatTs(candidate.getWindowStartTs()));
        alert.setWindowEnd(formatTs(candidate.getWindowEndTs()));
        alert.setTriggerTime(formatTs(System.currentTimeMillis()));
        String amounts = candidate.getRiskType().startsWith("SINGLE")
                ? String.valueOf(candidate.getFirstAmount())
                : candidate.getFirstAmount() + " + " + candidate.getSecondAmount();
        alert.setDetail("命中规则[" + String.join("、", matchedNames) + "] 风险分="
                + Math.round(score * 100) / 100.0 + " 金额=" + amounts);
        return alert;
    }

    @Override
    public void processBroadcastElement(Rule rule, Context ctx,
                                        Collector<RiskAlert> out) throws Exception {
        ctx.getBroadcastState(RULE_STATE_DESC).put(rule.getRuleId(), rule);
        LOG.info("规则热更新: ruleId={} ruleName={} ruleType={} threshold={} "
                        + "windowMs={}ms enabled={} version={} weight={}",
                rule.getRuleId(), rule.getRuleName(), rule.getRuleType(),
                rule.getThreshold(), rule.getWindowMs(), rule.isEnabled(),
                rule.getVersion(), rule.getWeight());
    }

    private static String formatTs(long ts) {
        return SDF.format(new Date(ts));
    }
}