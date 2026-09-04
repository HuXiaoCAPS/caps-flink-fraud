package com.fraud;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * 风控规则 POJO（来自 rule_topic 的 JSON，与 tools/rule_producer.py 的字段对齐）。
 *
 * 规则通过 Broadcast State 广播到所有并行实例，实现"改规则不重启作业"。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Rule implements Serializable {

    @JsonProperty("rule_id")
    private String ruleId;

    @JsonProperty("rule_name")
    private String ruleName;

    @JsonProperty("rule_type")
    private String ruleType;

    @JsonProperty("threshold")
    private double threshold;

    @JsonProperty("window_ms")
    private long windowMs;

    @JsonProperty("enabled")
    private boolean enabled;

    @JsonProperty("version")
    private int version;

    @JsonProperty("weight")
    private double weight = 0.5;

    public Rule() {
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    public String getRuleType() {
        return ruleType;
    }

    public void setRuleType(String ruleType) {
        this.ruleType = ruleType;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public long getWindowMs() {
        return windowMs;
    }

    public void setWindowMs(long windowMs) {
        this.windowMs = windowMs;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    @Override
    public String toString() {
        return "Rule{ruleId='" + ruleId + "', ruleName='" + ruleName
                + "', ruleType='" + ruleType + "', threshold=" + threshold
                + ", windowMs=" + windowMs + ", enabled=" + enabled + ", version=" + version + "}";
    }
}