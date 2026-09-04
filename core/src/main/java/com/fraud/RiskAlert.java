package com.fraud;

/**
 * 风控告警结果，对应 Doris 表 risk.dws_risk_result。
 */
public class RiskAlert {

    private String ruleId;
    private String ruleName;
    private String userId;
    private String orderIds;
    private double totalAmount;
    private String city;
    private String riskType;
    private String windowStart;
    private String windowEnd;
    private String triggerTime;
    private String detail;

    // CEP 候选匹配的原始信息，供 DynamicRuleProcessor 按最新规则判定
    private double firstAmount;
    private double secondAmount;
    private long windowStartTs;
    private long windowEndTs;

    public RiskAlert() {
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

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getOrderIds() {
        return orderIds;
    }

    public void setOrderIds(String orderIds) {
        this.orderIds = orderIds;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getRiskType() {
        return riskType;
    }

    public void setRiskType(String riskType) {
        this.riskType = riskType;
    }

    public String getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(String windowStart) {
        this.windowStart = windowStart;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }

    public String getTriggerTime() {
        return triggerTime;
    }

    public void setTriggerTime(String triggerTime) {
        this.triggerTime = triggerTime;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }

    public double getFirstAmount() {
        return firstAmount;
    }

    public void setFirstAmount(double firstAmount) {
        this.firstAmount = firstAmount;
    }

    public double getSecondAmount() {
        return secondAmount;
    }

    public void setSecondAmount(double secondAmount) {
        this.secondAmount = secondAmount;
    }

    public long getWindowStartTs() {
        return windowStartTs;
    }

    public void setWindowStartTs(long windowStartTs) {
        this.windowStartTs = windowStartTs;
    }

    public long getWindowEndTs() {
        return windowEndTs;
    }

    public void setWindowEndTs(long windowEndTs) {
        this.windowEndTs = windowEndTs;
    }

    @Override
    public String toString() {
        return "RiskAlert{ruleId='" + ruleId + "', ruleName='" + ruleName
                + "', userId='" + userId + "', totalAmount=" + totalAmount + "}";
    }
}