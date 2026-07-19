package com.example.streaming;

public class Rule {
    private String ruleId;
    private String targetEventName;
    private boolean enabled;

    public Rule() {
    }

    public Rule(String ruleId, String targetEventName, boolean enabled) {
        this.ruleId = ruleId;
        this.targetEventName = targetEventName;
        this.enabled = enabled;
    }

    public String getRuleId() {
        return ruleId;
    }

    public String getTargetEventName() {
        return targetEventName;
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public String toString() {
        return "Rule{" +
                "ruleId='" + ruleId + '\'' +
                ", targetEventName='" + targetEventName + '\'' +
                ", enabled=" + enabled +
                '}';
    }
}