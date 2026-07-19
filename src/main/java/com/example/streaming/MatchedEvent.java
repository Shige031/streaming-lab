package com.example.streaming;

public class MatchedEvent {
    private final Event event;
    private final Rule rule;

    public MatchedEvent(Event event, Rule rule) {
        this.event = event;
        this.rule = rule;
    }

    public Event getEvent() {
        return event;
    }

    public Rule getRule() {
        return rule;
    }
}