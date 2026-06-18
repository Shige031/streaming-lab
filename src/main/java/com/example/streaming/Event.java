package com.example.streaming;

import java.time.Instant;

public class Event {
    private String eventId;
    private String userId;
    private String eventName;
    private Instant trackedAt;

    public Event() {
    }

    public Event(String eventId, String userId, String eventName, Instant trackedAt) {
        this.eventId = eventId;
        this.userId = userId;
        this.eventName = eventName;
        this.trackedAt = trackedAt;
    }

    public String getEventId() {
        return eventId;
    }

    public String getUserId() {
        return userId;
    }

    public String getEventName() {
        return eventName;
    }

    public Instant getTrackedAt() {
        return trackedAt;
    }
}