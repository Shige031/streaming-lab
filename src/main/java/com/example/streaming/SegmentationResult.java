package com.example.streaming;

import java.time.Instant;

public class SegmentationResult {
    private String userId;
    private String segmentId;
    private boolean matched;
    private Instant evaluatedAt;

    public SegmentationResult() {
    }

    public SegmentationResult(
            String userId,
            String segmentId,
            boolean matched,
            Instant evaluatedAt
    ) {
        this.userId = userId;
        this.segmentId = segmentId;
        this.matched = matched;
        this.evaluatedAt = evaluatedAt;
    }

    public String getUserId() {
        return userId;
    }

    public String getSegmentId() {
        return segmentId;
    }

    public boolean isMatched() {
        return matched;
    }

    public Instant getEvaluatedAt() {
        return evaluatedAt;
    }

    @Override
    public String toString() {
        return "SegmentationResult{" +
                "userId='" + userId + '\'' +
                ", segmentId='" + segmentId + '\'' +
                ", matched=" + matched +
                ", evaluatedAt=" + evaluatedAt +
                '}';
    }
}