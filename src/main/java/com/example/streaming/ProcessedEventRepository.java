package com.example.streaming;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;

public class ProcessedEventRepository implements AutoCloseable {
    private final Connection connection;
    private final PreparedStatement insertStatement;

    public ProcessedEventRepository() {
        try {
            this.connection = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/streaming_lab",
                    "streaming",
                    "streaming"
            );

            this.insertStatement = connection.prepareStatement(
                    """
                    INSERT INTO processed_events (
                        event_id,
                        user_id,
                        event_name,
                        rule_id,
                        processed_at
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (event_id, rule_id) DO NOTHING
                    """
            );
        } catch (SQLException e) {
            throw new RuntimeException("Failed to connect to PostgreSQL", e);
        }
    }

    public void insert(MatchedEvent matchedEvent) {
        Event event = matchedEvent.getEvent();
        Rule rule = matchedEvent.getRule();

        try {
            insertStatement.setString(1, event.getEventId());
            insertStatement.setString(2, event.getUserId());
            insertStatement.setString(3, event.getEventName());
            insertStatement.setString(4, rule.getRuleId());
            insertStatement.setObject(5, OffsetDateTime.now());

            insertStatement.executeUpdate();

            System.out.printf(
                    "[processed-events] inserted eventId=%s, userId=%s, eventName=%s, ruleId=%s%n",
                    event.getEventId(),
                    event.getUserId(),
                    event.getEventName(),
                    rule.getRuleId()
            );
        } catch (SQLException e) {
            throw new RuntimeException("Failed to insert processed event", e);
        }
    }

    @Override
    public void close() {
        try {
            insertStatement.close();
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to close PostgreSQL resources", e);
        }
    }
}