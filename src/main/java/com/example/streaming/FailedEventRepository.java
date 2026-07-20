package com.example.streaming;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;

public class FailedEventRepository implements AutoCloseable {
    private final Connection connection;
    private final PreparedStatement upsertStatement;

    public FailedEventRepository() {
        try {
            this.connection = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/streaming_lab",
                    "streaming",
                    "streaming"
            );

            this.upsertStatement = connection.prepareStatement(
                    """
                    INSERT INTO failed_events (
                        event_id,
                        user_id,
                        event_name,
                        rule_id,
                        error_message,
                        retry_count,
                        status,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, 0, 'PENDING', ?, ?)
                    ON CONFLICT (event_id, rule_id)
                    DO UPDATE SET
                        error_message = EXCLUDED.error_message,
                        retry_count = failed_events.retry_count + 1,
                        status = 'PENDING',
                        updated_at = EXCLUDED.updated_at
                    """
            );
        } catch (SQLException e) {
            throw new RuntimeException("Failed to connect to PostgreSQL for failed_events", e);
        }
    }

    public void saveFailure(MatchedEvent matchedEvent, Exception error) {
        Event event = matchedEvent.getEvent();
        Rule rule = matchedEvent.getRule();
        OffsetDateTime now = OffsetDateTime.now();

        try {
            upsertStatement.setString(1, event.getEventId());
            upsertStatement.setString(2, event.getUserId());
            upsertStatement.setString(3, event.getEventName());
            upsertStatement.setString(4, rule.getRuleId());
            upsertStatement.setString(5, error.getMessage());
            upsertStatement.setObject(6, now);
            upsertStatement.setObject(7, now);

            upsertStatement.executeUpdate();

            System.out.printf(
                    "[failed-events] saved failure eventId=%s, userId=%s, eventName=%s, ruleId=%s, error=%s%n",
                    event.getEventId(),
                    event.getUserId(),
                    event.getEventName(),
                    rule.getRuleId(),
                    error.getMessage()
            );
        } catch (SQLException e) {
            throw new RuntimeException("Failed to save failed event", e);
        }
    }

    @Override
    public void close() {
        try {
            upsertStatement.close();
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to close failed event repository", e);
        }
    }
}