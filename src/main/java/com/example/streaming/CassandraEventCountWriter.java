package com.example.streaming;

import com.datastax.oss.driver.api.core.CqlSession;

import java.net.InetSocketAddress;
import java.time.Instant;

public class CassandraEventCountWriter {
  public void writeSampleCounts() {
    try(CqlSession session = CqlSession.builder()
         .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
         .withLocalDatacenter("datacenter1")
         .withKeyspace("streaming_lab")
         .build()) {
          session.execute(
                    """
                    INSERT INTO event_counts (
                        user_id,
                        event_name,
                        count,
                        updated_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    "user_001",
                    "app_open",
                    3L,
                    Instant.now()
            );

            session.execute(
                    """
                    INSERT INTO event_counts (
                        user_id,
                        event_name,
                        count,
                        updated_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    "user_001",
                    "purchase",
                    1L,
                    Instant.now()
            );

            session.execute(
                    """
                    INSERT INTO event_counts (
                        user_id,
                        event_name,
                        count,
                        updated_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    "user_002",
                    "app_open",
                    2L,
                    Instant.now()
            );
            System.out.println("[write-cassandra] sample counts inserted.");
         }
  }
}