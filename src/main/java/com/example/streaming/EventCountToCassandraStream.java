package com.example.streaming;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class EventCountToCassandraStream {
  private static final String INPUT_TOPIC = "events";

  private final ObjectMapper objectMapper;

  public EventCountToCassandraStream() {
    this.objectMapper = new ObjectMapper();
    this.objectMapper.registerModule(new JavaTimeModule());
  }

  public void start() throws InterruptedException {
    Properties props = new Properties();

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-lab-count-to-cassandra-stream");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");

    props.put(StreamsConfig.STATE_DIR_CONFIG, "./tmp/kafka-streams");

    CqlSession session = CqlSession.builder()
      .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
      .withLocalDatacenter("datacenter1")
      .withKeyspace("streaming_lab")
      .build();

    PreparedStatement insertCountStatement = session.prepare(
      """
      INSERT INTO event_name_counts (
        event_name,
        count,
        updated_at
      ) VALUES (?, ?, ?)
      """
    );

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> events = builder.stream(INPUT_TOPIC);

    KStream<String, String> eventsByName = events.selectKey((oldKey, value) -> {
      try{
        Event event = objectMapper.readValue(value, Event.class);
        return event.getEventName();
      } catch(Exception e) {
        throw new RuntimeException("Failed to parse event JSON: " + value, e);
      }
    });

    KGroupedStream<String, String> groupedByEventName = eventsByName.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

    KTable<String, Long> counts = groupedByEventName.count(Materialized.as("event-name-counts-store"));

    counts.toStream().peek((eventName, count) -> {
      System.out.printf("[streams-count-to-cassandra] eventName=%s, count=%d%n", eventName, count);
    }).foreach((eventName, count) -> {
      session.execute(insertCountStatement.bind(eventName, count, Instant.now()));
    });

    KafkaStreams streams = new KafkaStreams(builder.build(), props);

    CountDownLatch latch = new CountDownLatch(1);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        System.out.println("[streams-count-to-cassandra] shutting down...");
        streams.close();
        session.close();
        latch.countDown();
    }));

    streams.start();

    System.out.println("[streams-count-to-cassandra] started. Waiting for events...");

    latch.await();
    
  }
}