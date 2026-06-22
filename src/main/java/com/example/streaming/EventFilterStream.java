package com.example.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class EventFilterStream {
  private static final String INPUT_TOPIC = "events";

  private final ObjectMapper objectMapper;

  public EventFilterStream() {
    this.objectMapper = new ObjectMapper();
    this.objectMapper.registerModule(new JavaTimeModule());
  }

  public void start() throws Exception {
    Properties props = new Properties();

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-lab-filter-stream");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> events = builder.stream(INPUT_TOPIC);

    // mapValues は key を変えずに value だけ変換する操作。
    KStream<String, Event> parsedEvents = events.mapValues(value -> {
      try {
        // JSON文字列をEventオブジェクトに変換
        return objectMapper.readValue(value, Event.class);
      } catch (Exception e) {
        // 1件の壊れた message で stream 全体が止まる可能性。実務ではmalformed event を別 topic に逃がす、ログだけ出して除外する、などを考える。
        throw new RuntimeException("Failed to parse event JSON: " + value, e);
      }
    });

    KStream<String, Event> purchaseEvents = parsedEvents.filter((key, event) -> {
      return "purchase".equals(event.getEventName());
    });

    KStream<String, String> displayValues = purchaseEvents.mapValues(event -> {
      // eventオブジェクトを表示用の文字列に変換
      return "userId=%s, eventName=%s, trackedAt=%s".formatted(
        event.getUserId(),
        event.getEventName(),
        event.getTrackedAt()
      );
    });

    displayValues.peek((key, value) -> {
      System.out.printf("[streams-filter] key=%s, value=%s%n", key, value);
    });

    KafkaStreams streams = new KafkaStreams(builder.build(), props);

    CountDownLatch latch = new CountDownLatch(1);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("[streams-filter] shutting down...");
      streams.close();
      latch.countDown();
    }));

    streams.start();

    System.out.println("[streams-filter] started. Waiting for events...");

    latch.await();
  }
}