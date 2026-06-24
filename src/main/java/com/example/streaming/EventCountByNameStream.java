package com.example.streaming;

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
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class EventCountByNameStream {
  private static final String INPUT_TOPIC = "events";
  private static final String OUTPUT_TOPIC = "event-counts";

  private final ObjectMapper objectMapper;

  public EventCountByNameStream() {
    this.objectMapper = new ObjectMapper();
    this.objectMapper.registerModule(new JavaTimeModule());
  }

  public void start() throws InterruptedException {
    Properties props = new Properties();
    
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-lab-count-by-event-name-stream");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");

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

    KTable<String, Long> counts = groupedByEventName.count();

    // toStream() は KTable を KStream に変換する操作。更新がストリームとして出力される。
    counts.toStream().peek((eventName, count) -> {
      System.out.printf("[streams-count-by-event-name] eventName=%s, count=%d%n", eventName, count);
    }).to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

    KafkaStreams streams = new KafkaStreams(builder.build(), props);

    CountDownLatch latch = new CountDownLatch(1);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[streams-count-by-event-name] shutting down...");
            streams.close();
            latch.countDown();
    }));

    streams.start();

    System.out.println("[streams-count-by-event-name] started. Waiting for events...");

    latch.await();
  }
}