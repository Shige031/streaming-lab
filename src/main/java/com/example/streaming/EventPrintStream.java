package com.example.streaming;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class EventPrintStream {
  private static final String INPUT_TOPIC = "events";

  public void start() throws Exception {
    Properties props = new Properties();

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-lab-print-stream");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

   // Serializer + Deserializer
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> events = builder.stream(INPUT_TOPIC);

    // peekは、流れてきたrecordを横から覗く操作。副作用はあまり入れない。
    events.peek((key, value) -> {
      System.out.printf("[streams-print] key=%s, value=%s%n", key, value);
    });

    KafkaStreams streams = new KafkaStreams(builder.build(), props);

    CountDownLatch latch = new CountDownLatch(1);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("[streams-print] shutting down...");
      streams.close();
      latch.countDown();
    }));

    streams.start();

    System.out.println("[streams-print] started. Waiting for events...");

    latch.await();
  }
}