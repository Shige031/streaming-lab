package com.example.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class RuleReadStream {
    private static final String RULES_TOPIC = "rules";

    private final ObjectMapper objectMapper;

    public RuleReadStream() {
        this.objectMapper = new ObjectMapper();
    }

    public void start() throws InterruptedException {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-lab-read-rules-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> ruleMessages = builder.stream(RULES_TOPIC);

        KStream<String, Rule> rules = ruleMessages.mapValues(value -> {
            try {
                return objectMapper.readValue(value, Rule.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse rule JSON: " + value, e);
            }
        });

        rules.peek((key, rule) -> {
            System.out.printf(
                    "[streams-read-rules] key=%s, ruleId=%s, targetEventName=%s, enabled=%s%n",
                    key,
                    rule.getRuleId(),
                    rule.getTargetEventName(),
                    rule.isEnabled()
            );
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[streams-read-rules] shutting down...");
            streams.close();
            latch.countDown();
        }));

        streams.start();

        System.out.println("[streams-read-rules] started. Waiting for rules...");

        latch.await();
    }
}