package com.example.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class EventRuleToPostgresStream {
    private static final String EVENTS_TOPIC = "events";
    private static final String RULES_TOPIC = "rules";

    private final ObjectMapper objectMapper;

    public EventRuleToPostgresStream() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    public void start() throws InterruptedException {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-lab-event-rule-to-postgres-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "./tmp/kafka-streams");

        ProcessedEventRepository repository = new ProcessedEventRepository();

        StreamsBuilder builder = new StreamsBuilder();

        RuleSerde ruleSerde = new RuleSerde();

        GlobalKTable<String, Rule> rulesTable = builder.globalTable(
                RULES_TOPIC,
                Consumed.with(Serdes.String(), ruleSerde),
                Materialized.as("rules-global-store-for-postgres")
        );

        KStream<String, String> eventMessages = builder.stream(
                EVENTS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        KStream<String, Event> events = eventMessages.mapValues(value -> {
            try {
                return objectMapper.readValue(value, Event.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse event JSON: " + value, e);
            }
        });

        KStream<String, MatchedEvent> matchedEvents = events.join(
                rulesTable,
                (eventKey, event) -> "rule_" + event.getEventName(),
                MatchedEvent::new
        );

        matchedEvents
                .filter((key, matched) -> {
                    Rule rule = matched.getRule();
                    Event event = matched.getEvent();

                    return rule.isEnabled()
                            && rule.getTargetEventName().equals(event.getEventName());
                })
                .foreach((key, matched) -> {
                    repository.insert(matched);
                });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[event-rule-to-postgres] shutting down...");
            streams.close();
            repository.close();
            latch.countDown();
        }));

        streams.start();

        System.out.println("[event-rule-to-postgres] started. Waiting for events and rules...");

        latch.await();
    }
}