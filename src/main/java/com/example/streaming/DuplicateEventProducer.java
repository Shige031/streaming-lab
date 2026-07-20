package com.example.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;

public class DuplicateEventProducer {
    private static final String TOPIC = "events";

    private final ObjectMapper objectMapper;

    public DuplicateEventProducer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    public void produceDuplicatePurchaseEvent() {
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");

        Event event = new Event(
                "duplicate-event-001",
                "user_999",
                "purchase",
                Instant.now()
        );

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String key = event.getUserId();
            String value = objectMapper.writeValueAsString(event);

            for (int i = 1; i <= 2; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        TOPIC,
                        key,
                        value
                );

                RecordMetadata metadata = producer.send(record).get();

                System.out.printf(
                        "[produce-duplicate] sent #%d partition=%d offset=%d key=%s value=%s%n",
                        i,
                        metadata.partition(),
                        metadata.offset(),
                        key,
                        value
                );
            }

            producer.flush();
        } catch (Exception e) {
            throw new RuntimeException("Failed to produce duplicate event", e);
        }
    }
}