package com.example.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.List;
import java.util.Properties;

public class SegmentationResultProducer {
    private static final String TOPIC = "segmentation-results";

    private final ObjectMapper objectMapper;

    public SegmentationResultProducer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    public void produceSampleResults() {
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");

        List<SegmentationResult> results = List.of(
                new SegmentationResult(
                        "user_001",
                        "segment_vip",
                        true,
                        Instant.now()
                ),
                new SegmentationResult(
                        "user_002",
                        "segment_vip",
                        false,
                        Instant.now()
                ),
                new SegmentationResult(
                        "user_003",
                        "segment_vip",
                        true,
                        Instant.now()
                )
        );

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (SegmentationResult result : results) {
                String key = result.getUserId();
                String value = objectMapper.writeValueAsString(result);

                ProducerRecord<String, String> record = new ProducerRecord<>(
                        TOPIC,
                        key,
                        value
                );

                RecordMetadata metadata = producer.send(record).get();

                System.out.printf(
                        "[produce-segmentation-results] sent partition=%d offset=%d key=%s value=%s%n",
                        metadata.partition(),
                        metadata.offset(),
                        key,
                        value
                );
            }

            producer.flush();
        } catch (Exception e) {
            throw new RuntimeException("Failed to produce segmentation results", e);
        }
    }
}