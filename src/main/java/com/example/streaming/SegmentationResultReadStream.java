package com.example.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class SegmentationResultReadStream {
    private static final String TOPIC = "segmentation-results";

    private final ObjectMapper objectMapper;

    public SegmentationResultReadStream() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    public void start() throws InterruptedException {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-lab-read-segmentation-results-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> messages = builder.stream(
                TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        KStream<String, SegmentationResult> results = messages.mapValues(value -> {
            try {
                return objectMapper.readValue(value, SegmentationResult.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to parse segmentation result JSON: " + value, e);
            }
        });

        results.peek((key, result) -> {
            System.out.printf(
                    "[read-segmentation-results] key=%s, userId=%s, segmentId=%s, matched=%s, evaluatedAt=%s%n",
                    key,
                    result.getUserId(),
                    result.getSegmentId(),
                    result.isMatched(),
                    result.getEvaluatedAt()
            );
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[read-segmentation-results] shutting down...");
            streams.close();
            latch.countDown();
        }));

        streams.start();

        System.out.println("[read-segmentation-results] started. Waiting for segmentation results...");

        latch.await();
    }
}