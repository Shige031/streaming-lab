package com.example.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class EventConsumer {
    private static final String TOPIC = "events";

    private final KafkaConsumer<String, String> consumer;

    public EventConsumer() {
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("group.id", "streaming-lab-consumer");

        props.put("auto.offset.reset", "earliest");

        props.put("enable.auto.commit", "false");

        this.consumer = new KafkaConsumer<>(props);
    }

    public void consumeForever() {
        consumer.subscribe(List.of(TOPIC));

        System.out.println("Consumer started. Waiting for events...");

        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf(
                        "Consumed event: topic=%s, partition=%d, offset=%d, key=%s, value=%s%n",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        record.value()
                );
            }

            consumer.commitSync();
        }
    }
}