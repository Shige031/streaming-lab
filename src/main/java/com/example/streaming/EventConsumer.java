package com.example.streaming;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class EventConsumer {
    private static final String TOPIC = "events";

    private final String consumerName;
    private final KafkaConsumer<String, String> consumer;

    public EventConsumer(String consumerName) {
        this.consumerName = consumerName;
        
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("group.id", "streaming-lab-consumer");

        props.put("client.id", consumerName);

        props.put("auto.offset.reset", "earliest");

        props.put("enable.auto.commit", "false");

        this.consumer = new KafkaConsumer<>(props);
    }

    public void consumeForever() {
        consumer.subscribe(
          List.of(TOPIC),
          new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
              System.out.printf("[%s] revoked partitions: %s%n", consumerName, partitions);
            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
              System.out.printf("[%s] assigned partitions: %s%n", consumerName, partitions);
            }
          }
        );

        System.out.printf("[%s] Consumer started. Waiting for events...%n", consumerName);

        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf(
                        "[%s] Consumed event: topic=%s, partition=%d, offset=%d, key=%s, value=%s%n",
                        consumerName,
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