package com.example.streaming;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class RuleGlobalTableStream {
    private static final String RULES_TOPIC = "rules";

    public void start() throws InterruptedException {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-lab-rules-global-table-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "./tmp/kafka-streams");

        StreamsBuilder builder = new StreamsBuilder();

        RuleSerde ruleSerde = new RuleSerde();

        GlobalKTable<String, Rule> rulesTable = builder.globalTable(
                RULES_TOPIC,
                Consumed.with(Serdes.String(), ruleSerde),
                Materialized.as("rules-global-store")
        );

        /*
         * 今回は rulesTable を作るだけです。
         * GlobalKTable は、後の Part 17 で events と join して使います。
         */
        System.out.println("[rules-global-table] topology created: " + rulesTable);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[rules-global-table] shutting down...");
            streams.close();
            latch.countDown();
        }));

        streams.start();

        System.out.println("[rules-global-table] started. Waiting for rules...");

        latch.await();
    }
}