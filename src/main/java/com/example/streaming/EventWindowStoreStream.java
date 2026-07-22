package com.example.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.StoreBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class EventWindowStoreStream {
    private static final String EVENTS_TOPIC = "events";
    private static final String STORE_NAME = "pending-events-window-store";

    private final ObjectMapper objectMapper;

    public EventWindowStoreStream() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    public void start() throws InterruptedException {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-lab-event-window-store-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "./tmp/kafka-streams");

        StreamsBuilder builder = new StreamsBuilder();

        /*
         * Window Store を作る。
         *
         * retentionPeriod:
         *   store に event を保持する期間
         *
         * windowSize:
         *   1つの window のサイズ
         *
         * retainDuplicates:
         *   同じ key + timestamp の値を複数保持するか
         */
        WindowBytesStoreSupplier storeSupplier = Stores.persistentWindowStore(
                STORE_NAME,
                Duration.ofMinutes(10),
                Duration.ofMinutes(10),
                true
        );

        StoreBuilder<WindowStore<String, String>> storeBuilder =
                Stores.windowStoreBuilder(
                        storeSupplier,
                        Serdes.String(),
                        Serdes.String()
                );

        builder.addStateStore(storeBuilder);

        builder.stream(EVENTS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .process(
                        new EventWindowStoreProcessorSupplier(),
                        STORE_NAME
                );

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[event-window-store] shutting down...");
            streams.close();
            latch.countDown();
        }));

        streams.start();

        System.out.println("[event-window-store] started. Waiting for events...");

        latch.await();
    }

    private class EventWindowStoreProcessorSupplier implements ProcessorSupplier<String, String, Void, Void> {
        @Override
        public Processor<String, String, Void, Void> get() {
            return new EventWindowStoreProcessor();
        }
    }

    private class EventWindowStoreProcessor implements Processor<String, String, Void, Void> {
        private WindowStore<String, String> store;

        @Override
        public void init(ProcessorContext<Void, Void> context) {
            this.store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(Record<String, String> record) {
            try {
                Event event = objectMapper.readValue(record.value(), Event.class);

                String userId = event.getUserId();
                long eventTimestamp = event.getTrackedAt().toEpochMilli();

                store.put(userId, record.value(), eventTimestamp);

                System.out.printf(
                        "[event-window-store] stored userId=%s, eventId=%s, eventName=%s, trackedAt=%s%n",
                        userId,
                        event.getEventId(),
                        event.getEventName(),
                        event.getTrackedAt()
                );

                /*
                 * 学習用:
                 * 保存後に、同じ userId の直近10分の event を読んでみる。
                 */
                Instant from = event.getTrackedAt().minus(Duration.ofMinutes(10));
                Instant to = event.getTrackedAt().plus(Duration.ofSeconds(1));

                try (WindowStoreIterator<String> iterator = store.fetch(
                        userId,
                        from,
                        to
                )) {
                    System.out.printf(
                            "[event-window-store] recent events for userId=%s:%n",
                            userId
                    );

                    while (iterator.hasNext()) {
                        KeyValue<Long, String> item = iterator.next();

                        System.out.printf(
                                "  timestamp=%s, value=%s%n",
                                Instant.ofEpochMilli(item.key),
                                item.value
                        );
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to store event in window store: " + record.value(), e);
            }
        }

        @Override
        public void close() {
        }
    }
}