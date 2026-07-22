package com.example.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class EventReevaluationStream {
    private static final String EVENTS_TOPIC = "events";
    private static final String SEGMENTATION_RESULTS_TOPIC = "segmentation-results";
    private static final String STORE_NAME = "pending-events-for-reevaluation-store";

    private final ObjectMapper objectMapper;

    public EventReevaluationStream() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    public void start() throws InterruptedException {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-lab-event-reevaluation-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "./tmp/kafka-streams");

        StreamsBuilder builder = new StreamsBuilder();

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

        /*
         * events topic:
         * event が来たら Window Store に保存する。
         */
        builder.stream(EVENTS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .process(
                        new EventStoreProcessorSupplier(),
                        STORE_NAME
                );

        /*
         * segmentation-results topic:
         * segmentation result が来たら Window Store から同じ userId の event を探す。
         */
        builder.stream(SEGMENTATION_RESULTS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .process(
                        new SegmentationResultReevaluationProcessorSupplier(),
                        STORE_NAME
                );

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[event-reevaluation] shutting down...");
            streams.close();
            latch.countDown();
        }));

        streams.start();

        System.out.println("[event-reevaluation] started. Waiting for events and segmentation results...");

        latch.await();
    }

    private class EventStoreProcessorSupplier implements ProcessorSupplier<String, String, Void, Void> {
        @Override
        public Processor<String, String, Void, Void> get() {
            return new EventStoreProcessor();
        }
    }

    private class EventStoreProcessor implements Processor<String, String, Void, Void> {
        private WindowStore<String, String> store;

        @Override
        public void init(ProcessorContext<Void, Void> context) {
            this.store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process (Record<String, String> record) {
            try {
                Event event = objectMapper.readValue(record.value(), Event.class);

                String userId = event.getUserId();
                long timestamp = event.getTrackedAt().toEpochMilli();

                store.put(userId, record.value(), timestamp);

                System.out.printf(
                        "[event-reevaluation] stored pending event userId=%s, eventId=%s, eventName=%s, trackedAt=%s%n",
                        userId,
                        event.getEventId(),
                        event.getEventName(),
                        event.getTrackedAt()
                );
            } catch (Exception e) {
                throw new RuntimeException("Failed to store pending event: " + record.value(), e);
            }
        }

        @Override
        public void close() {
        }
    }

    private class SegmentationResultReevaluationProcessorSupplier implements ProcessorSupplier<String, String, Void, Void> {
        @Override
        public Processor<String, String, Void, Void> get() {
            return new SegmentationResultReevaluationProcessor();
        }
    }

    private class SegmentationResultReevaluationProcessor implements Processor<String, String, Void, Void> {
        private WindowStore<String, String> store;

        @Override
        public void init(ProcessorContext<Void, Void> context) {
            this.store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(Record<String, String> record) {
            try {
                SegmentationResult result = objectMapper.readValue(record.value(), SegmentationResult.class);

                String userId = result.getUserId();

                System.out.printf(
                        "[event-reevaluation] received segmentation result userId=%s, segmentId=%s, matched=%s, evaluatedAt=%s%n",
                        result.getUserId(),
                        result.getSegmentId(),
                        result.isMatched(),
                        result.getEvaluatedAt()
                );

                if (!result.isMatched()) {
                    System.out.printf(
                            "[event-reevaluation] skip userId=%s because segmentId=%s matched=false%n",
                            result.getUserId(),
                            result.getSegmentId()
                    );
                    return;
                }

                /*
                 * segmentation result の evaluatedAt を基準に、
                 * 直近10分の pending event を探す。
                 */
                Instant from = result.getEvaluatedAt().minus(Duration.ofMinutes(10));
                Instant to = result.getEvaluatedAt().plus(Duration.ofSeconds(1));

                try (WindowStoreIterator<String> iterator = store.fetch(userId, from, to)) {
                    boolean found = false;

                    while (iterator.hasNext()) {
                        found = true;

                        KeyValue<Long, String> item = iterator.next();
                        Event event = objectMapper.readValue(item.value, Event.class);

                        if (!"purchase".equals(event.getEventName())) {
                            System.out.printf(
                                    "[event-reevaluation] found event but not target eventName userId=%s, eventName=%s%n",
                                    event.getUserId(),
                                    event.getEventName()
                            );
                            continue;
                        }

                        System.out.printf(
                                "[event-reevaluation] RE-EVALUATED MATCH userId=%s, eventId=%s, eventName=%s, segmentId=%s, eventTrackedAt=%s, resultEvaluatedAt=%s%n",
                                event.getUserId(),
                                event.getEventId(),
                                event.getEventName(),
                                result.getSegmentId(),
                                event.getTrackedAt(),
                                result.getEvaluatedAt()
                        );
                    }

                    if (!found) {
                        System.out.printf(
                                "[event-reevaluation] no pending events found userId=%s, segmentId=%s%n",
                                result.getUserId(),
                                result.getSegmentId()
                        );
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to reevaluate events: " + record.value(), e);
            }
        }

        @Override
        public void close() {
        }
    }
}