package com.example.streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.UUID;

public class EventProducer {
  private static final String TOPIC = "events";

  private final KafkaProducer<String, String> producer;
  private final ObjectMapper objectMapper;

  public EventProducer() {
    Properties props = new Properties();

    props.put("bootstrap.servers", "localhost:9092");

   // Kafka はネットワーク越しに byte 配列として message を送る。そのためのシリアライザを指定する。
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    props.put("acks", "all");

    this.producer = new KafkaProducer<>(props);
    this.objectMapper = new ObjectMapper();
    this.objectMapper.registerModule(new JavaTimeModule());
  }

  public void produceSampleEvents() throws Exception {
    List<String> userIds = List.of("user001", "user002", "user003");
    List<String> eventNames = List.of("app_open", "view_item", "purchase");

    for (int i =0; i < 10; i++) {
      String userId = userIds.get(i % userIds.size());
      String eventName = eventNames.get(i % eventNames.size());

      Event event = new Event("evt_" + UUID.randomUUID(), userId, eventName, Instant.now());
      
      String key = event.getUserId();
      String value = objectMapper.writeValueAsString(event);

      ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);

      Future<RecordMetadata> future = producer.send(record);
      // future.get() は送信完了まで待つ。なのでこのコードは実質的に 1件送るたびに完了を待つ同期送信となっている。
      RecordMetadata metadata = future.get();

      System.out.printf(
                    "Produced event: key=%s, partition=%d, offset=%d, value=%s%n",
                    key,
                    metadata.partition(),
                    metadata.offset(),
                    value
            );
    }

    producer.flush();
    producer.close();
  }

}