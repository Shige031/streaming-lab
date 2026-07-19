package com.example.streaming;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class RuleSerde implements Serde<Rule> {
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public Serializer<Rule> serializer() {
    return (topic, data) -> {
      if(data == null) {
        return null;
      }

      try {
        String json = objectMapper.writeValueAsString(data);
        return json.getBytes(StandardCharsets.UTF_8);
      } catch(Exception e) {
        throw new RuntimeException("Failed to serialize rule: " + data, e);
      }
    };
  }

  @Override
  public Deserializer<Rule> deserializer() {
    return (topic, data) -> {
      if(data == null) {
        return null;
      }

      try {
        String json = new String(data, StandardCharsets.UTF_8);
        return objectMapper.readValue(json, Rule.class);
      } catch(Exception e) {
        throw new RuntimeException("Failed to deserialize rule: " + new String(data, StandardCharsets.UTF_8), e);
      }
    };
  }
}