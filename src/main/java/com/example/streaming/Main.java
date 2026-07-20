package com.example.streaming;

public class Main {
  public static void main(String[] args) throws Exception {
    System.out.println("Streaming Lab started!");

    if(args.length == 0) {
      System.out.println("usage:");
      System.out.println(" ./gradlew run --args=\"produce\"");
      System.out.println("  ./gradlew run --args=\"consume consumer-a\"");
      System.out.println("  ./gradlew run --args=\"consume consumer-b\"");
      System.out.println("  ./gradlew run --args=\"streams-print\"");
      System.out.println("  ./gradlew run --args=\"streams-filter\"");
      System.out.println("  ./gradlew run --args=\"streams-to-topic\"");
      System.out.println("  ./gradlew run --args=\"streams-select-key\"");
      System.out.println("  ./gradlew run --args=\"streams-count-by-event-name\"");
      System.out.println("  ./gradlew run --args=\"write-cassandra\"");
      System.out.println("  ./gradlew run --args=\"streams-count-to-cassandra\"");
      System.out.println("  ./gradlew run --args=\"streams-read-rules\"");
      System.out.println("  ./gradlew run --args=\"streams-rules-global-table\"");
      System.out.println("  ./gradlew run --args=\"streams-event-rule-join\"");
      System.out.println("  ./gradlew run --args=\"streams-event-rule-to-postgres\"");
      System.out.println("  ./gradlew run --args=\"produce-duplicate\"");
      System.out.println("  ./gradlew run --args=\"streams-event-rule-with-failure\"");
      return;
    }

    String command = args[0];

    switch(command) {
      case "produce" -> {
        EventProducer producer = new EventProducer();
        producer.produceSampleEvents();
      }
      case "consume" -> {
        String consumerName = args.length >= 2 ? args[1] : "consumer-default";
        EventConsumer consumer = new EventConsumer(consumerName);
        consumer.consumeForever();
      }
      case "streams-print" -> {
        EventPrintStream stream = new EventPrintStream();
        stream.start();
      }
      case "streams-filter" -> {
        EventFilterStream stream = new EventFilterStream();
        stream.start();
      }
      case "streams-to-topic" -> {
        EventToTopicStream stream = new EventToTopicStream();
        stream.start();
      }
      case "streams-select-key" -> {
        EventSelectKeyStream stream = new EventSelectKeyStream();
        stream.start();
      }
      case "streams-count-by-event-name" -> {
        EventCountByNameStream stream = new EventCountByNameStream();
        stream.start();
      }
      case "write-cassandra" -> {
        CassandraEventCountWriter writer = new CassandraEventCountWriter();
        writer.writeSampleCounts();
      }
      case "streams-count-to-cassandra" -> {
        EventCountToCassandraStream stream = new EventCountToCassandraStream();
        stream.start();
      }
      case "streams-read-rules" -> {
        RuleReadStream stream = new RuleReadStream();
        stream.start();
      }
      case "streams-rules-global-table" -> {
        RuleGlobalTableStream stream = new RuleGlobalTableStream();
        stream.start();
      }
      case "streams-event-rule-join" -> {
        EventRuleJoinStream stream = new EventRuleJoinStream();
        stream.start();
      }
      case "streams-event-rule-to-postgres" -> {
        EventRuleToPostgresStream stream = new EventRuleToPostgresStream();
        stream.start();
      }
      case "produce-duplicate" -> {
        DuplicateEventProducer producer = new DuplicateEventProducer();
        producer.produceDuplicatePurchaseEvent();
      }
      case "streams-event-rule-with-failure" -> {
        EventRuleToPostgresWithFailureStream stream = new EventRuleToPostgresWithFailureStream();
        stream.start();
      }
      default -> {
        System.out.println("Unknown command: " + command);
      }
    }
  }
}