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
      default -> {
        System.out.println("Unknown command: " + command);
      }
    }
  }
}