package com.example.streaming;

public class Main {
  public static void main(String[] args) throws Exception {
    System.out.println("Streaming Lab started!");

    if(args.length == 0) {
      System.out.println("usage:");
      System.out.println(" ./gradlew run --args=\"produce\"");
      System.out.println("  ./gradlew run --args=\"consume consumer-a\"");
      System.out.println("  ./gradlew run --args=\"consume consumer-b\"");
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
      default -> {
        System.out.println("Unknown command: " + command);
      }
    }
  }
}