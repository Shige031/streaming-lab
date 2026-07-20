package com.example.streaming;

public class MatchedEventProcessor {
    public void process(MatchedEvent matchedEvent) {
        Event event = matchedEvent.getEvent();

        /*
         * 学習用:
         * purchase event はわざと失敗させる。
         */
        if ("purchase".equals(event.getEventName())) {
            throw new RuntimeException("Simulated failure for purchase event");
        }

        System.out.printf(
                "[matched-event-processor] processed eventId=%s, userId=%s, eventName=%s%n",
                event.getEventId(),
                event.getUserId(),
                event.getEventName()
        );
    }
}