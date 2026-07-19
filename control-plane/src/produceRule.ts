import { Kafka } from "kafkajs";

type Rule = {
  ruleId: string;
  targetEventName: string;
  enabled: boolean;
};

const TOPIC = "rules";

const rules: Rule[] = [
  {
    ruleId: "rule_purchase",
    targetEventName: "purchase",
    enabled: true,
  },
  {
    ruleId: "rule_app_open",
    targetEventName: "app_open",
    enabled: true,
  },
  {
    ruleId: "rule_view_item_disabled",
    targetEventName: "view_item",
    enabled: false,
  },
];

async function main() {
  const kafka = new Kafka({
    clientId: "control-plane-rule-producer",
    brokers: ["localhost:9092"],
  });

  const producer = kafka.producer();

  await producer.connect();

  try {
    for (const rule of rules) {
      const value = JSON.stringify(rule);

      await producer.send({
        topic: TOPIC,
        messages: [
          {
            key: rule.ruleId,
            value,
          },
        ],
      });

      console.log(`[produce-rule] sent key=${rule.ruleId}, value=${value}`);
    }
  } finally {
    await producer.disconnect();
  }
}

main().catch((error) => {
  console.error("[produce-rule] failed", error);
  process.exit(1);
});
