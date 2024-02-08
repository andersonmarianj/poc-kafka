import { Kafka } from "kafkajs";

import { config } from "./config.js";

const kafkaConsumer = new Kafka({
  clientId: "my-app",
  brokers: config.brokers.consumer,
  logLevel: 0,
});

async function consume(groupId) {
  const consumer = kafkaConsumer.consumer({ groupId });

  await consumer.connect();

  await consumer.subscribe({
    topics: config.topics,
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if (partition == 0) {
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }

      console.log("message", groupId);
      console.log({
        value: message.value.toString(),
        offset: message.offset,
        timestamp: message.timestamp,
        partition,
        topic,
      });
    },
  });
}

await consume("test-group-1");
await consume("test-group-2");

console.log("Consumers started");
