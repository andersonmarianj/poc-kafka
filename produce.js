import { Kafka } from "kafkajs";

import { config } from "./config.js";

const kafkaProducer = new Kafka({
  clientId: "my-app",
  brokers: config.brokers.producer,
});

/**
 * Produces messages to a Kafka topic.
 *
 * @param {string} topic - The name of the Kafka topic.
 * @param {Array<{value: string, partition?: number}>} messages - An array of messages to be sent to the topic.
 * @returns {Promise<void>} - A promise that resolves when the messages are successfully sent.
 */
export async function produce(topic, messages) {
  const producer = kafkaProducer.producer();

  await producer.connect();

  await producer.send({
    topic,
    messages,
  });

  await producer.disconnect();
}

produce(config.topics[0], [
  { value: "Hello KafkaJS user!", partition: 1 },
  { value: "This is a test message", partition: 0 },
]);
