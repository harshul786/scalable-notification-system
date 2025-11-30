import { StructuredLog } from "../models/Log";
import { Kafka } from "kafkajs";

export interface ILogConsumer {
  consume(handler: (log: StructuredLog) => Promise<void>): Promise<void>;
  consumeMultiple(
    topics: string[],
    handler: (log: any) => Promise<void>
  ): Promise<void>;
}

export class LogConsumerService implements ILogConsumer {
  private consumer: any;

  constructor(kafka: Kafka) {
    this.consumer = kafka.consumer({
      groupId: "logger-group",
      sessionTimeout: 30000,
    });
  }

  async consume(handler: (log: StructuredLog) => Promise<void>): Promise<void> {
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: "logs", fromBeginning: false });

    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }: any) => {
        try {
          const log = JSON.parse(
            message.value?.toString() || "{}"
          ) as StructuredLog;
          await handler(log);
        } catch (error) {
          console.error("Error processing log message:", error);
        }
      },
    });
  }

  async consumeMultiple(
    topics: string[],
    handler: (log: any) => Promise<void>
  ): Promise<void> {
    await this.consumer.connect();
    for (const topic of topics) {
      await this.consumer.subscribe({ topic, fromBeginning: false });
    }

    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }: any) => {
        try {
          const log = JSON.parse(message.value?.toString() || "{}");
          await handler(log);
        } catch (error) {
          console.error("Error processing message from topic:", topic, error);
        }
      },
    });
  }
}
