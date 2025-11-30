import { Kafka, Consumer, logLevel } from "kafkajs";
import { StructuredLog } from "../../domain/entities/Log";
import { ILogConsumer } from "../../domain/interfaces/index";

export class KafkaLogConsumer implements ILogConsumer {
  private consumer: Consumer;

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
      eachMessage: async ({ topic, partition, message }) => {
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
}
