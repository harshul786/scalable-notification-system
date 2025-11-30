import { Kafka } from "kafkajs";
import { IEventPublisher } from "../../domain/interfaces/index";

export class KafkaEventPublisher implements IEventPublisher {
  constructor(private producer: any, private kafka?: Kafka) {}

  async publishLog(log: any): Promise<void> {
    await this.producer.send({
      topic: "logs",
      messages: [
        {
          key: log.traceId,
          value: JSON.stringify(log),
        },
      ],
    });
  }

  async publishRetryMessage(message: any): Promise<void> {
    const topic = `messages.${message.channel}`;
    await this.producer.send({
      topic,
      messages: [
        {
          key: message.userId,
          value: JSON.stringify(message),
          headers: {
            traceId: message.traceId,
            attempt: message.attempt.toString(),
          },
        },
      ],
    });
  }

  async publishDLQMessage(
    messageId: string,
    channel: string,
    reason: string
  ): Promise<void> {
    const topic = `dlq.${channel}`;
    await this.producer.send({
      topic,
      messages: [
        {
          key: messageId,
          value: JSON.stringify({ messageId, channel, reason }),
        },
      ],
    });
  }
}
