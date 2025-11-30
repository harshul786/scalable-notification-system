import { Kafka } from "kafkajs";
import { Message } from "../../domain/entities/Message";
import { IMessagePublisher } from "../../domain/interfaces/index";

export class KafkaMessagePublisher implements IMessagePublisher {
  private producer: any;

  constructor(producer: any) {
    this.producer = producer;
  }

  async publishMessageEvent(message: Message): Promise<void> {
    const topic = `messages.${message.channel}`;

    await this.producer.send({
      topic,
      messages: [
        {
          key: message.userId,
          value: JSON.stringify(message),
          headers: {
            traceId: message.traceId,
            attempt: "0",
          },
        },
      ],
    });
  }

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
}
