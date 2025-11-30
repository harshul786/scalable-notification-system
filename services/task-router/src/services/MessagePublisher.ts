import { Message } from "../models/Message";

export class MessagePublisher {
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

  async publishError(error: any): Promise<void> {
    await this.producer.send({
      topic: "errors",
      messages: [
        {
          key: error.traceId || "error",
          value: JSON.stringify(error),
        },
      ],
    });
  }
}
