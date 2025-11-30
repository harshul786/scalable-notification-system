export interface IEventPublisher {
  publishLog(log: any): Promise<void>;
  publishRetryMessage(message: any): Promise<void>;
  publishDLQMessage(
    messageId: string,
    channel: string,
    reason: string
  ): Promise<void>;
  publishError(error: any): Promise<void>;
}

export class EventPublisherService implements IEventPublisher {
  constructor(private producer: any) {}

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

  async publishError(error: any): Promise<void> {
    await this.producer.send({
      topic: "errors",
      messages: [
        {
          key: error.traceId || error.messageId || "error",
          value: JSON.stringify(error),
        },
      ],
    });
  }
}
