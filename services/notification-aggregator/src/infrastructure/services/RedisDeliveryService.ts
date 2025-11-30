import {
  IDeliveryDeduplicationService,
  IRetryScheduler,
} from "../../domain/interfaces/index";

export class RedisDeliveryService
  implements IDeliveryDeduplicationService, IRetryScheduler
{
  constructor(private client: any) {}

  async checkDeliveryDedup(dedupKey: string): Promise<boolean> {
    try {
      const result = await this.client.setNX(`delivered:${dedupKey}`, "1");
      return result !== null;
    } catch {
      return false;
    }
  }

  async scheduleRetry(
    messageId: string,
    channel: string,
    nextAttempt: number,
    delayMs: number
  ): Promise<void> {
    const nextRetryAt = Date.now() + delayMs;
    const payload = `${messageId}:${channel}:${nextAttempt}`;
    await this.client.zAdd("retries", [{ score: nextRetryAt, value: payload }]);
  }

  async getPendingRetries(): Promise<
    Array<{ messageId: string; channel: string; attempt: number }>
  > {
    const now = Date.now();
    const items = await this.client.zRangeByScore("retries", 0, now);

    const result = items.map((item: string) => {
      const [messageId, channel, attempt] = item.split(":");
      return { messageId, channel, attempt: parseInt(attempt, 10) };
    });

    // Remove processed items
    if (items.length > 0) {
      await this.client.zRemRangeByScore("retries", 0, now);
    }

    return result;
  }
}
