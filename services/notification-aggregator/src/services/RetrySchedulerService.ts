export interface IRetryScheduler {
  scheduleRetry(
    messageId: string,
    channel: string,
    nextAttempt: number,
    delayMs: number
  ): Promise<void>;

  getPendingRetries(): Promise<
    Array<{ messageId: string; channel: string; attempt: number }>
  >;
}

export class RetrySchedulerService implements IRetryScheduler {
  constructor(private client: any) {}

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
