/**
 * RetrySchedulerService: Exponential backoff retry scheduling
 *
 * Implements DB-anchored retry logic:
 * - DO NOT create new DB records on retry
 * - Reuse same message row, increment attempt_count
 * - Schedule retry in Redis ZSET with exponential backoff
 * - Retry scheduler polls every 100ms and republishes to Kafka
 * - Consumer processes as same messageId (idempotency handled in consumer)
 *
 * Backoff delays: 0s, 1s, 10s, 30s, 5m (then DLQ)
 */
export interface IRetryScheduler {
  scheduleRetry(
    messageId: string,
    channel: string,
    currentAttempt: number,
    delayMs: number
  ): Promise<void>;

  getPendingRetries(): Promise<
    Array<{ messageId: string; channel: string; attempt: number }>
  >;

  getRetryStats(): Promise<{ pendingCount: number; oldestRetryAge: number }>;
}

export class RetrySchedulerService implements IRetryScheduler {
  constructor(private client: any) {}

  /**
   * Schedule a retry for a message
   *
   * @param messageId - Message ID (same as original request)
   * @param channel - Channel (email, sms, whatsapp)
   * @param currentAttempt - Current attempt number (1, 2, 3, ...)
   * @param delayMs - Delay before retry (exponential backoff)
   */
  async scheduleRetry(
    messageId: string,
    channel: string,
    currentAttempt: number,
    delayMs: number
  ): Promise<void> {
    try {
      const nextRetryAt = Date.now() + delayMs;
      const nextAttempt = currentAttempt + 1;
      const payload = `${messageId}:${channel}:${nextAttempt}`;

      console.log(
        `[RetryScheduler] Scheduled ${messageId} for retry ${nextAttempt} at ${new Date(
          nextRetryAt
        ).toISOString()}`
      );

      // Store in Redis ZSET sorted by retry timestamp
      // Score = unix timestamp when retry should happen
      // Value = {messageId}:{channel}:{attemptNumber}
      await this.client.zAdd("retries", [
        { score: nextRetryAt, value: payload },
      ]);
    } catch (error) {
      console.error(
        `[RetryScheduler] Failed to schedule retry for ${messageId}:`,
        error
      );
      throw error;
    }
  }

  /**
   * Get all retries that are due (scheduled time <= now)
   *
   * @returns Array of retries to process
   */
  async getPendingRetries(): Promise<
    Array<{ messageId: string; channel: string; attempt: number }>
  > {
    try {
      const now = Date.now();

      // Query: Get all items with score <= now (due for retry)
      const items = await this.client.zRangeByScore(
        "retries",
        0,
        now,
        "BYSCORE"
      );

      const result = items.map((item: string) => {
        const [messageId, channel, attempt] = item.split(":");
        return {
          messageId,
          channel,
          attempt: parseInt(attempt, 10),
        };
      });

      // Remove processed items from Redis
      // This prevents re-processing same retry multiple times
      if (items.length > 0) {
        await this.client.zRemRangeByScore("retries", 0, now);
        console.log(
          `[RetryScheduler] Found ${items.length} pending retries, removed from queue`
        );
      }

      return result;
    } catch (error) {
      console.error(`[RetryScheduler] Error getting pending retries:`, error);
      return [];
    }
  }

  /**
   * Get retry queue statistics
   * Useful for monitoring and alerting
   */
  async getRetryStats(): Promise<{
    pendingCount: number;
    oldestRetryAge: number;
  }> {
    try {
      // Get total count
      const pendingCount = await this.client.zCard("retries");

      // Get oldest item (lowest score)
      const oldest = await this.client.zRange("retries", 0, 0, {
        BYSCORE: true,
        LIMIT: { offset: 0, count: 1 },
      });

      let oldestRetryAge = 0;
      if (oldest.length > 0) {
        const oldestItem = await this.client.zScore("retries", oldest[0]);
        oldestRetryAge = Math.max(0, Date.now() - oldestItem);
      }

      return {
        pendingCount,
        oldestRetryAge,
      };
    } catch (error) {
      console.error(`[RetryScheduler] Error getting stats:`, error);
      return { pendingCount: 0, oldestRetryAge: 0 };
    }
  }

  /**
   * Cancel a scheduled retry (used if message processed early)
   * @param messageId - Message ID to cancel
   * @param channel - Channel
   */
  async cancelRetry(messageId: string, channel: string): Promise<void> {
    try {
      // Remove all retries for this message (any attempt number)
      const pattern = `${messageId}:${channel}:*`;
      const keys = await this.client.keys(pattern);

      for (const key of keys) {
        await this.client.zRem("retries", key);
      }

      console.log(
        `[RetryScheduler] Cancelled all pending retries for ${messageId}`
      );
    } catch (error) {
      console.error(`[RetryScheduler] Failed to cancel retry:`, error);
    }
  }
}
