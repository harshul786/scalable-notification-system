/**
 * IdempotencyMetricsService: Observability for DB-anchored idempotency
 *
 * Tracks metrics for:
 * - Redis ingress cache hits/misses
 * - DB bulk idempotency checks
 * - Duplicate skipping
 * - Retry attempts and success rates
 * - Latency per batch
 * - Provider performance
 */

import { Pool } from "mysql2/promise";

export interface IdempotencyMetrics {
  // Ingress cache metrics
  ingressCacheHits: number;
  ingressCacheMisses: number;

  // Batch processing metrics
  batchesProcessed: number;
  messagesProcessed: number;
  messagesNewCount: number;
  messagesDuplicateCount: number;

  // Delivery metrics
  messagesDeliveredCount: number;
  messagesFailedCount: number;
  messagesRetryScheduledCount: number;
  messagesInDLQCount: number;

  // Retry metrics
  retryAttemptCount: number;
  retrySuccessCount: number;
  retryFailureCount: number;
  averageAttemptsPerMessage: number;

  // Latency metrics
  averageBatchLatencyMs: number;
  averageDeliveryLatencyMs: number;
  averageDedupCheckLatencyMs: number;

  // Provider-specific metrics
  emailSuccessRate: number;
  smsSuccessRate: number;
  whatsappSuccessRate: number;
}

export class IdempotencyMetricsService {
  private metrics: Map<string, number> = new Map();
  private batchLatencies: number[] = [];
  private deliveryLatencies: number[] = [];
  private dedupCheckLatencies: number[] = [];

  constructor(private pool: Pool) {
    this.initializeMetrics();
  }

  private initializeMetrics(): void {
    this.metrics.set("ingressCacheHits", 0);
    this.metrics.set("ingressCacheMisses", 0);
    this.metrics.set("batchesProcessed", 0);
    this.metrics.set("messagesProcessed", 0);
    this.metrics.set("messagesNewCount", 0);
    this.metrics.set("messagesDuplicateCount", 0);
    this.metrics.set("messagesDeliveredCount", 0);
    this.metrics.set("messagesFailedCount", 0);
    this.metrics.set("messagesRetryScheduledCount", 0);
    this.metrics.set("messagesInDLQCount", 0);
    this.metrics.set("retryAttemptCount", 0);
    this.metrics.set("retrySuccessCount", 0);
    this.metrics.set("retryFailureCount", 0);
  }

  /**
   * Record ingress cache hit
   */
  recordIngressCacheHit(): void {
    this.increment("ingressCacheHits");
  }

  /**
   * Record ingress cache miss
   */
  recordIngressCacheMiss(): void {
    this.increment("ingressCacheMisses");
  }

  /**
   * Record batch processing
   */
  recordBatchProcessed(
    totalMessages: number,
    newMessages: number,
    duplicates: number,
    latencyMs: number
  ): void {
    this.increment("batchesProcessed");
    this.metrics.set(
      "messagesProcessed",
      (this.metrics.get("messagesProcessed") || 0) + totalMessages
    );
    this.metrics.set(
      "messagesNewCount",
      (this.metrics.get("messagesNewCount") || 0) + newMessages
    );
    this.metrics.set(
      "messagesDuplicateCount",
      (this.metrics.get("messagesDuplicateCount") || 0) + duplicates
    );
    this.batchLatencies.push(latencyMs);
  }

  /**
   * Record successful delivery
   */
  recordDeliverySuccess(latencyMs: number, attemptNumber: number): void {
    this.increment("messagesDeliveredCount");
    this.deliveryLatencies.push(latencyMs);
    this.recordAttemptMetrics(attemptNumber, true);
  }

  /**
   * Record failed delivery (final failure, in DLQ)
   */
  recordDeliveryFailure(latencyMs: number, attemptNumber: number): void {
    this.increment("messagesFailedCount");
    this.increment("messagesInDLQCount");
    this.deliveryLatencies.push(latencyMs);
    this.recordAttemptMetrics(attemptNumber, false);
  }

  /**
   * Record retry scheduled
   */
  recordRetryScheduled(): void {
    this.increment("messagesRetryScheduledCount");
  }

  /**
   * Record attempt metrics
   */
  private recordAttemptMetrics(attemptNumber: number, success: boolean): void {
    this.increment("retryAttemptCount");
    if (success) {
      this.increment("retrySuccessCount");
    } else {
      this.increment("retryFailureCount");
    }
  }

  /**
   * Record dedup check latency
   */
  recordDedupCheckLatency(latencyMs: number): void {
    this.dedupCheckLatencies.push(latencyMs);
  }

  /**
   * Get current metrics snapshot
   */
  getMetrics(): IdempotencyMetrics {
    return {
      ingressCacheHits: this.metrics.get("ingressCacheHits") || 0,
      ingressCacheMisses: this.metrics.get("ingressCacheMisses") || 0,
      batchesProcessed: this.metrics.get("batchesProcessed") || 0,
      messagesProcessed: this.metrics.get("messagesProcessed") || 0,
      messagesNewCount: this.metrics.get("messagesNewCount") || 0,
      messagesDuplicateCount: this.metrics.get("messagesDuplicateCount") || 0,
      messagesDeliveredCount: this.metrics.get("messagesDeliveredCount") || 0,
      messagesFailedCount: this.metrics.get("messagesFailedCount") || 0,
      messagesRetryScheduledCount:
        this.metrics.get("messagesRetryScheduledCount") || 0,
      messagesInDLQCount: this.metrics.get("messagesInDLQCount") || 0,
      retryAttemptCount: this.metrics.get("retryAttemptCount") || 0,
      retrySuccessCount: this.metrics.get("retrySuccessCount") || 0,
      retryFailureCount: this.metrics.get("retryFailureCount") || 0,
      averageAttemptsPerMessage: this.calculateAverageAttempts(),
      averageBatchLatencyMs: this.calculateAverage(this.batchLatencies),
      averageDeliveryLatencyMs: this.calculateAverage(this.deliveryLatencies),
      averageDedupCheckLatencyMs: this.calculateAverage(
        this.dedupCheckLatencies
      ),
      emailSuccessRate: 0, // To be populated from DB
      smsSuccessRate: 0,
      whatsappSuccessRate: 0,
    };
  }

  /**
   * Get provider-specific success rates from DB
   */
  async getProviderSuccessRates(): Promise<{
    email: number;
    sms: number;
    whatsapp: number;
  }> {
    const connection = await this.pool.getConnection();
    try {
      const query = `
        SELECT
          channel,
          SUM(CASE WHEN status = 'SENT' THEN 1 ELSE 0 END) as successCount,
          COUNT(*) as totalCount
        FROM messages
        WHERE createdAt > DATE_SUB(NOW(), INTERVAL 1 HOUR)
        GROUP BY channel
      `;

      const [rows] = await connection.execute(query);
      const result = (rows as any[]) || [];

      const rates: Record<string, number> = {
        email: 0,
        sms: 0,
        whatsapp: 0,
      };

      for (const row of result) {
        const successRate =
          row.totalCount > 0 ? (row.successCount / row.totalCount) * 100 : 0;
        rates[row.channel] = Math.round(successRate * 100) / 100;
      }

      return {
        email: rates.email || 0,
        sms: rates.sms || 0,
        whatsapp: rates.whatsapp || 0,
      };
    } finally {
      connection.release();
    }
  }

  /**
   * Get retry success rate
   */
  async getRetrySuccessRate(): Promise<number> {
    const totalRetries = this.metrics.get("retryAttemptCount") || 0;
    const successRetries = this.metrics.get("retrySuccessCount") || 0;

    if (totalRetries === 0) return 0;
    return Math.round((successRetries / totalRetries) * 10000) / 100;
  }

  /**
   * Get deduplication effectiveness (% of duplicates caught)
   */
  getDeduplicationEffectiveness(): {
    cacheHitRate: number;
    duplicateRate: number;
  } {
    const totalCache =
      (this.metrics.get("ingressCacheHits") || 0) +
      (this.metrics.get("ingressCacheMisses") || 0);
    const cacheHitRate =
      totalCache > 0
        ? Math.round(
            ((this.metrics.get("ingressCacheHits") || 0) / totalCache) * 10000
          ) / 100
        : 0;

    const totalProcessed = this.metrics.get("messagesProcessed") || 0;
    const duplicateRate =
      totalProcessed > 0
        ? Math.round(
            ((this.metrics.get("messagesDuplicateCount") || 0) /
              totalProcessed) *
              10000
          ) / 100
        : 0;

    return { cacheHitRate, duplicateRate };
  }

  /**
   * Reset metrics (useful for periodic reporting)
   */
  reset(): void {
    this.initializeMetrics();
    this.batchLatencies = [];
    this.deliveryLatencies = [];
    this.dedupCheckLatencies = [];
  }

  private increment(key: string): void {
    this.metrics.set(key, (this.metrics.get(key) || 0) + 1);
  }

  private calculateAverage(values: number[]): number {
    if (values.length === 0) return 0;
    const sum = values.reduce((a, b) => a + b, 0);
    return Math.round((sum / values.length) * 100) / 100;
  }

  private calculateAverageAttempts(): number {
    const totalDelivered = this.metrics.get("messagesDeliveredCount") || 0;
    if (totalDelivered === 0) return 0;

    const totalAttempts = this.metrics.get("retryAttemptCount") || 0;
    return Math.round((totalAttempts / totalDelivered) * 100) / 100;
  }
}
