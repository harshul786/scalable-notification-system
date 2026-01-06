import {
  MessageToDeliver,
  DeliveryAttempt,
  DLQEntry,
} from "../models/Delivery";
import { IDeliveryDeduplicationService } from "./DeliveryDeduplicationService";
import { IRetryScheduler } from "./RetrySchedulerService";
import { IEventPublisher } from "./EventPublisherService";
import { IProvider } from "./ProviderFactory";
import { IDeliveryRepository } from "../repositories/DeliveryRepository";

/**
 * DeliveryProcessorService: DB-First State Machine
 *
 * Implements DB-anchored idempotency with provider-side idempotency tokens.
 *
 * State transitions:
 * - IN_PROGRESS → SENT (success) or RETRY_SCHEDULED (failure with retries left)
 * - IN_PROGRESS → FAILED (max attempts reached)
 *
 * Key principles:
 * 1. DB is written BEFORE calling provider (IN_PROGRESS state)
 * 2. Provider called with idempotency token (messageId or idempotencyKey)
 * 3. Provider failure → Schedule retry (RETRY_SCHEDULED state)
 * 4. Max attempts → Move to DLQ
 * 5. Never call provider twice for same message (dedup key in delivery table)
 * 6. Warm Redis cache AFTER DB commit succeeds
 *
 * Backoff strategy: 0s, 1s, 10s, 30s, 5m (then DLQ)
 */
export class DeliveryProcessorService {
  private readonly BACKOFF_DELAYS = [0, 1000, 10000, 30000, 300000]; // 0s, 1s, 10s, 30s, 5m
  private readonly MAX_ATTEMPTS = 4;

  constructor(
    private deliveryRepository: IDeliveryRepository,
    private provider: IProvider,
    private deduplicationService: IDeliveryDeduplicationService,
    private retryScheduler: IRetryScheduler,
    private eventPublisher: IEventPublisher
  ) {}

  /**
   * Process delivery with DB-first state machine
   *
   * Flow:
   * 1. Message already in DB with status=IN_PROGRESS (inserted by batch consumer)
   * 2. Check delivery dedup (Redis) - prevent duplicate provider calls
   * 3. Call provider with idempotency token
   * 4. On success: Update DB status=SENT, set finalDelivered=true
   * 5. On failure: Update DB status=RETRY_SCHEDULED or move to DLQ
   * 6. Warm Redis cache AFTER DB commit succeeds
   */
  async processDelivery(
    message: MessageToDeliver
  ): Promise<{
    success: boolean;
    shouldRetry: boolean;
    isDuplicate?: boolean;
  }> {
    const currentAttempt = message.attempt + 1;
    const providerIdempotencyToken = message.messageId; // Use messageId as provider token

    try {
      // STEP 1: Check delivery deduplication (Redis soft cache)
      // This prevents duplicate provider calls if Kafka replays the message
      const isNewDelivery = await this.deduplicationService.checkDeliveryDedup(
        message.dedupKey
      );

      if (!isNewDelivery) {
        // Already marked as delivered in Redis
        // Could be a Kafka replay - check DB for actual status
        const dbStatus = await this.deliveryRepository.getStatus(
          message.messageId
        );
        if (dbStatus?.finalDelivered) {
          console.log(
            `[DeliveryProcessor] Message ${message.messageId} already delivered (DB verified)`
          );
          return { success: true, shouldRetry: false, isDuplicate: true };
        }
      }

      // STEP 2: Call provider with idempotency token
      // Provider should be idempotent - same token = same result
      console.log(
        `[DeliveryProcessor] Calling ${message.channel} provider for ${message.messageId} (attempt ${currentAttempt})`
      );

      const result = await this.provider.send(message.recipient, message.body, {
        idempotencyToken: providerIdempotencyToken,
        attemptNumber: currentAttempt,
        headers: {
          "Idempotency-Key": providerIdempotencyToken,
          "Attempt-Number": currentAttempt.toString(),
        },
      });

      // STEP 3: Success - Update DB status BEFORE returning
      // This ensures DB is source of truth before we mark as success
      console.log(
        `[DeliveryProcessor] Provider call successful for ${message.messageId}`
      );

      await this.deliveryRepository.updateStatus(
        message.messageId,
        "SENT",
        true, // finalDelivered = true
        currentAttempt
      );

      // Record successful attempt in audit trail
      const attempt: DeliveryAttempt = {
        messageId: message.messageId,
        attemptNumber: currentAttempt,
        status: "SUCCESS",
        providerResponse: JSON.stringify(result.response),
      };
      await this.deliveryRepository.saveAttempt(attempt);

      // STEP 4: Warm Redis cache (AFTER DB commit)
      // Now that DB is committed, safe to cache
      await this.deduplicationService.setDeliveredCache(message.dedupKey);

      // Emit success log
      await this.eventPublisher.publishLog({
        service: "aggregator",
        traceId: message.traceId,
        messageId: message.messageId,
        userId: message.userId,
        dedupKey: message.dedupKey,
        channel: message.channel,
        status: "SENT",
        message: `Successfully delivered ${message.channel} message on attempt ${currentAttempt}`,
        attempt: currentAttempt,
        error: null,
        timestamp: new Date().toISOString(),
      });

      return { success: true, shouldRetry: false };
    } catch (error: any) {
      const errorMsg = error.message || "Unknown error";
      const errorCode = error.code || "UNKNOWN_ERROR";

      console.error(
        `[DeliveryProcessor] Provider call failed for ${message.messageId}:`,
        errorMsg
      );

      // Record failed attempt in audit trail
      const attempt: DeliveryAttempt = {
        messageId: message.messageId,
        attemptNumber: currentAttempt,
        status: "FAILED",
        error: errorMsg,
        providerResponse: errorCode,
      };
      await this.deliveryRepository.saveAttempt(attempt);

      // STEP 5: Determine retry vs DLQ
      if (currentAttempt >= this.MAX_ATTEMPTS) {
        // Max attempts reached: move to DLQ
        return this.handleMaxAttemptsReached(message, currentAttempt, errorMsg);
      } else {
        // Retry: Schedule next attempt with exponential backoff
        return this.handleRetry(message, currentAttempt, errorMsg);
      }
    }
  }

  /**
   * Handle max attempts reached - move to DLQ
   */
  private async handleMaxAttemptsReached(
    message: MessageToDeliver,
    attemptNumber: number,
    errorMsg: string
  ): Promise<{ success: boolean; shouldRetry: boolean }> {
    console.log(
      `[DeliveryProcessor] Max attempts (${attemptNumber}) reached for ${message.messageId}`
    );

    // Update DB status to FAILED
    await this.deliveryRepository.updateStatus(
      message.messageId,
      "FAILED",
      false, // Don't mark as delivered
      attemptNumber
    );

    // Save to DLQ
    const dlqEntry: DLQEntry = {
      messageId: message.messageId,
      channel: message.channel,
      failureReason: errorMsg,
      maxAttemptsReached: true,
    };
    await this.deliveryRepository.saveDLQEntry(dlqEntry);

    // Publish to DLQ topic for manual review
    await this.eventPublisher.publishDLQMessage(
      message.messageId,
      message.channel,
      errorMsg
    );

    // Emit failure log
    await this.eventPublisher.publishLog({
      service: "aggregator",
      traceId: message.traceId,
      messageId: message.messageId,
      userId: message.userId,
      dedupKey: message.dedupKey,
      channel: message.channel,
      status: "FAILED",
      message: `Message failed after ${attemptNumber} attempts - moved to DLQ`,
      attempt: attemptNumber,
      error: errorMsg,
      timestamp: new Date().toISOString(),
    });

    // Also publish to errors topic for alerting
    await this.eventPublisher.publishError({
      service: "aggregator",
      traceId: message.traceId,
      messageId: message.messageId,
      userId: message.userId,
      channel: message.channel,
      level: "ERROR",
      message: `Message delivery failed after ${attemptNumber} attempts`,
      error: errorMsg,
      attempt: attemptNumber,
      timestamp: new Date().toISOString(),
    });

    return { success: false, shouldRetry: false };
  }

  /**
   * Handle retry - schedule next attempt with exponential backoff
   * DO NOT create new DB row, reuse existing row with incremented attempt_count
   */
  private async handleRetry(
    message: MessageToDeliver,
    attemptNumber: number,
    errorMsg: string
  ): Promise<{ success: boolean; shouldRetry: boolean }> {
    const nextAttemptNumber = attemptNumber + 1;
    const delayMs = this.BACKOFF_DELAYS[attemptNumber] || 300000; // Default 5m

    console.log(
      `[DeliveryProcessor] Scheduling retry ${nextAttemptNumber} for ${message.messageId} (delay: ${delayMs}ms)`
    );

    // Update DB status to PENDING (retrying, don't increment attempt yet)
    await this.deliveryRepository.updateStatus(
      message.messageId,
      "PENDING",
      false, // Don't mark as delivered
      attemptNumber
    );

    // Schedule retry in Redis ZSET
    // DO NOT create new DB row - consumer will process with same messageId
    await this.retryScheduler.scheduleRetry(
      message.messageId,
      message.channel,
      attemptNumber, // Pass current attempt, retry scheduler increments
      delayMs
    );

    // Emit retry log
    await this.eventPublisher.publishLog({
      service: "aggregator",
      traceId: message.traceId,
      messageId: message.messageId,
      userId: message.userId,
      dedupKey: message.dedupKey,
      channel: message.channel,
      status: "RETRY_SCHEDULED",
      message: `Scheduling retry ${nextAttemptNumber} after ${delayMs}ms`,
      attempt: attemptNumber,
      error: errorMsg,
      timestamp: new Date().toISOString(),
    });

    return { success: false, shouldRetry: true };
  }
}
