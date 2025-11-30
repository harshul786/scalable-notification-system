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

  async processDelivery(
    message: MessageToDeliver
  ): Promise<{ success: boolean; shouldRetry: boolean }> {
    const currentAttempt = message.attempt + 1;

    try {
      // Step 1: Check delivery deduplication
      const isNewDelivery = await this.deduplicationService.checkDeliveryDedup(
        message.dedupKey
      );
      if (!isNewDelivery) {
        return { success: true, shouldRetry: false };
      }

      // Step 2: Verify message status
      const status = await this.deliveryRepository.getStatus(message.messageId);
      if (status && status.finalDelivered) {
        return { success: true, shouldRetry: false };
      }

      // Step 3: Call provider
      const result = await this.provider.send(message.recipient, message.body);

      // Success: Update message status
      await this.deliveryRepository.updateStatus(
        message.messageId,
        "SENT",
        true,
        currentAttempt
      );

      const attempt: DeliveryAttempt = {
        messageId: message.messageId,
        attemptNumber: currentAttempt,
        status: "SUCCESS",
        providerResponse: JSON.stringify(result.response),
      };
      await this.deliveryRepository.saveAttempt(attempt);

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

      // Insert into delivery attempts (even for failures)
      const attempt: DeliveryAttempt = {
        messageId: message.messageId,
        attemptNumber: currentAttempt,
        status: "FAILED",
        error: errorMsg,
      };
      await this.deliveryRepository.saveAttempt(attempt);

      // Determine if we should retry
      if (currentAttempt >= this.MAX_ATTEMPTS) {
        // Max attempts reached: send to DLQ
        const dlqEntry: DLQEntry = {
          messageId: message.messageId,
          channel: message.channel,
          failureReason: errorMsg,
          maxAttemptsReached: true,
        };
        await this.deliveryRepository.saveDLQEntry(dlqEntry);

        // Publish to DLQ topic
        await this.eventPublisher.publishDLQMessage(
          message.messageId,
          message.channel,
          errorMsg
        );

        // Update message status
        await this.deliveryRepository.updateStatus(
          message.messageId,
          "FAILED",
          false,
          currentAttempt
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
          message: `Message failed after ${currentAttempt} attempts - moved to DLQ`,
          attempt: currentAttempt,
          error: errorMsg,
          timestamp: new Date().toISOString(),
        });

        // Also publish to errors topic
        await this.eventPublisher.publishError({
          service: "aggregator",
          traceId: message.traceId,
          messageId: message.messageId,
          userId: message.userId,
          channel: message.channel,
          level: "ERROR",
          message: `Message delivery failed after ${currentAttempt} attempts`,
          error: errorMsg,
          attempt: currentAttempt,
          timestamp: new Date().toISOString(),
        });

        return { success: false, shouldRetry: false };
      } else {
        // Schedule retry
        const delayMs = this.BACKOFF_DELAYS[currentAttempt] || 300000;
        await this.retryScheduler.scheduleRetry(
          message.messageId,
          message.channel,
          currentAttempt,
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
          status: "RETRYING",
          message: `Scheduling retry for attempt ${
            currentAttempt + 1
          } after ${delayMs}ms`,
          attempt: currentAttempt,
          error: errorMsg,
          timestamp: new Date().toISOString(),
        });

        return { success: false, shouldRetry: true };
      }
    }
  }
}
