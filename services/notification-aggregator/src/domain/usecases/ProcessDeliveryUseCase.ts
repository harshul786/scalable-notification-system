import {
  MessageToDeliver,
  DeliveryAttempt,
  DLQEntry,
} from "../entities/Delivery";
import {
  IDeliveryAttemptRepository,
  IDLQRepository,
  IMessageStatusRepository,
  IProvider,
  IDeliveryDeduplicationService,
  IRetryScheduler,
  IEventPublisher,
} from "../interfaces/index";

// Use case: Process delivery for a message
export class ProcessDeliveryUseCase {
  private readonly BACKOFF_DELAYS = [0, 1000, 10000, 30000, 300000]; // 0s, 1s, 10s, 30s, 5m
  private readonly MAX_ATTEMPTS = 4;

  constructor(
    private deliveryAttemptRepository: IDeliveryAttemptRepository,
    private dlqRepository: IDLQRepository,
    private messageStatusRepository: IMessageStatusRepository,
    private provider: IProvider,
    private deduplicationService: IDeliveryDeduplicationService,
    private retryScheduler: IRetryScheduler,
    private eventPublisher: IEventPublisher
  ) {}

  async execute(
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
      const status = await this.messageStatusRepository.getStatus(
        message.messageId
      );
      if (status && status.finalDelivered) {
        return { success: true, shouldRetry: false };
      }

      // Step 3: Call provider
      const result = await this.provider.send(message.recipient, message.body);

      // Success: Update message status
      await this.messageStatusRepository.updateStatus(
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
      await this.deliveryAttemptRepository.save(attempt);

      // Emit success log
      await this.eventPublisher.publishLog({
        service: "aggregator",
        traceId: message.traceId,
        messageId: message.messageId,
        userId: message.userId,
        dedupKey: message.dedupKey,
        channel: message.channel,
        status: "SENT",
        attempt: currentAttempt,
        error: null,
        timestamp: new Date().toISOString(),
      });

      return { success: true, shouldRetry: false };
    } catch (error: any) {
      // Save failed attempt
      const attempt: DeliveryAttempt = {
        messageId: message.messageId,
        attemptNumber: currentAttempt,
        status: "FAILED",
        error: error.message,
      };
      await this.deliveryAttemptRepository.save(attempt);

      // Check max attempts
      if (currentAttempt >= this.MAX_ATTEMPTS) {
        // Send to DLQ
        await this.messageStatusRepository.updateStatus(
          message.messageId,
          "FAILED",
          false,
          currentAttempt
        );

        const dlqEntry: DLQEntry = {
          messageId: message.messageId,
          channel: message.channel,
          failureReason: `Max attempts (${currentAttempt}) reached: ${error.message}`,
          maxAttemptsReached: true,
        };
        await this.dlqRepository.save(dlqEntry);

        // Emit DLQ log
        await this.eventPublisher.publishLog({
          service: "aggregator",
          traceId: message.traceId,
          messageId: message.messageId,
          userId: message.userId,
          dedupKey: message.dedupKey,
          channel: message.channel,
          status: "DLQ",
          attempt: currentAttempt,
          error: error.message,
          timestamp: new Date().toISOString(),
        });

        return { success: false, shouldRetry: false };
      }

      // Schedule retry with exponential backoff
      const delayMs =
        this.BACKOFF_DELAYS[
          Math.min(currentAttempt, this.BACKOFF_DELAYS.length - 1)
        ];
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
        attempt: currentAttempt + 1,
        error: error.message,
        timestamp: new Date().toISOString(),
      });

      return { success: false, shouldRetry: true };
    }
  }
}
