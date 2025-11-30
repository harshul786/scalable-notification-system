import {
  DeliveryAttempt,
  DLQEntry,
  MessageToDeliver,
} from "../entities/Delivery";

// Repository interfaces
export interface IDeliveryAttemptRepository {
  save(attempt: DeliveryAttempt): Promise<void>;
  findDeliveryAttempts(messageId: string): Promise<DeliveryAttempt[]>;
}

export interface IDLQRepository {
  save(entry: DLQEntry): Promise<void>;
  findByMessageId(messageId: string): Promise<DLQEntry | null>;
}

export interface IMessageStatusRepository {
  updateStatus(
    messageId: string,
    status: "PENDING" | "SENT" | "FAILED",
    finalDelivered: boolean,
    attempts: number
  ): Promise<void>;
  getStatus(messageId: string): Promise<{
    finalDelivered: boolean;
    status: string;
    attempts: number;
  } | null>;
}

// Provider interface
export interface IProvider {
  send(
    recipient: string,
    body: string
  ): Promise<{ success: boolean; response: any }>;
}

// Deduplication interface
export interface IDeliveryDeduplicationService {
  checkDeliveryDedup(dedupKey: string): Promise<boolean>;
}

// Retry scheduler interface
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

// Event publisher interface
export interface IEventPublisher {
  publishLog(log: any): Promise<void>;
  publishRetryMessage(message: MessageToDeliver): Promise<void>;
  publishDLQMessage(
    messageId: string,
    channel: string,
    reason: string
  ): Promise<void>;
}
