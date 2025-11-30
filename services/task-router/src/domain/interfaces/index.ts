import { Message, CreateMessageRequest } from "../entities/Message";

// Repository interface - Domain abstraction for persistence
export interface IMessageRepository {
  save(message: Message): Promise<void>;
  findById(messageId: string): Promise<Message | null>;
}

// Event publisher interface - Domain abstraction for events
export interface IMessagePublisher {
  publishMessageEvent(message: Message): Promise<void>;
  publishLog(log: any): Promise<void>;
}

// Deduplication service interface
export interface IDeduplicationService {
  checkDuplicate(dedupKey: string): Promise<boolean>;
}

// Hash generation interface
export interface IHashService {
  generateDedupKey(
    body: string,
    userId: string,
    recipient: string,
    tenantId: string
  ): string;
}
