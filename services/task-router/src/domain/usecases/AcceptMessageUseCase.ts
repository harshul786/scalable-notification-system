import { v4 as uuidv4 } from "uuid";
import {
  Message,
  CreateMessageRequest,
  MessageResponse,
} from "../entities/Message";
import {
  IMessageRepository,
  IMessagePublisher,
  IDeduplicationService,
  IHashService,
} from "../interfaces/index";

// Use case: Accept and route a message
export class AcceptMessageUseCase {
  constructor(
    private messageRepository: IMessageRepository,
    private messagePublisher: IMessagePublisher,
    private deduplicationService: IDeduplicationService,
    private hashService: IHashService
  ) {}

  async execute(request: CreateMessageRequest): Promise<MessageResponse> {
    // Validate input
    this.validateRequest(request);

    const traceId = uuidv4();
    const messageId = uuidv4();
    const dedupKey = this.hashService.generateDedupKey(
      request.body,
      request.userId,
      request.recipient,
      request.tenantId
    );

    // Check for duplicates using Redis
    const isNew = await this.deduplicationService.checkDuplicate(dedupKey);

    if (!isNew) {
      return {
        messageId,
        dedupKey,
        traceId,
        status: "DUPLICATE",
      };
    }

    // Create message entity
    const message: Message = {
      messageId,
      dedupKey,
      userId: request.userId,
      tenantId: request.tenantId,
      channel: request.channel,
      recipient: request.recipient,
      body: request.body,
      traceId,
      status: "PENDING",
      finalDelivered: false,
      attempts: 0,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    // Persist to database
    await this.messageRepository.save(message);

    // Publish to Kafka
    await this.messagePublisher.publishMessageEvent(message);

    // Emit log
    await this.messagePublisher.publishLog({
      service: "router",
      traceId,
      spanId: uuidv4(),
      messageId,
      userId: request.userId,
      dedupKey,
      channel: request.channel,
      status: "ACCEPTED",
      timestamp: new Date().toISOString(),
    });

    return {
      messageId,
      dedupKey,
      traceId,
      status: "ACCEPTED",
    };
  }

  private validateRequest(request: CreateMessageRequest): void {
    if (
      !request.tenantId ||
      !request.userId ||
      !request.idempotencyKey ||
      !request.channel ||
      !request.recipient ||
      !request.body
    ) {
      throw new Error("Missing required fields");
    }

    if (!["email", "sms", "whatsapp"].includes(request.channel)) {
      throw new Error("Invalid channel");
    }

    // Validate email format for email channel
    if (request.channel === "email") {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(request.recipient)) {
        throw new Error("Invalid email recipient");
      }
    }
  }
}
