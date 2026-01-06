import { Request, Response } from "express";
import { v4 as uuidv4 } from "uuid";
import {
  CreateMessageRequest,
  MessageResponse,
  Message,
} from "../models/Message";
import { MessageRepository } from "../repositories/MessageRepository";
import { MessagePublisher } from "../services/MessagePublisher";
import { DeduplicationService } from "../services/DeduplicationService";
import { HashService } from "../services/HashService";

/**
 * MessageController: Ingress point for message submission
 *
 * DB-Anchored Idempotency Model:
 * - Redis: Soft ingress cache only (fast path for duplicates)
 * - DB: Source of truth for idempotency
 * - Kafka: Unconditional enqueue (idempotency checked in consumer)
 *
 * Flow:
 * 1. Check Redis cache (fast path)
 *    - Hit: Return cached response, DO NOT enqueue
 *    - Miss: Continue
 * 2. Enqueue to Kafka (unconditionally)
 * 3. Consumer validates against DB (batch bulk check)
 * 4. Consumer inserts only NEW messages to DB
 * 5. Warm Redis cache after DB commit (not before)
 */
export class MessageController {
  constructor(
    private messageRepository: MessageRepository,
    private messagePublisher: MessagePublisher,
    private deduplicationService: DeduplicationService,
    private hashService: HashService
  ) {}

  async createMessage(req: Request, res: Response): Promise<void> {
    try {
      const request: CreateMessageRequest = req.body;
      const response = await this.handleCreateMessage(request);

      if (response.status === "DUPLICATE") {
        res.status(200).json(response);
      } else {
        res.status(202).json(response);
      }
    } catch (error: any) {
      const traceId = uuidv4();
      try {
        await this.messagePublisher.publishError({
          service: "router",
          traceId: traceId,
          level: "ERROR",
          message: `Error processing message: ${error.message}`,
          error: error.message,
          errorType: error.name || "UnknownError",
          timestamp: new Date().toISOString(),
        });
      } catch (logError) {
        // Silently handle error publishing
      }
      console.error("Error processing message:", error.message);
      res.status(400).json({ error: error.message, traceId });
    }
  }

  /**
   * Handle message creation with DB-anchored idempotency
   *
   * Key changes from previous implementation:
   * 1. Check Redis cache first (soft dedup)
   * 2. If Redis miss: Enqueue to Kafka unconditionally
   * 3. DO NOT write to DB in this service (moved to consumer)
   * 4. Consumer does bulk DB idempotency check
   */
  private async handleCreateMessage(
    request: CreateMessageRequest
  ): Promise<MessageResponse> {
    this.validateRequest(request);

    const traceId = uuidv4();
    const messageId = uuidv4();

    // Generate both idempotency key (from request) and dedup key (content hash)
    const idempotencyKey = request.idempotencyKey;
    const dedupKey = this.hashService.generateDedupKey(
      request.body,
      request.userId,
      request.recipient,
      request.tenantId
    );

    // STEP 1: Check Redis ingress cache (soft layer only)
    const isNew = await this.deduplicationService.checkAndCacheIngressRequest(
      request.tenantId,
      idempotencyKey
    );

    if (!isNew) {
      // Redis HIT: Request was recently seen
      // Check if we have cached response
      const cached = await this.deduplicationService.getCachedIngressRequest(
        request.tenantId,
        idempotencyKey
      );

      console.log(
        `[MessageController] Redis cache HIT for ${request.tenantId}/${idempotencyKey}`,
        cached
      );

      // Emit duplicate log
      try {
        await this.messagePublisher.publishLog({
          service: "router",
          traceId,
          spanId: uuidv4(),
          messageId: cached?.messageId || messageId,
          userId: request.userId,
          dedupKey,
          channel: request.channel,
          status: "DUPLICATE",
          message: `Duplicate request (Redis cache hit)`,
          timestamp: new Date().toISOString(),
        });
      } catch (logError) {
        // Silently handle log publishing
      }

      return {
        messageId: cached?.messageId || messageId,
        dedupKey,
        traceId,
        status: "DUPLICATE",
      };
    }

    // Redis MISS: Request is new (or Redis unavailable)
    // STEP 2: Enqueue to Kafka unconditionally
    // DB write and idempotency check happens in Kafka consumer

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
      idempotencyKey,
    };

    // Publish to Kafka without writing to DB
    await this.messagePublisher.publishMessageEvent(message);

    // Emit accepted log
    await this.messagePublisher.publishLog({
      service: "router",
      traceId,
      spanId: uuidv4(),
      messageId,
      userId: request.userId,
      dedupKey,
      channel: request.channel,
      status: "ACCEPTED",
      message: `Message accepted for ${request.channel} delivery to ${request.recipient}`,
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

    if (request.channel === "email") {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(request.recipient)) {
        throw new Error("Invalid email recipient");
      }
    }

    if (!request.body || request.body.trim().length === 0) {
      throw new Error("Empty body");
    }
  }
}
