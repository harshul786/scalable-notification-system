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
      const { v4: uuidv4 } = require("uuid");
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

  private async handleCreateMessage(
    request: CreateMessageRequest
  ): Promise<MessageResponse> {
    this.validateRequest(request);

    const traceId = uuidv4();
    const messageId = uuidv4();
    const dedupKey = this.hashService.generateDedupKey(
      request.body,
      request.userId,
      request.recipient,
      request.tenantId
    );

    const isNew = await this.deduplicationService.checkDuplicate(dedupKey);

    if (!isNew) {
      // Emit duplicate log
      try {
        await this.messagePublisher.publishLog({
          service: "router",
          traceId,
          spanId: uuidv4(),
          messageId,
          userId: request.userId,
          dedupKey,
          channel: request.channel,
          status: "DUPLICATE",
          message: `Duplicate message detected`,
          error: "Duplicate message",
          attempt: 0,
          timestamp: new Date().toISOString(),
        });
      } catch (logError) {
        // Silently handle log publishing
      }

      return {
        messageId,
        dedupKey,
        traceId,
        status: "DUPLICATE",
      };
    }

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

    await this.messageRepository.save(message);
    await this.messagePublisher.publishMessageEvent(message);
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
