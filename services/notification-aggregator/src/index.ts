import * as dotenv from "dotenv";
dotenv.config();

import { createPool, Pool } from "mysql2/promise";
import * as redis from "redis";
import { Kafka } from "kafkajs";
import { DeliveryRepository } from "./repositories/DeliveryRepository";
import { DeliveryDeduplicationService } from "./services/DeliveryDeduplicationService";
import { RetrySchedulerService } from "./services/RetrySchedulerService";
import { EventPublisherService } from "./services/EventPublisherService";
import { BatchIdempotencyService } from "./services/BatchIdempotencyService";
import {
  EmailProvider,
  SMSProvider,
  WhatsAppProvider,
  ProviderFactory,
} from "./services/ProviderFactory";
import { DeliveryProcessorService } from "./services/DeliveryProcessorService";
import { DeliveryController } from "./controllers/DeliveryController";
import { MessageToDeliver } from "./models/Delivery";
import type { EachBatchPayload, KafkaMessage } from "kafkajs";

interface ParsedKafkaMessage extends MessageToDeliver {
  kafkaOffset: string;
  kafkaIndex: number;
  idempotencyKey?: string; // Optional field from message payload
}

/**
 * Kafka Consumer with Batch Idempotent Processing
 *
 * Implements DB-anchored idempotency:
 * 1. Consumer receives batch of messages from Kafka
 * 2. Extracts idempotency keys from batch
 * 3. Runs ONE bulk DB query to find already-processed messages
 * 4. Partitions batch into NEW and DUPLICATE messages
 * 5. Inserts NEW messages with status=IN_PROGRESS (atomically)
 * 6. Processes only NEW messages (calls providers, updates DB)
 * 7. Skips DUPLICATE messages (no side-effects)
 * 8. Only commits Kafka offset on successful processing
 */

async function bootstrap(): Promise<void> {
  try {
    // Initialize MySQL
    const mysqlPool: Pool = await createPool({
      host: process.env.MYSQL_HOST || "localhost",
      user: process.env.MYSQL_USER || "notif_user",
      password: process.env.MYSQL_PASSWORD || "notif-password",
      database: process.env.MYSQL_DATABASE || "notification_db",
      waitForConnections: true,
      connectionLimit: 10,
    });

    // Initialize Redis
    const redisClient: any = redis.createClient({
      url: `redis://${process.env.REDIS_HOST || "localhost"}:${
        process.env.REDIS_PORT || 6379
      }`,
    });
    redisClient.on("error", (err: Error) => {
      // Log Redis errors silently
    });
    await redisClient.connect();

    // Initialize Kafka
    const kafka = new Kafka({
      clientId: "notification-aggregator",
      brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
    });

    const consumer = kafka.consumer({
      groupId: "delivery-workers-group",
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
    });
    await consumer.connect();

    const producer = kafka.producer({ idempotent: true });
    await producer.connect();

    // Initialize repositories and services
    const deliveryRepository = new DeliveryRepository(mysqlPool);
    const deduplicationService = new DeliveryDeduplicationService(redisClient);
    const retryScheduler = new RetrySchedulerService(redisClient);
    const eventPublisher = new EventPublisherService(producer);
    const batchIdempotencyService = new BatchIdempotencyService(mysqlPool);

    // Subscribe to message topics
    const topics = ["messages.email", "messages.sms", "messages.whatsapp"];
    for (const topic of topics) {
      await consumer.subscribe({ topic, fromBeginning: false });
    }

    // Retry scheduler loop
    let retryInterval: ReturnType<typeof setInterval>;
    retryInterval = setInterval(async () => {
      try {
        const retries = await retryScheduler.getPendingRetries();
        for (const retry of retries) {
          // Republish to Kafka for reprocessing
          await eventPublisher.publishRetryMessage({
            messageId: retry.messageId,
            channel: retry.channel,
            attempt: retry.attempt,
            traceId: `retry-${retry.messageId}`,
          });
        }
      } catch (error) {
        // Silently handle retry scheduler errors
      }
    }, 100); // Poll every 100ms

    // Start consuming messages with BATCH PROCESSING
    await consumer.run({
      eachBatch: async ({ batch, resolveOffset, heartbeat }: any) => {
        const { topic, messages } = batch;
        const channel = topic.replace("messages.", "");

        console.log(
          `[BatchConsumer] Processing batch of ${messages.length} messages from ${topic}`
        );

        try {
          // STEP 1: Parse all messages in batch
          const parsedMessages = messages.map(
            (msg: KafkaMessage, index: number): ParsedKafkaMessage => {
              const msgData = JSON.parse(
                msg.value?.toString() || "{}"
              ) as MessageToDeliver;

              // Ensure attempt is a number
              if (typeof msgData.attempt === "string") {
                msgData.attempt = parseInt(msgData.attempt, 10);
              }
              if (!msgData.attempt) {
                msgData.attempt = 0;
              }

              return {
                ...msgData,
                kafkaOffset: msg.offset,
                kafkaIndex: index,
              };
            }
          );

          // STEP 2: Bulk DB idempotency check
          const idempotencyResult =
            await batchIdempotencyService.checkBatchIdempotency(
              parsedMessages.map((m: ParsedKafkaMessage) => ({
                messageId: m.messageId,
                tenantId: m.tenantId,
                idempotencyKey: m.idempotencyKey || m.dedupKey, // Use provided key or dedup key
                dedupKey: m.dedupKey,
                channel: m.channel,
                userId: m.userId,
                recipient: m.recipient,
                body: m.body,
                traceId: m.traceId,
                attempt: m.attempt,
              }))
            );

          const { newMessages, duplicateMessages, totalProcessed } =
            idempotencyResult;

          console.log(
            `[BatchConsumer] Batch results: ${newMessages.length} NEW, ${duplicateMessages.length} DUPLICATES`
          );

          // STEP 3: Insert all new messages as IN_PROGRESS (atomically)
          if (newMessages.length > 0) {
            const insertResults =
              await batchIdempotencyService.insertNewMessagesInProgress(
                newMessages
              );
            console.log(
              `[BatchConsumer] Inserted ${insertResults.length} messages as IN_PROGRESS`
            );
          }

          // STEP 4: Process only NEW messages
          const processedResults = [];
          for (const msgData of parsedMessages) {
            // Check if this message was marked as new
            const isNew = newMessages.some(
              (nm) => nm.messageId === msgData.messageId
            );
            const isDuplicate = duplicateMessages.some(
              (dm) => dm.messageId === msgData.messageId
            );

            if (isDuplicate) {
              // Skip: This is a duplicate, don't process
              console.log(
                `[BatchConsumer] Skipping duplicate message ${msgData.messageId}`
              );
              processedResults.push({
                success: true,
                shouldRetry: false,
                isDuplicate: true,
                messageId: msgData.messageId,
              });
              continue;
            }

            // Process NEW message
            try {
              const provider = ProviderFactory.getProvider(
                channel as "email" | "sms" | "whatsapp"
              );

              const processorService = new DeliveryProcessorService(
                deliveryRepository,
                provider,
                deduplicationService,
                retryScheduler,
                eventPublisher
              );
              const controller = new DeliveryController(processorService);

              const result = await controller.handleMessage(msgData);
              processedResults.push(result);
            } catch (error: any) {
              console.error(
                `[BatchConsumer] Error processing message ${msgData.messageId}:`,
                error
              );
              processedResults.push({
                success: false,
                shouldRetry: true,
                error: error.message,
                messageId: msgData.messageId,
              });
            }
          }

          // STEP 5: Commit offset only if all new messages processed successfully
          const allSuccessful = processedResults.every(
            (r: any) => r.success || r.isDuplicate
          );
          if (allSuccessful) {
            // Commit the highest offset in this batch
            const lastMessage = messages[messages.length - 1];
            resolveOffset(lastMessage.offset);
            await heartbeat();
            console.log(
              `[BatchConsumer] Batch committed successfully (offset: ${lastMessage.offset})`
            );
          } else {
            console.log(
              `[BatchConsumer] Batch processing incomplete, not committing offset`
            );
          }
        } catch (error: any) {
          console.error(`[BatchConsumer] Batch processing error:`, error);
          // Don't commit offset on batch error - let Kafka retry
        }
      },
    });
  } catch (error) {
    console.error("Bootstrap error:", error);
    if (typeof process !== "undefined" && process.exit) {
      process.exit(1);
    }
  }
}

bootstrap();
