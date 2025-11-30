import * as dotenv from "dotenv";
dotenv.config();

import { createPool, Pool } from "mysql2/promise";
import * as redis from "redis";
import { Kafka } from "kafkajs";
import { DeliveryRepository } from "./repositories/DeliveryRepository";
import { DeliveryDeduplicationService } from "./services/DeliveryDeduplicationService";
import { RetrySchedulerService } from "./services/RetrySchedulerService";
import { EventPublisherService } from "./services/EventPublisherService";
import {
  EmailProvider,
  SMSProvider,
  WhatsAppProvider,
  ProviderFactory,
} from "./services/ProviderFactory";
import { DeliveryProcessorService } from "./services/DeliveryProcessorService";
import { DeliveryController } from "./controllers/DeliveryController";
import { MessageToDeliver } from "./models/Delivery";

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

    // Start consuming messages
    await consumer.run({
      eachBatch: async ({ batch, resolveOffset, heartbeat }: any) => {
        const { topic, messages } = batch;
        const channel = topic.replace("messages.", "");

        for (const message of messages) {
          try {
            const msgData = JSON.parse(
              message.value?.toString() || "{}"
            ) as MessageToDeliver;

            // Ensure attempt is a number
            if (typeof msgData.attempt === "string") {
              msgData.attempt = parseInt(msgData.attempt, 10);
            }
            if (!msgData.attempt) {
              msgData.attempt = 0;
            }

            // Get provider for this channel
            const provider = ProviderFactory.getProvider(
              channel as "email" | "sms" | "whatsapp"
            );

            // Create processor and controller
            const processorService = new DeliveryProcessorService(
              deliveryRepository,
              provider,
              deduplicationService,
              retryScheduler,
              eventPublisher
            );
            const controller = new DeliveryController(processorService);

            const result = await controller.handleMessage(msgData);

            // Only commit if successful and no retry
            if (result.success && !result.shouldRetry) {
              resolveOffset(message.offset);
              await heartbeat();
            }
          } catch (error) {
            // Silently handle message processing errors
          }
        }
      },
    });
  } catch (error) {
    if (typeof process !== "undefined" && process.exit) {
      process.exit(1);
    }
  }
}

bootstrap();
