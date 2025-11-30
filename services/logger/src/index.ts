import * as dotenv from "dotenv";
dotenv.config();

import { Client } from "@elastic/elasticsearch";
import { Kafka } from "kafkajs";
import { ElasticsearchLogRepository } from "./repositories/LogRepository";
import { LogConsumerService } from "./services/LogConsumerService";
import { LogPersistenceService } from "./services/LogPersistenceService";
import { LogController } from "./controllers/LogController";

async function bootstrap(): Promise<void> {
  try {
    console.log("Initializing Logger Service...");

    // Initialize Elasticsearch
    const esClient = new Client({
      node: `http://${process.env.ELASTICSEARCH_HOST || "localhost"}:${
        process.env.ELASTICSEARCH_PORT || 9200
      }`,
    });
    await esClient.info();
    console.log("[OK] Elasticsearch connected");

    // Initialize Kafka
    const kafka = new Kafka({
      clientId: "logger-service",
      brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
    });
    console.log("[OK] Kafka initialized");

    // Initialize repositories and services
    const logRepository = new ElasticsearchLogRepository(esClient);
    const logConsumer = new LogConsumerService(kafka);
    const persistenceService = new LogPersistenceService(logRepository);
    const logController = new LogController(persistenceService);

    // Start consuming logs from both topics
    console.log("Starting log consumer...");
    await logConsumer.consumeMultiple(["logs", "errors"], async (log) => {
      await logController.handleLog(log);
      const level = log.level || "INFO";
      console.log(`[${log.service}] ${level} - ${log.status || log.message}`);
    });

    console.log("[OK] Logger service running");
  } catch (error) {
    console.error("Failed to bootstrap:", error);
    process.exit(1);
  }
}

bootstrap();
