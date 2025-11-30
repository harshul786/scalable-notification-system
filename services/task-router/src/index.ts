import express, { Request, Response } from "express";
import { createPool, Pool } from "mysql2/promise";
import * as redis from "redis";
import { Kafka } from "kafkajs";
import { HashService } from "./services/HashService";
import { DeduplicationService } from "./services/DeduplicationService";
import { MessageRepository } from "./repositories/MessageRepository";
import { MessagePublisher } from "./services/MessagePublisher";
import { MessageController } from "./controllers/MessageController";
import { createMessageRoutes } from "./routes/messageRoutes";

const app = express();
const PORT = process.env.PORT || 3001;

app.use(express.json());

app.get("/health", (req, res) => {
  res.json({ status: "ok", service: "task-router" });
});

async function bootstrap(): Promise<void> {
  try {
    const mysqlPool: Pool = await createPool({
      host: process.env.MYSQL_HOST || "localhost",
      user: process.env.MYSQL_USER || "notif_user",
      password: process.env.MYSQL_PASSWORD || "notif-password",
      database: process.env.MYSQL_DATABASE || "notification_db",
      waitForConnections: true,
      connectionLimit: 10,
    });

    const redisClient: any = redis.createClient({
      url: `redis://${process.env.REDIS_HOST || "localhost"}:${
        process.env.REDIS_PORT || 6379
      }`,
    });
    redisClient.on("error", (err: Error) => {
      console.error("Redis error:", err);
    });
    await redisClient.connect();

    const kafka = new Kafka({
      clientId: "task-router",
      brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
    });
    const producer = kafka.producer();
    await producer.connect();

    const hashService = new HashService();
    const deduplicationService = new DeduplicationService(redisClient);
    const messageRepository = new MessageRepository(mysqlPool);
    const messagePublisher = new MessagePublisher(producer);

    const messageController = new MessageController(
      messageRepository,
      messagePublisher,
      deduplicationService,
      hashService
    );

    app.use("/api", createMessageRoutes(messageController));

    app.listen(PORT, () => {
      console.log(`Task Router service running on port ${PORT}`);
    });
  } catch (error) {
    console.error("Bootstrap error:", error);
    process.exit(1);
  }
}

bootstrap();
