import { Pool } from "mysql2/promise";
import { DeliveryAttempt, DLQEntry } from "../../domain/entities/Delivery";
import {
  IDeliveryAttemptRepository,
  IDLQRepository,
  IMessageStatusRepository,
} from "../../domain/interfaces/index";

export class MySQLDeliveryRepository
  implements
    IDeliveryAttemptRepository,
    IDLQRepository,
    IMessageStatusRepository
{
  constructor(private pool: Pool) {}

  async save(entity: DeliveryAttempt | DLQEntry): Promise<void> {
    const connection = await this.pool.getConnection();
    try {
      if ("attemptNumber" in entity) {
        // DeliveryAttempt
        await connection.execute(
          `INSERT INTO delivery_attempts (messageId, attemptNumber, status, error, providerResponse)
           VALUES (?, ?, ?, ?, ?)`,
          [
            entity.messageId,
            entity.attemptNumber,
            entity.status,
            entity.error || null,
            entity.providerResponse || null,
          ]
        );
      } else {
        // DLQEntry
        await connection.execute(
          `INSERT INTO dlq_entries (messageId, channel, failureReason, maxAttemptsReached)
           VALUES (?, ?, ?, ?)`,
          [
            entity.messageId,
            entity.channel,
            entity.failureReason,
            entity.maxAttemptsReached,
          ]
        );
      }
    } finally {
      connection.release();
    }
  }

  async findDeliveryAttempts(messageId: string): Promise<DeliveryAttempt[]> {
    const connection = await this.pool.getConnection();
    try {
      const [rows] = await connection.execute(
        "SELECT * FROM delivery_attempts WHERE messageId = ? ORDER BY attemptAt ASC",
        [messageId]
      );
      return (rows as any[]) || [];
    } finally {
      connection.release();
    }
  }

  async findByMessageId(messageId: string): Promise<DLQEntry | null> {
    const connection = await this.pool.getConnection();
    try {
      const [rows] = await connection.execute(
        "SELECT * FROM dlq_entries WHERE messageId = ?",
        [messageId]
      );
      if (Array.isArray(rows) && rows.length > 0) {
        return rows[0] as DLQEntry;
      }
      return null;
    } finally {
      connection.release();
    }
  }

  async updateStatus(
    messageId: string,
    status: "PENDING" | "SENT" | "FAILED",
    finalDelivered: boolean,
    attempts: number
  ): Promise<void> {
    const connection = await this.pool.getConnection();
    try {
      await connection.execute(
        "UPDATE messages SET status = ?, finalDelivered = ?, attempts = ? WHERE messageId = ?",
        [status, finalDelivered, attempts, messageId]
      );
    } finally {
      connection.release();
    }
  }

  async getStatus(messageId: string): Promise<{
    finalDelivered: boolean;
    status: string;
    attempts: number;
  } | null> {
    const connection = await this.pool.getConnection();
    try {
      const [rows] = await connection.execute(
        "SELECT finalDelivered, status, attempts FROM messages WHERE messageId = ?",
        [messageId]
      );

      if (Array.isArray(rows) && rows.length > 0) {
        return rows[0] as any;
      }
      return null;
    } finally {
      connection.release();
    }
  }
}
