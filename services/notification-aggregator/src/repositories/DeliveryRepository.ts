import { Pool } from "mysql2/promise";
import { DeliveryAttempt, DLQEntry, MessageStatus } from "../models/Delivery";

export interface IDeliveryRepository {
  saveAttempt(entity: DeliveryAttempt): Promise<void>;
  saveDLQEntry(entity: DLQEntry): Promise<void>;
  findDeliveryAttempts(messageId: string): Promise<DeliveryAttempt[]>;
  findDLQEntry(messageId: string): Promise<DLQEntry | null>;
  updateStatus(
    messageId: string,
    status: "PENDING" | "SENT" | "FAILED",
    finalDelivered: boolean,
    attempts: number
  ): Promise<void>;
  getStatus(messageId: string): Promise<MessageStatus | null>;
}

export class DeliveryRepository implements IDeliveryRepository {
  constructor(private pool: Pool) {}

  async saveAttempt(entity: DeliveryAttempt): Promise<void> {
    const connection = await this.pool.getConnection();
    try {
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
    } finally {
      connection.release();
    }
  }

  async saveDLQEntry(entity: DLQEntry): Promise<void> {
    const connection = await this.pool.getConnection();
    try {
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

  async findDLQEntry(messageId: string): Promise<DLQEntry | null> {
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

  async getStatus(messageId: string): Promise<MessageStatus | null> {
    const connection = await this.pool.getConnection();
    try {
      const [rows] = await connection.execute(
        "SELECT finalDelivered, status, attempts FROM messages WHERE messageId = ?",
        [messageId]
      );

      if (Array.isArray(rows) && rows.length > 0) {
        return rows[0] as MessageStatus;
      }
      return null;
    } finally {
      connection.release();
    }
  }
}
