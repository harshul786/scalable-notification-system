import { createPool, Pool } from "mysql2/promise";
import { Message } from "../../domain/entities/Message";
import { IMessageRepository } from "../../domain/interfaces/index";

export class MySQLMessageRepository implements IMessageRepository {
  private pool: Pool;

  constructor(pool: Pool) {
    this.pool = pool;
  }

  async save(message: Message): Promise<void> {
    const connection = await this.pool.getConnection();
    try {
      const query = `
        INSERT INTO messages 
        (messageId, dedupKey, userId, tenantId, channel, recipient, body, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING')
      `;

      await connection.execute(query, [
        message.messageId,
        message.dedupKey,
        message.userId,
        message.tenantId,
        message.channel,
        message.recipient,
        message.body,
      ]);
    } finally {
      connection.release();
    }
  }

  async findById(messageId: string): Promise<Message | null> {
    const connection = await this.pool.getConnection();
    try {
      const [rows] = await connection.execute(
        "SELECT * FROM messages WHERE messageId = ?",
        [messageId]
      );

      if (Array.isArray(rows) && rows.length > 0) {
        return rows[0] as Message;
      }
      return null;
    } finally {
      connection.release();
    }
  }
}
