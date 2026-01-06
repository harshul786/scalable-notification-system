/**
 * BatchIdempotencyService: Bulk DB idempotency checking for Kafka batch processing
 *
 * Implements DB-anchored idempotency:
 * - Receives batch of messages from Kafka
 * - Extracts idempotency keys
 * - Performs ONE bulk DB query to find already-processed messages
 * - Partitions batch into NEW (not in DB) and DUPLICATES (already processed)
 * - Skips duplicates completely, processes only new messages
 *
 * This ensures:
 * - DB is source of truth for idempotency
 * - Bulk queries are more efficient than per-message lookups
 * - Kafka replay is safe (duplicates are no-ops)
 */

import { Pool } from "mysql2/promise";

export interface MessageToCheckIdempotency {
  messageId: string;
  tenantId: string;
  idempotencyKey: string;
  dedupKey: string;
  channel: string;
  userId: string;
  recipient: string;
  body: string;
  traceId: string;
  attempt?: number;
}

export interface BatchIdempotencyResult {
  newMessages: MessageToCheckIdempotency[];
  duplicateMessages: Array<{
    messageId: string;
    idempotencyKey: string;
    status: string;
    reason: string;
  }>;
  totalProcessed: number;
}

export class BatchIdempotencyService {
  constructor(private pool: Pool) {}

  /**
   * Check batch of messages against DB for idempotency
   *
   * @param messages - Batch of messages from Kafka
   * @returns Split batch into new vs duplicate
   */
  async checkBatchIdempotency(
    messages: MessageToCheckIdempotency[]
  ): Promise<BatchIdempotencyResult> {
    if (messages.length === 0) {
      return {
        newMessages: [],
        duplicateMessages: [],
        totalProcessed: 0,
      };
    }

    const connection = await this.pool.getConnection();
    try {
      // STEP 1: Extract unique idempotency keys (by tenant + idempotencyKey)
      const uniqueKeys = this.extractUniqueIdempotencyKeys(messages);

      // STEP 2: Bulk DB query - check which messages already exist
      const existingMessages = await this.queryExistingMessages(
        connection,
        uniqueKeys
      );

      // Build lookup map for O(1) access
      const existingMap = new Map<string, any>();
      for (const existing of existingMessages) {
        const key = `${existing.tenantId}:${existing.idempotencyKey}`;
        existingMap.set(key, existing);
      }

      // STEP 3: Partition messages
      const newMessages: MessageToCheckIdempotency[] = [];
      const duplicateMessages: Array<{
        messageId: string;
        idempotencyKey: string;
        status: string;
        reason: string;
      }> = [];

      for (const msg of messages) {
        const key = `${msg.tenantId}:${msg.idempotencyKey}`;
        if (existingMap.has(key)) {
          // Duplicate: already in DB
          const existing = existingMap.get(key);
          duplicateMessages.push({
            messageId: msg.messageId,
            idempotencyKey: msg.idempotencyKey,
            status: existing.status,
            reason: `Duplicate idempotency key (existing: ${existing.messageId}, status: ${existing.status})`,
          });
        } else {
          // New: not in DB, process it
          newMessages.push(msg);
        }
      }

      return {
        newMessages,
        duplicateMessages,
        totalProcessed: messages.length,
      };
    } finally {
      connection.release();
    }
  }

  /**
   * Insert new messages with status = IN_PROGRESS
   * Called BEFORE any provider API calls (safe-first pattern)
   *
   * @param messages - New messages to insert
   * @returns Inserted message IDs with status
   */
  async insertNewMessagesInProgress(
    messages: MessageToCheckIdempotency[]
  ): Promise<Array<{ messageId: string; status: string }>> {
    if (messages.length === 0) {
      return [];
    }

    const connection = await this.pool.getConnection();
    try {
      // Start transaction for atomicity
      await connection.beginTransaction();

      const results: Array<{ messageId: string; status: string }> = [];

      // Insert each message with status = IN_PROGRESS
      for (const msg of messages) {
        const query = `
          INSERT INTO messages (
            messageId, 
            idempotencyKey,
            tenantId,
            dedupKey,
            userId,
            channel,
            recipient,
            body,
            status,
            finalDelivered,
            attemptCount,
            maxAttempts,
            createdAt,
            updatedAt
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'IN_PROGRESS', FALSE, 0, 4, NOW(), NOW())
        `;

        try {
          await connection.execute(query, [
            msg.messageId,
            msg.idempotencyKey,
            msg.tenantId,
            msg.dedupKey,
            msg.userId,
            msg.channel,
            msg.recipient,
            msg.body,
          ]);

          results.push({
            messageId: msg.messageId,
            status: "IN_PROGRESS",
          });
        } catch (error: any) {
          // Handle duplicate key error gracefully
          if (error.code === "ER_DUP_ENTRY") {
            console.log(
              `[BatchIdempotencyService] Duplicate key for message ${msg.messageId}, skipping insert`
            );
            results.push({
              messageId: msg.messageId,
              status: "SKIPPED_DUP",
            });
          } else {
            throw error;
          }
        }
      }

      // Commit transaction
      await connection.commit();

      return results;
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  }

  /**
   * Extract unique idempotency keys (tenant + key combination)
   */
  private extractUniqueIdempotencyKeys(
    messages: MessageToCheckIdempotency[]
  ): Array<{ tenantId: string; idempotencyKey: string }> {
    const seen = new Set<string>();
    const unique: Array<{ tenantId: string; idempotencyKey: string }> = [];

    for (const msg of messages) {
      const key = `${msg.tenantId}:${msg.idempotencyKey}`;
      if (!seen.has(key)) {
        seen.add(key);
        unique.push({
          tenantId: msg.tenantId,
          idempotencyKey: msg.idempotencyKey,
        });
      }
    }

    return unique;
  }

  /**
   * Query DB for existing messages by idempotency key
   */
  private async queryExistingMessages(
    connection: any,
    keys: Array<{ tenantId: string; idempotencyKey: string }>
  ): Promise<
    Array<{
      tenantId: string;
      idempotencyKey: string;
      messageId: string;
      status: string;
    }>
  > {
    if (keys.length === 0) {
      return [];
    }

    // Build WHERE clause for bulk query
    // Query: SELECT * FROM messages WHERE (tenantId, idempotencyKey) IN ((?, ?), (?, ?), ...)
    const placeholders = keys.map(() => "(?, ?)").join(", ");
    const flatParams = keys.flatMap((k) => [k.tenantId, k.idempotencyKey]);

    const query = `
      SELECT 
        tenantId,
        idempotencyKey,
        messageId,
        status,
        finalDelivered,
        attemptCount
      FROM messages
      WHERE (tenantId, idempotencyKey) IN (${placeholders})
    `;

    const [rows] = await connection.execute(query, flatParams);
    return rows as any[];
  }

  /**
   * Get metrics on batch processing
   */
  async getIdempotencyMetrics(
    tenantId: string
  ): Promise<{
    newCount: number;
    duplicateCount: number;
    failedCount: number;
  }> {
    const connection = await this.pool.getConnection();
    try {
      const query = `
        SELECT
          SUM(CASE WHEN status = 'PENDING' OR status = 'IN_PROGRESS' THEN 1 ELSE 0 END) as newCount,
          SUM(CASE WHEN status = 'SENT' THEN 1 ELSE 0 END) as duplicateCount,
          SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failedCount
        FROM messages
        WHERE tenantId = ?
      `;

      const [rows] = await connection.execute(query, [tenantId]);
      const result = (rows as any[])[0];

      return {
        newCount: result?.newCount || 0,
        duplicateCount: result?.duplicateCount || 0,
        failedCount: result?.failedCount || 0,
      };
    } finally {
      connection.release();
    }
  }
}
