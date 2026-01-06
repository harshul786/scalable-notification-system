/**
 * DeliveryDeduplicationService: Delivery-side idempotency cache
 *
 * Redis cache to prevent duplicate provider calls on Kafka replay.
 * DB is the source of truth; this is an optimization to skip
 * provider calls on message replay.
 */
export interface IDeliveryDeduplicationService {
  checkDeliveryDedup(dedupKey: string): Promise<boolean>;
  setDeliveredCache(dedupKey: string): Promise<void>;
}

export class DeliveryDeduplicationService
  implements IDeliveryDeduplicationService
{
  private readonly DELIVERY_CACHE_TTL = 86400; // 24 hours

  constructor(private client: any) {}

  /**
   * Check if message was already delivered (soft cache)
   *
   * @returns true if NEW (not yet delivered), false if CACHED
   */
  async checkDeliveryDedup(dedupKey: string): Promise<boolean> {
    try {
      const cacheKey = this.buildDeliveryKey(dedupKey);
      const exists = await this.client.exists(cacheKey);

      if (exists === 1) {
        // Hit: Already marked as delivered
        return false;
      }

      // Miss: Set optimistically (may race, but safe due to DB)
      await this.client.setNX(cacheKey, "1", {
        EX: this.DELIVERY_CACHE_TTL,
      });

      return true; // New delivery
    } catch (error) {
      console.error(`DeliveryDeduplicationService: Redis error:`, error);
      // Safe fail: Return true, let DB prevent duplicates
      return true;
    }
  }

  /**
   * Explicitly warm delivery cache after DB commit succeeds
   * Called AFTER status=SENT is persisted to DB
   *
   * @param dedupKey - Message dedup key
   */
  async setDeliveredCache(dedupKey: string): Promise<void> {
    try {
      const cacheKey = this.buildDeliveryKey(dedupKey);
      await this.client.setEx(cacheKey, this.DELIVERY_CACHE_TTL, "1");
    } catch (error) {
      console.error(
        `DeliveryDeduplicationService: Failed to warm cache for ${dedupKey}:`,
        error
      );
      // Non-critical: Don't fail delivery for cache write
    }
  }

  private buildDeliveryKey(dedupKey: string): string {
    return `delivered:${dedupKey}`;
  }
}
