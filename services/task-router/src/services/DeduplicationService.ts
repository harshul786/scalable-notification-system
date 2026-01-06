/**
 * DeduplicationService: Soft ingress cache layer (Redis only)
 *
 * IMPORTANT: Redis is NOT the source of truth for idempotency.
 * DB (MySQL) is the authoritative source.
 *
 * This service provides:
 * 1. Fast cache checks to prevent re-enqueuing duplicate requests
 * 2. Anti-spam protection with temporary TTL (24-72h)
 * 3. Quick response on duplicate detection without DB hits
 *
 * Key format: idem:tenant:{tenantId}:key:{idempotencyKey}
 *
 * Behavior:
 * - If Redis hit: Return false (cached, skip re-enqueue)
 * - If Redis miss: Return true (new/expired, allow processing)
 * - On Redis failure: Return true (safe-fail, let DB handle dedup)
 */
export class DeduplicationService {
  private client: any;
  private readonly INGRESS_TTL_SECONDS = 259200; // 72 hours

  constructor(client: any) {
    this.client = client;
  }

  /**
   * Check if request was recently seen (soft cache check)
   *
   * @param tenantId - Tenant identifier
   * @param idempotencyKey - Request's idempotency key
   * @returns true if NEW (allow processing), false if CACHED (skip)
   */
  async checkAndCacheIngressRequest(
    tenantId: string,
    idempotencyKey: string
  ): Promise<boolean> {
    try {
      const cacheKey = this.buildIngressCacheKey(tenantId, idempotencyKey);

      // Check if cached
      const cached = await this.client.exists(cacheKey);
      if (cached === 1) {
        // Hit: Request was recently seen, skip re-enqueue
        return false;
      }

      // Miss: Set cache with TTL (72h safety window for retries)
      await this.client.setNX(cacheKey, "1", {
        EX: this.INGRESS_TTL_SECONDS,
      });

      // Return true: proceed with processing
      return true;
    } catch (error) {
      console.error(
        `DeduplicationService: Redis error for ${tenantId}/${idempotencyKey}:`,
        error
      );
      // Safe fail: Return true, let DB enforce idempotency
      return true;
    }
  }

  /**
   * Warm Redis cache after DB commit succeeds
   * Called ONLY after database persistence is confirmed
   *
   * @param tenantId - Tenant identifier
   * @param idempotencyKey - Request's idempotency key
   * @param cachedData - Message state to cache
   */
  async warmIngressCache(
    tenantId: string,
    idempotencyKey: string,
    cachedData: {
      messageId: string;
      status: string;
      channel: string;
      createdAt: string;
    }
  ): Promise<void> {
    try {
      const cacheKey = this.buildIngressCacheKey(tenantId, idempotencyKey);
      const value = JSON.stringify(cachedData);

      await this.client.setEx(cacheKey, this.INGRESS_TTL_SECONDS, value);
    } catch (error) {
      console.error(
        `DeduplicationService: Failed to warm cache for ${tenantId}/${idempotencyKey}:`,
        error
      );
      // Non-critical: logging only, don't fail request
    }
  }

  /**
   * Retrieve cached message state if available
   *
   * @param tenantId - Tenant identifier
   * @param idempotencyKey - Request's idempotency key
   * @returns Cached message data or null if not cached
   */
  async getCachedIngressRequest(
    tenantId: string,
    idempotencyKey: string
  ): Promise<{
    messageId: string;
    status: string;
    channel: string;
    createdAt: string;
  } | null> {
    try {
      const cacheKey = this.buildIngressCacheKey(tenantId, idempotencyKey);
      const cached = await this.client.get(cacheKey);

      if (!cached) {
        return null;
      }

      return JSON.parse(cached);
    } catch (error) {
      console.error(
        `DeduplicationService: Failed to retrieve cache for ${tenantId}/${idempotencyKey}:`,
        error
      );
      return null;
    }
  }

  /**
   * Build standardized cache key format
   * Format: idem:tenant:{tenantId}:key:{idempotencyKey}
   */
  private buildIngressCacheKey(
    tenantId: string,
    idempotencyKey: string
  ): string {
    return `idem:tenant:${tenantId}:key:${idempotencyKey}`;
  }

  /**
   * Health check: Can we reach Redis?
   */
  async isHealthy(): Promise<boolean> {
    try {
      await this.client.ping();
      return true;
    } catch (error) {
      console.error("DeduplicationService: Redis unreachable:", error);
      return false;
    }
  }
}
