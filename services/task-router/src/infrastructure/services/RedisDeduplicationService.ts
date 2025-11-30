import { IDeduplicationService } from "../../domain/interfaces/index";

export class RedisDeduplicationService implements IDeduplicationService {
  private client: any;

  constructor(client: any) {
    this.client = client;
  }

  async checkDuplicate(dedupKey: string): Promise<boolean> {
    try {
      const key = `dedup:${dedupKey}`;

      // Check if key already exists first
      const exists = await this.client.exists(key);
      if (exists) {
        return false; // Key already exists - this is a duplicate
      }

      // Key doesn't exist, set it with expiration
      await this.client.set(key, "1", {
        EX: 3600, // 1 hour expiration
      });

      return true; // Key was newly set - this is a new message
    } catch (error) {
      console.error("Redis dedup error:", error);
      return false; // Fail safe: treat as duplicate on error
    }
  }
}
