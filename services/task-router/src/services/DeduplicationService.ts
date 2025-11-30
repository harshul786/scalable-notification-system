export class DeduplicationService {
  private client: any;

  constructor(client: any) {
    this.client = client;
  }

  async checkDuplicate(dedupKey: string): Promise<boolean> {
    try {
      const key = `dedup:${dedupKey}`;

      const exists = await this.client.exists(key);
      if (exists) {
        return false;
      }

      await this.client.set(key, "1", {
        EX: 3600,
      });

      return true;
    } catch (error) {
      console.error("Deduplication service error:", error);
      return false;
    }
  }
}
