export interface IDeliveryDeduplicationService {
  checkDeliveryDedup(dedupKey: string): Promise<boolean>;
}

export class DeliveryDeduplicationService
  implements IDeliveryDeduplicationService
{
  constructor(private client: any) {}

  async checkDeliveryDedup(dedupKey: string): Promise<boolean> {
    try {
      const exists = await this.client.exists(`delivered:${dedupKey}`);
      if (exists === 1) {
        return false; // Already delivered
      }
      await this.client.set(`delivered:${dedupKey}`, "1");
      return true; // New delivery
    } catch {
      return false;
    }
  }
}
