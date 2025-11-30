import { MessageToDeliver } from "../models/Delivery";
import { DeliveryProcessorService } from "../services/DeliveryProcessorService";

export class DeliveryController {
  constructor(private processorService: DeliveryProcessorService) {}

  async handleMessage(
    message: MessageToDeliver
  ): Promise<{ success: boolean; shouldRetry: boolean }> {
    return this.processorService.processDelivery(message);
  }
}
