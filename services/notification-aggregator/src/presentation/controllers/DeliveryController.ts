import { ProcessDeliveryUseCase } from "../../domain/usecases/ProcessDeliveryUseCase";
import { MessageToDeliver } from "../../domain/entities/Delivery";

export class DeliveryController {
  constructor(private processDeliveryUseCase: ProcessDeliveryUseCase) {}

  async handleMessage(
    message: MessageToDeliver
  ): Promise<{ success: boolean; shouldRetry: boolean }> {
    return this.processDeliveryUseCase.execute(message);
  }
}
