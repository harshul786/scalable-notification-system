import { PersistLogUseCase } from "../../domain/usecases/PersistLogUseCase";
import { StructuredLog } from "../../domain/entities/Log";

export class LogController {
  constructor(private persistLogUseCase: PersistLogUseCase) {}

  async handleLog(log: StructuredLog): Promise<void> {
    await this.persistLogUseCase.execute(log);
  }
}
