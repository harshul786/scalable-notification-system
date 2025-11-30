import { ILogRepository } from "../../domain/interfaces/index";
import { StructuredLog } from "../../domain/entities/Log";

// Use case: Persist log to Elasticsearch
export class PersistLogUseCase {
  constructor(private logRepository: ILogRepository) {}

  async execute(log: StructuredLog): Promise<void> {
    await this.logRepository.save(log);
  }
}
