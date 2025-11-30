import { StructuredLog } from "../models/Log";
import { ILogRepository } from "../repositories/LogRepository";

export class LogPersistenceService {
  constructor(private logRepository: ILogRepository) {}

  async persistLog(log: StructuredLog): Promise<void> {
    await this.logRepository.save(log);
  }
}
