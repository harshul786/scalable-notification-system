import { StructuredLog } from "../models/Log";
import { LogPersistenceService } from "../services/LogPersistenceService";

export class LogController {
  constructor(private persistenceService: LogPersistenceService) {}

  async handleLog(log: StructuredLog): Promise<void> {
    await this.persistenceService.persistLog(log);
  }
}
