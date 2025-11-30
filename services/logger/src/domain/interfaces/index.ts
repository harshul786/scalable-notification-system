import { StructuredLog } from "../entities/Log";

// Repository interface
export interface ILogRepository {
  save(log: StructuredLog): Promise<void>;
}

// Log consumer interface
export interface ILogConsumer {
  consume(handler: (log: StructuredLog) => Promise<void>): Promise<void>;
}
