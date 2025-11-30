import { StructuredLog } from "../models/Log";
import { Client } from "@elastic/elasticsearch";

export interface ILogRepository {
  save(log: StructuredLog): Promise<void>;
}

export class ElasticsearchLogRepository implements ILogRepository {
  constructor(private client: Client) {}

  async save(log: StructuredLog): Promise<void> {
    const index = `logs-${new Date().toISOString().split("T")[0]}`;

    // Add event.dataset based on channel or service
    const dataset = log.channel || log.service || "notification";

    await this.client.index({
      index,
      document: {
        ...log,
        event: {
          dataset: dataset,
        },
        "@timestamp": new Date().toISOString(),
      },
    });
  }
}
