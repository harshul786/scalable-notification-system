import { StructuredLog } from "../../domain/entities/Log";
import { ILogRepository } from "../../domain/interfaces/index";
import { Client } from "@elastic/elasticsearch";

export class ElasticsearchLogRepository implements ILogRepository {
  constructor(private client: Client) {}

  async save(log: StructuredLog): Promise<void> {
    const index = `logs-${new Date().toISOString().split("T")[0]}`;

    await this.client.index({
      index,
      document: {
        ...log,
        "@timestamp": new Date().toISOString(),
      },
    });
  }
}
