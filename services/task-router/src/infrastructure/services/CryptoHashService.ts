import crypto from "crypto";
import { IHashService } from "../../domain/interfaces/index";

export class CryptoHashService implements IHashService {
  generateDedupKey(
    body: string,
    userId: string,
    recipient: string,
    tenantId: string
  ): string {
    const normalized = `${body}:${userId}:${recipient}:${tenantId}`;
    return crypto.createHash("sha256").update(normalized).digest("hex");
  }
}
