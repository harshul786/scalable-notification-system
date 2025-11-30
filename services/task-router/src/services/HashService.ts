import crypto from "crypto";

export class HashService {
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
