export interface Message {
  messageId: string;
  dedupKey: string;
  userId: string;
  tenantId: string;
  channel: "email" | "sms" | "whatsapp";
  recipient: string;
  body: string;
  traceId: string;
  status: "PENDING" | "SENT" | "FAILED";
  finalDelivered: boolean;
  attempts: number;
  createdAt: Date;
  updatedAt: Date;
  idempotencyKey?: string; // Optional field for Kafka transport
}

export interface CreateMessageRequest {
  tenantId: string;
  userId: string;
  idempotencyKey: string;
  channel: "email" | "sms" | "whatsapp";
  recipient: string;
  body: string;
  metadata?: Record<string, any>;
}

export interface MessageResponse {
  messageId: string;
  dedupKey: string;
  traceId: string;
  status: "ACCEPTED" | "DUPLICATE";
}
