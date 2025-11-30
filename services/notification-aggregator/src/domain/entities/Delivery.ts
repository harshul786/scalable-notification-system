// Domain Entity for Delivery Attempt
export interface DeliveryAttempt {
  id?: number;
  messageId: string;
  attemptNumber: number;
  status: "SUCCESS" | "FAILED";
  error?: string;
  providerResponse?: string;
  attemptAt?: Date;
}

// Domain Entity for DLQ Entry
export interface DLQEntry {
  id?: number;
  messageId: string;
  channel: "email" | "sms" | "whatsapp";
  failureReason: string;
  maxAttemptsReached: boolean;
  createdAt?: Date;
}

// Value object for message to be delivered
export interface MessageToDeliver {
  messageId: string;
  dedupKey: string;
  userId: string;
  tenantId: string;
  channel: "email" | "sms" | "whatsapp";
  recipient: string;
  body: string;
  traceId: string;
  attempt: number;
}
