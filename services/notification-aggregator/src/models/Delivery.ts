// Delivery domain models and interfaces

export interface DeliveryAttempt {
  id?: number;
  messageId: string;
  attemptNumber: number;
  status: "SUCCESS" | "FAILED";
  error?: string;
  providerResponse?: string;
  attemptAt?: Date;
}

export interface DLQEntry {
  id?: number;
  messageId: string;
  channel: "email" | "sms" | "whatsapp";
  failureReason: string;
  maxAttemptsReached: boolean;
  createdAt?: Date;
}

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

export interface MessageStatus {
  finalDelivered: boolean;
  status: string;
  attempts: number;
}

export interface ProviderResult {
  success: boolean;
  response: any;
}
