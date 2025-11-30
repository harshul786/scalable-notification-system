// Domain entity for structured log
export interface StructuredLog {
  service: "router" | "aggregator" | "scheduler" | "logger";
  level: "INFO" | "ERROR" | "WARN";
  message: string;
  traceId: string | null;
  spanId: string;
  parentSpanId: string | null;
  messageId: string | null;
  userId: string | null;
  dedupKey: string | null;
  channel: "email" | "sms" | "whatsapp" | null;
  status: string | null;
  attempt: number | null;
  error: string | null;
  timestamp: string;
}
