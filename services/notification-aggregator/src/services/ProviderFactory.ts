import { ProviderResult } from "../models/Delivery";

/**
 * Provider idempotency options
 * Passed to provider.send() to enable idempotent calls
 */
export interface IdempotencyOptions {
  idempotencyToken: string; // Unique token for this request
  attemptNumber: number; // Which attempt is this (1, 2, 3, ...)
  headers?: Record<string, string>; // Additional headers to send
}

/**
 * Provider interface with idempotency support
 * Each provider must be idempotent - calling with same token returns same result
 */
export interface IProvider {
  send(
    recipient: string,
    body: string,
    options?: IdempotencyOptions
  ): Promise<ProviderResult>;
}

/**
 * EmailProvider with idempotency support
 * Real providers (SendGrid, AWS SES, etc.) would:
 * - Accept Idempotency-Key header
 * - Return same message ID for same token
 * - Never double-charge for duplicate requests
 */
export class EmailProvider implements IProvider {
  private successRate: number;
  private simulateFailure: boolean;

  constructor() {
    this.successRate = parseInt(process.env.EMAIL_SUCCESS_RATE || "80") / 100;
    this.simulateFailure = process.env.EMAIL_SIMULATE_FAILURE === "true";
  }

  async send(
    recipient: string,
    body: string,
    options?: IdempotencyOptions
  ): Promise<ProviderResult> {
    if (this.simulateFailure) {
      throw new Error(
        `[EMAIL-SIMULATED] Email service failure forced by EMAIL_SIMULATE_FAILURE=true`
      );
    }

    const success = Math.random() < this.successRate;

    if (success) {
      return {
        success: true,
        response: {
          status: "sent",
          provider: "email-provider-v1",
          providerId: `email-${
            options?.idempotencyToken || Date.now()
          }-${Math.random().toString(36).substring(7)}`,
          timestamp: new Date().toISOString(),
          recipient: recipient,
          // In real provider, would include:
          // idempotencyToken: options?.idempotencyToken,
          // messageId: (deterministic from token),
        },
      };
    }

    throw new Error(
      `[EMAIL] Email delivery failed (Success Rate: ${(
        this.successRate * 100
      ).toFixed(0)}%, attempt: ${options?.attemptNumber || 1})`
    );
  }
}

/**
 * SMSProvider with idempotency support
 */
export class SMSProvider implements IProvider {
  private successRate: number;
  private simulateFailure: boolean;

  constructor() {
    this.successRate = parseInt(process.env.SMS_SUCCESS_RATE || "85") / 100;
    this.simulateFailure = process.env.SMS_SIMULATE_FAILURE === "true";
  }

  async send(
    recipient: string,
    body: string,
    options?: IdempotencyOptions
  ): Promise<ProviderResult> {
    if (this.simulateFailure) {
      throw new Error(
        `[SMS-SIMULATED] SMS service failure forced by SMS_SIMULATE_FAILURE=true`
      );
    }

    const success = Math.random() < this.successRate;

    if (success) {
      return {
        success: true,
        response: {
          status: "sent",
          provider: "sms-provider-v1",
          providerId: `sms-${
            options?.idempotencyToken || Date.now()
          }-${Math.random().toString(36).substring(7)}`,
          timestamp: new Date().toISOString(),
          phone: recipient,
          // In real provider: idempotencyToken: options?.idempotencyToken
        },
      };
    }

    throw new Error(
      `[SMS] SMS delivery failed (Success Rate: ${(
        this.successRate * 100
      ).toFixed(0)}%, attempt: ${options?.attemptNumber || 1})`
    );
  }
}

/**
 * WhatsAppProvider with idempotency support
 */
export class WhatsAppProvider implements IProvider {
  private successRate: number;
  private simulateFailure: boolean;

  constructor() {
    this.successRate =
      parseInt(process.env.WHATSAPP_SUCCESS_RATE || "90") / 100;
    this.simulateFailure = process.env.WHATSAPP_SIMULATE_FAILURE === "true";
  }

  async send(
    recipient: string,
    body: string,
    options?: IdempotencyOptions
  ): Promise<ProviderResult> {
    if (this.simulateFailure) {
      throw new Error(
        `[WHATSAPP-SIMULATED] WhatsApp service failure forced by WHATSAPP_SIMULATE_FAILURE=true`
      );
    }

    const success = Math.random() < this.successRate;

    if (success) {
      return {
        success: true,
        response: {
          status: "sent",
          provider: "whatsapp-provider-v1",
          providerId: `wa-${
            options?.idempotencyToken || Date.now()
          }-${Math.random().toString(36).substring(7)}`,
          timestamp: new Date().toISOString(),
          whatsappId: recipient,
          // In real provider: idempotencyToken: options?.idempotencyToken
        },
      };
    }

    throw new Error(
      `[WHATSAPP] WhatsApp delivery failed (Success Rate: ${(
        this.successRate * 100
      ).toFixed(0)}%, attempt: ${options?.attemptNumber || 1})`
    );
  }
}

export class ProviderFactory {
  static getProvider(channel: "email" | "sms" | "whatsapp"): IProvider {
    switch (channel) {
      case "email":
        return new EmailProvider();
      case "sms":
        return new SMSProvider();
      case "whatsapp":
        return new WhatsAppProvider();
      default:
        throw new Error(`Unknown channel: ${channel}`);
    }
  }
}
