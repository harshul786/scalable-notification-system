import { IProvider } from "../../domain/interfaces/index";

/**
 * Email Provider - Dummy Implementation with Configurable Success Rate
 *
 * Environment Variables:
 * - EMAIL_SUCCESS_RATE: Percentage of successful sends (0-100, default: 80)
 * - EMAIL_SIMULATE_FAILURE: Force all emails to fail (true/false, default: false)
 *
 * Example:
 *   EMAIL_SUCCESS_RATE=50 docker compose up  # 50% success rate
 *   EMAIL_SIMULATE_FAILURE=true docker compose up  # All fail
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
    body: string
  ): Promise<{ success: boolean; response: any }> {
    // Force failure if configured
    if (this.simulateFailure) {
      throw new Error(
        `[EMAIL-SIMULATED] Email service failure forced by EMAIL_SIMULATE_FAILURE=true`
      );
    }

    // Simulate configurable success rate
    const success = Math.random() < this.successRate;

    if (success) {
      return {
        success: true,
        response: {
          status: "sent",
          provider: "email-provider-v1",
          providerId: `email-${Date.now()}-${Math.random()
            .toString(36)
            .substring(7)}`,
          timestamp: new Date().toISOString(),
          recipient: recipient,
        },
      };
    }

    throw new Error(
      `[EMAIL] Email delivery failed (Success Rate: ${(
        this.successRate * 100
      ).toFixed(0)}%)`
    );
  }
}

/**
 * SMS Provider - Dummy Implementation with Configurable Success Rate
 *
 * Environment Variables:
 * - SMS_SUCCESS_RATE: Percentage of successful sends (0-100, default: 85)
 * - SMS_SIMULATE_FAILURE: Force all SMS to fail (true/false, default: false)
 *
 * Example:
 *   SMS_SUCCESS_RATE=70 docker compose up  # 70% success rate
 *   SMS_SIMULATE_FAILURE=true docker compose up  # All fail
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
    body: string
  ): Promise<{ success: boolean; response: any }> {
    // Force failure if configured
    if (this.simulateFailure) {
      throw new Error(
        `[SMS-SIMULATED] SMS service failure forced by SMS_SIMULATE_FAILURE=true`
      );
    }

    // Simulate configurable success rate
    const success = Math.random() < this.successRate;

    if (success) {
      return {
        success: true,
        response: {
          status: "queued",
          provider: "sms-provider-v1",
          providerId: `sms-${Date.now()}-${Math.random()
            .toString(36)
            .substring(7)}`,
          timestamp: new Date().toISOString(),
          recipient: recipient,
          credits: Math.ceil(body.length / 160),
        },
      };
    }

    throw new Error(
      `[SMS] SMS delivery failed (Success Rate: ${(
        this.successRate * 100
      ).toFixed(0)}%)`
    );
  }
}

/**
 * WhatsApp Provider - Dummy Implementation with Configurable Success Rate
 *
 * Environment Variables:
 * - WHATSAPP_SUCCESS_RATE: Percentage of successful sends (0-100, default: 90)
 * - WHATSAPP_SIMULATE_FAILURE: Force all WhatsApp to fail (true/false, default: false)
 *
 * Example:
 *   WHATSAPP_SUCCESS_RATE=60 docker compose up  # 60% success rate
 *   WHATSAPP_SIMULATE_FAILURE=true docker compose up  # All fail
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
    body: string
  ): Promise<{ success: boolean; response: any }> {
    // Force failure if configured
    if (this.simulateFailure) {
      throw new Error(
        `[WHATSAPP-SIMULATED] WhatsApp service failure forced by WHATSAPP_SIMULATE_FAILURE=true`
      );
    }

    // Simulate configurable success rate
    const success = Math.random() < this.successRate;

    if (success) {
      return {
        success: true,
        response: {
          status: "sent",
          provider: "whatsapp-provider-v1",
          providerId: `whatsapp-${Date.now()}-${Math.random()
            .toString(36)
            .substring(7)}`,
          timestamp: new Date().toISOString(),
          recipient: recipient,
          messageType: "text",
        },
      };
    }

    throw new Error(
      `[WHATSAPP] WhatsApp delivery failed (Success Rate: ${(
        this.successRate * 100
      ).toFixed(0)}%)`
    );
  }
}
