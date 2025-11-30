import { ProviderResult } from "../models/Delivery";

export interface IProvider {
  send(recipient: string, body: string): Promise<ProviderResult>;
}

export class EmailProvider implements IProvider {
  private successRate: number;
  private simulateFailure: boolean;

  constructor() {
    this.successRate = parseInt(process.env.EMAIL_SUCCESS_RATE || "80") / 100;
    this.simulateFailure = process.env.EMAIL_SIMULATE_FAILURE === "true";
  }

  async send(recipient: string, body: string): Promise<ProviderResult> {
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

export class SMSProvider implements IProvider {
  private successRate: number;
  private simulateFailure: boolean;

  constructor() {
    this.successRate = parseInt(process.env.SMS_SUCCESS_RATE || "85") / 100;
    this.simulateFailure = process.env.SMS_SIMULATE_FAILURE === "true";
  }

  async send(recipient: string, body: string): Promise<ProviderResult> {
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
          providerId: `sms-${Date.now()}-${Math.random()
            .toString(36)
            .substring(7)}`,
          timestamp: new Date().toISOString(),
          phone: recipient,
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

export class WhatsAppProvider implements IProvider {
  private successRate: number;
  private simulateFailure: boolean;

  constructor() {
    this.successRate =
      parseInt(process.env.WHATSAPP_SUCCESS_RATE || "90") / 100;
    this.simulateFailure = process.env.WHATSAPP_SIMULATE_FAILURE === "true";
  }

  async send(recipient: string, body: string): Promise<ProviderResult> {
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
          providerId: `wa-${Date.now()}-${Math.random()
            .toString(36)
            .substring(7)}`,
          timestamp: new Date().toISOString(),
          whatsappId: recipient,
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
