#!/usr/bin/env node

/**
 * Test script - sends sample messages to test the system
 */

const http = require("http");

const testMessages = [
  {
    tenantId: "tenant-1",
    userId: "user-123",
    idempotencyKey: "req-email-001",
    channel: "email",
    recipient: "john@example.com",
    body: "Welcome to our service! This is your first email notification.",
    metadata: { priority: "high", template: "welcome" },
  },
  {
    tenantId: "tenant-1",
    userId: "user-456",
    idempotencyKey: "req-sms-001",
    channel: "sms",
    recipient: "+1-234-567-8900",
    body: "Your verification code is 123456. Valid for 10 minutes.",
    metadata: { priority: "high" },
  },
  {
    tenantId: "tenant-1",
    userId: "user-789",
    idempotencyKey: "req-whatsapp-001",
    channel: "whatsapp",
    recipient: "+1-555-123-4567",
    body: "Hi! Your order #12345 has been confirmed. Track it here: http://example.com/orders/12345",
    metadata: { priority: "medium" },
  },
];

function sendMessage(message) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: "localhost",
      port: 3001,
      path: "/api/messages",
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
    };

    const req = http.request(options, (res) => {
      let data = "";
      res.on("data", (chunk) => {
        data += chunk;
      });
      res.on("end", () => {
        try {
          const response = JSON.parse(data);
          resolve({ status: res.statusCode, data: response });
        } catch {
          resolve({ status: res.statusCode, data });
        }
      });
    });

    req.on("error", reject);
    req.write(JSON.stringify(message));
    req.end();
  });
}

async function runTests() {
  console.log("🚀 Sending test messages...\n");

  for (const msg of testMessages) {
    try {
      const result = await sendMessage(msg);
      console.log(`✓ ${msg.channel.toUpperCase()}: ${result.data.status}`);
      console.log(`  messageId: ${result.data.messageId}`);
      console.log(`  traceId: ${result.data.traceId}`);
      console.log();
    } catch (error) {
      console.error(`✗ Error sending ${msg.channel}: ${error.message}`);
    }
  }

  console.log("✓ All test messages sent!");
  console.log("\n📊 Check status with:");
  console.log(
    '   docker-compose exec mysql mysql -u notif_user -pnotif-password -D notification_db -e "SELECT * FROM messages;"'
  );
  console.log("\n📋 View logs in Kibana: http://localhost:5601");
  console.log(
    "\n📦 Check retry queue: docker-compose exec redis redis-cli ZRANGE retries 0 -1 WITHSCORES"
  );
}

runTests().catch(console.error);
