#!/usr/bin/env node

/**
 * Comprehensive Integration Test Suite
 * Tests all aspects of the Notification Aggregator System
 *
 * Usage: npm run test:integration
 * Or: node tests/integration-tests.js
 */

const http = require("http");
const assert = require("assert");

// Configuration
const API_HOST = process.env.API_HOST || "localhost";
const API_PORT = process.env.API_PORT || 3001;
const MYSQL_HOST = process.env.MYSQL_HOST || "127.0.0.1";
const MYSQL_PORT = process.env.MYSQL_PORT || 3307;
const MYSQL_USER = process.env.MYSQL_USER || "root";
const MYSQL_PASSWORD = process.env.MYSQL_PASSWORD || "harshul12345";
const MYSQL_DB = process.env.MYSQL_DATABASE || "notification_db";

// Test state
let testResults = {
  total: 0,
  passed: 0,
  failed: 0,
  errors: [],
};

// ==================== HTTP Helper ====================

function makeRequest(method, path, body) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: API_HOST,
      port: API_PORT,
      path: path,
      method: method,
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
          const parsed = data ? JSON.parse(data) : {};
          resolve({
            status: res.statusCode,
            headers: res.headers,
            body: parsed,
            raw: data,
          });
        } catch (e) {
          resolve({
            status: res.statusCode,
            headers: res.headers,
            body: data,
            raw: data,
          });
        }
      });
    });

    req.on("error", reject);

    if (body) {
      req.write(JSON.stringify(body));
    }
    req.end();
  });
}

// ==================== Test Runners ====================

async function runTest(testName, testFn) {
  testResults.total++;
  process.stdout.write(`  [waiting] ${testName}... `);

  try {
    await testFn();
    testResults.passed++;
    console.log("[PASS]");
    return true;
  } catch (error) {
    testResults.failed++;
    console.log("[FAIL]");
    testResults.errors.push({
      test: testName,
      error: error.message,
      stack: error.stack,
    });
    return false;
  }
}

function assertEquals(actual, expected, message) {
  if (actual !== expected) {
    throw new Error(`${message}: expected ${expected}, got ${actual}`);
  }
}

function assertExists(value, message) {
  if (!value) {
    throw new Error(`${message}: value does not exist`);
  }
}

function assertFieldsExist(obj, fields, message) {
  for (const field of fields) {
    if (!(field in obj)) {
      throw new Error(`${message}: missing field '${field}'`);
    }
  }
}

// ==================== Test Suites ====================

async function testEmailChannel() {
  console.log("\n[EMAIL CHANNEL TESTS]");

  await runTest("Send email message", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-email-" + Date.now(),
      userId: "u-email-" + Date.now(),
      idempotencyKey: "email-" + Date.now(),
      channel: "email",
      recipient: "test@example.com",
      body: "Test email message",
      metadata: { priority: "high" },
    });

    assertEquals(response.status, 202, "Status code");
    assertEquals(response.body.status, "ACCEPTED", "Response status");
    assertFieldsExist(
      response.body,
      ["messageId", "traceId", "status"],
      "Response fields"
    );
  });

  await runTest("Email with special characters in body", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-email-" + Date.now(),
      userId: "u-email-" + Date.now(),
      idempotencyKey: "email-" + Date.now(),
      channel: "email",
      recipient: "test@example.com",
      body: 'Test with special chars: !@#$%^&*()_+-=[]{}|;:",.<>?/`~',
      metadata: { priority: "normal" },
    });

    assertEquals(response.status, 202, "Status code");
    assertEquals(response.body.status, "ACCEPTED", "Response status");
  });

  await runTest("Email with long body (5000+ chars)", async () => {
    const longBody = "A".repeat(5000);
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-email-" + Date.now(),
      userId: "u-email-" + Date.now(),
      idempotencyKey: "email-" + Date.now(),
      channel: "email",
      recipient: "test@example.com",
      body: longBody,
      metadata: { priority: "low" },
    });

    assertEquals(response.status, 202, "Status code");
    assertEquals(response.body.status, "ACCEPTED", "Response status");
  });

  await runTest("Email with metadata object", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-email-" + Date.now(),
      userId: "u-email-" + Date.now(),
      idempotencyKey: "email-" + Date.now(),
      channel: "email",
      recipient: "test@example.com",
      body: "Test email with metadata",
      metadata: {
        priority: "high",
        tags: ["test", "integration"],
        retryCount: 3,
      },
    });

    assertEquals(response.status, 202, "Status code");
    assertEquals(response.body.status, "ACCEPTED", "Response status");
  });
}

async function testSmsChannel() {
  console.log("\n[SMS CHANNEL TESTS]");

  await runTest("Send SMS message", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-sms-" + Date.now(),
      userId: "u-sms-" + Date.now(),
      idempotencyKey: "sms-" + Date.now(),
      channel: "sms",
      recipient: "+1234567890",
      body: "Test SMS message",
      metadata: { priority: "high" },
    });

    assertEquals(response.status, 202, "Status code");
    assertEquals(response.body.status, "ACCEPTED", "Response status");
    assertFieldsExist(
      response.body,
      ["messageId", "traceId", "status"],
      "Response fields"
    );
  });

  await runTest("SMS with international phone number", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-sms-" + Date.now(),
      userId: "u-sms-" + Date.now(),
      idempotencyKey: "sms-" + Date.now(),
      channel: "sms",
      recipient: "+447911123456",
      body: "International SMS test",
      metadata: { priority: "normal" },
    });

    assertEquals(response.status, 202, "Status code");
    assertEquals(response.body.status, "ACCEPTED", "Response status");
  });

  await runTest("SMS with maximum 160 characters", async () => {
    const body160 = "A".repeat(160);
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-sms-" + Date.now(),
      userId: "u-sms-" + Date.now(),
      idempotencyKey: "sms-" + Date.now(),
      channel: "sms",
      recipient: "+1234567890",
      body: body160,
      metadata: { priority: "high" },
    });

    assertEquals(response.status, 202, "Status code");
    assertEquals(response.body.status, "ACCEPTED", "Response status");
  });

  await runTest("SMS exceeding 160 characters (multi-part)", async () => {
    const body320 = "B".repeat(320);
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-sms-" + Date.now(),
      userId: "u-sms-" + Date.now(),
      idempotencyKey: "sms-" + Date.now(),
      channel: "sms",
      recipient: "+1234567890",
      body: body320,
      metadata: { priority: "normal" },
    });

    assertEquals(response.status, 202, "Status code");
    assertEquals(response.body.status, "ACCEPTED", "Response status");
  });
}

async function testWhatsAppChannel() {
  console.log("\n[WHATSAPP CHANNEL TESTS]");

  await runTest("Send WhatsApp message", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-wa-" + Date.now(),
      userId: "u-wa-" + Date.now(),
      idempotencyKey: "wa-" + Date.now(),
      channel: "whatsapp",
      recipient: "+1987654321",
      body: "Test WhatsApp message",
      metadata: { priority: "high" },
    });

    assertEquals(response.status, 202, "Status code");
    assertEquals(response.body.status, "ACCEPTED", "Response status");
    assertFieldsExist(
      response.body,
      ["messageId", "traceId", "status"],
      "Response fields"
    );
  });

  await runTest("WhatsApp with emoji", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-wa-" + Date.now(),
      userId: "u-wa-" + Date.now(),
      idempotencyKey: "wa-" + Date.now(),
      channel: "whatsapp",
      recipient: "+1987654321",
      body: "Test with emoji and symbols",
      metadata: { priority: "normal" },
    });

    assertEquals(response.status, 202, "Status code");
    assertEquals(response.body.status, "ACCEPTED", "Response status");
  });

  await runTest("WhatsApp with media URL", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-wa-" + Date.now(),
      userId: "u-wa-" + Date.now(),
      idempotencyKey: "wa-" + Date.now(),
      channel: "whatsapp",
      recipient: "+1987654321",
      body: "Check this image: https://example.com/image.jpg",
      metadata: { priority: "high", type: "media" },
    });

    assertEquals(response.status, 202, "Status code");
    assertEquals(response.body.status, "ACCEPTED", "Response status");
  });
}

async function testDuplicateDetection() {
  console.log("\n[DUPLICATE DETECTION TESTS]");

  await runTest("First message accepted", async () => {
    const idempotencyKey = "dedup-" + Date.now() + "-" + Math.random();
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-dedup-" + Date.now(),
      userId: "u-dedup-" + Date.now(),
      idempotencyKey: idempotencyKey,
      channel: "email",
      recipient: "test@example.com",
      body: "First message",
      metadata: { priority: "high" },
    });

    assertEquals(response.status, 202, "Status code");
    assertEquals(
      response.body.status,
      "ACCEPTED",
      "Response status should be ACCEPTED"
    );
  });

  await runTest("Duplicate message rejected", async () => {
    const idempotencyKey = "dedup-dup-" + Date.now();
    const body = {
      tenantId: "t-dedup-dup-" + Date.now(),
      userId: "u-dedup-dup-" + Date.now(),
      idempotencyKey: idempotencyKey,
      channel: "email",
      recipient: "test@example.com",
      body: "Test duplicate",
      metadata: { priority: "high" },
    };

    // Send first message
    const first = await makeRequest("POST", "/api/messages", body);
    assertEquals(first.status, 202, "First message should return 202");
    assertEquals(
      first.body.status,
      "ACCEPTED",
      "First message should be ACCEPTED"
    );

    // Wait 100ms
    await new Promise((r) => setTimeout(r, 100));

    // Send duplicate
    const second = await makeRequest("POST", "/api/messages", body);
    assertEquals(second.status, 200, "Duplicate should return 200");
    assertEquals(
      second.body.status,
      "DUPLICATE",
      "Duplicate should be marked as DUPLICATE"
    );
  });

  await runTest(
    "Same userId with different idempotencyKey accepted",
    async () => {
      const userId = "u-dedup-multi-" + Date.now();
      const tenantId = "t-dedup-multi-" + Date.now();

      const response1 = await makeRequest("POST", "/api/messages", {
        tenantId: tenantId,
        userId: userId,
        idempotencyKey: "msg-1-" + Date.now(),
        channel: "email",
        recipient: "test@example.com",
        body: "Message 1",
        metadata: { priority: "high" },
      });

      const response2 = await makeRequest("POST", "/api/messages", {
        tenantId: tenantId,
        userId: userId,
        idempotencyKey: "msg-2-" + Date.now(),
        channel: "email",
        recipient: "test@example.com",
        body: "Message 2",
        metadata: { priority: "high" },
      });

      assertEquals(response1.body.status, "ACCEPTED", "First message accepted");
      assertEquals(
        response2.body.status,
        "ACCEPTED",
        "Second message accepted (different key)"
      );
    }
  );
}

async function testValidationErrors() {
  console.log("\n[VALIDATION ERROR TESTS]");

  await runTest("Missing tenantId", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      userId: "u-test",
      idempotencyKey: "test",
      channel: "email",
      recipient: "test@example.com",
      body: "Test",
      metadata: {},
    });

    // Should get 400 error or REJECTED status
    if (response.status === 200) {
      assertEquals(
        response.body.status,
        "REJECTED",
        "Should reject missing tenantId"
      );
    } else {
      assertEquals(response.status, 400, "Should return 400 for missing field");
    }
  });

  await runTest("Missing userId", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-test",
      idempotencyKey: "test",
      channel: "email",
      recipient: "test@example.com",
      body: "Test",
      metadata: {},
    });

    if (response.status === 200) {
      assertEquals(
        response.body.status,
        "REJECTED",
        "Should reject missing userId"
      );
    } else {
      assertEquals(response.status, 400, "Should return 400 for missing field");
    }
  });

  await runTest("Missing idempotencyKey", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-test",
      userId: "u-test",
      channel: "email",
      recipient: "test@example.com",
      body: "Test",
      metadata: {},
    });

    if (response.status === 200) {
      assertEquals(
        response.body.status,
        "REJECTED",
        "Should reject missing idempotencyKey"
      );
    } else {
      assertEquals(response.status, 400, "Should return 400 for missing field");
    }
  });

  await runTest("Invalid channel", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-test",
      userId: "u-test",
      idempotencyKey: "test",
      channel: "invalid-channel",
      recipient: "test@example.com",
      body: "Test",
      metadata: {},
    });

    if (response.status === 200) {
      assertEquals(
        response.body.status,
        "REJECTED",
        "Should reject invalid channel"
      );
    } else {
      assertEquals(
        response.status,
        400,
        "Should return 400 for invalid channel"
      );
    }
  });

  await runTest("Empty body", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-test",
      userId: "u-test",
      idempotencyKey: "test",
      channel: "email",
      recipient: "test@example.com",
      body: "",
      metadata: {},
    });

    if (response.status === 200) {
      assertEquals(
        response.body.status,
        "REJECTED",
        "Should reject empty body"
      );
    } else {
      assertEquals(response.status, 400, "Should return 400 for empty body");
    }
  });

  await runTest("Invalid email recipient", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-test",
      userId: "u-test",
      idempotencyKey: "test",
      channel: "email",
      recipient: "invalid-email",
      body: "Test",
      metadata: {},
    });

    if (response.status === 200) {
      assertEquals(
        response.body.status,
        "REJECTED",
        "Should reject invalid email"
      );
    } else {
      assertEquals(response.status, 400, "Should return 400 for invalid email");
    }
  });
}

async function testMultiTenant() {
  console.log("\n[MULTI-TENANT TESTS]");

  await runTest("Different tenants isolated", async () => {
    const idempotencyKey = "multi-tenant-" + Date.now();

    const response1 = await makeRequest("POST", "/api/messages", {
      tenantId: "tenant-a-" + Date.now(),
      userId: "u-test",
      idempotencyKey: idempotencyKey,
      channel: "email",
      recipient: "test@example.com",
      body: "Tenant A message",
      metadata: {},
    });

    const response2 = await makeRequest("POST", "/api/messages", {
      tenantId: "tenant-b-" + Date.now(),
      userId: "u-test",
      idempotencyKey: idempotencyKey,
      channel: "email",
      recipient: "test@example.com",
      body: "Tenant B message",
      metadata: {},
    });

    assertEquals(
      response1.body.status,
      "ACCEPTED",
      "Tenant A message accepted"
    );
    assertEquals(
      response2.body.status,
      "ACCEPTED",
      "Tenant B message accepted (same key but different tenant)"
    );
  });

  await runTest("Multiple users in same tenant", async () => {
    const tenantId = "tenant-multi-" + Date.now();

    const response1 = await makeRequest("POST", "/api/messages", {
      tenantId: tenantId,
      userId: "user-1",
      idempotencyKey: "msg-1-" + Date.now(),
      channel: "email",
      recipient: "user1@example.com",
      body: "Message for user 1",
      metadata: {},
    });

    const response2 = await makeRequest("POST", "/api/messages", {
      tenantId: tenantId,
      userId: "user-2",
      idempotencyKey: "msg-2-" + Date.now(),
      channel: "email",
      recipient: "user2@example.com",
      body: "Message for user 2",
      metadata: {},
    });

    assertEquals(response1.body.status, "ACCEPTED", "User 1 message accepted");
    assertEquals(response2.body.status, "ACCEPTED", "User 2 message accepted");
  });
}

async function testMetadata() {
  console.log("\n[METADATA TESTS]");

  await runTest("Message with priority metadata", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-meta-" + Date.now(),
      userId: "u-meta-" + Date.now(),
      idempotencyKey: "meta-" + Date.now(),
      channel: "email",
      recipient: "test@example.com",
      body: "Test message",
      metadata: { priority: "high" },
    });

    assertEquals(response.status, 202, "Status code");
    assertEquals(response.body.status, "ACCEPTED", "Response status");
  });

  await runTest("Message with custom metadata fields", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-meta-" + Date.now(),
      userId: "u-meta-" + Date.now(),
      idempotencyKey: "meta-custom-" + Date.now(),
      channel: "email",
      recipient: "test@example.com",
      body: "Test message",
      metadata: {
        priority: "high",
        template: "welcome",
        userId: "12345",
        orderId: "ORD-6789",
        customField: "customValue",
      },
    });

    assertEquals(response.status, 202, "Status code");
    assertEquals(response.body.status, "ACCEPTED", "Response status");
  });

  await runTest("Message with empty metadata", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-meta-" + Date.now(),
      userId: "u-meta-" + Date.now(),
      idempotencyKey: "meta-empty-" + Date.now(),
      channel: "email",
      recipient: "test@example.com",
      body: "Test message",
      metadata: {},
    });

    assertEquals(response.status, 202, "Status code");
    assertEquals(response.body.status, "ACCEPTED", "Response status");
  });
}

async function testResponseFormat() {
  console.log("\n[RESPONSE FORMAT TESTS]");

  await runTest("Response contains required fields", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-resp-" + Date.now(),
      userId: "u-resp-" + Date.now(),
      idempotencyKey: "resp-" + Date.now(),
      channel: "email",
      recipient: "test@example.com",
      body: "Test message",
      metadata: { priority: "high" },
    });

    assertFieldsExist(
      response.body,
      ["messageId", "status", "traceId"],
      "Response should contain messageId, status, traceId"
    );
  });

  await runTest("messageId is UUID format", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-uuid-" + Date.now(),
      userId: "u-uuid-" + Date.now(),
      idempotencyKey: "uuid-" + Date.now(),
      channel: "email",
      recipient: "test@example.com",
      body: "Test message",
      metadata: {},
    });

    const uuidRegex =
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    if (!uuidRegex.test(response.body.messageId)) {
      throw new Error(
        `messageId is not valid UUID: ${response.body.messageId}`
      );
    }
  });

  await runTest("traceId is UUID format", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-uuid-" + Date.now(),
      userId: "u-uuid-" + Date.now(),
      idempotencyKey: "uuid-trace-" + Date.now(),
      channel: "email",
      recipient: "test@example.com",
      body: "Test message",
      metadata: {},
    });

    const uuidRegex =
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    if (!uuidRegex.test(response.body.traceId)) {
      throw new Error(`traceId is not valid UUID: ${response.body.traceId}`);
    }
  });

  await runTest("Status field contains valid value", async () => {
    const response = await makeRequest("POST", "/api/messages", {
      tenantId: "t-status-" + Date.now(),
      userId: "u-status-" + Date.now(),
      idempotencyKey: "status-" + Date.now(),
      channel: "email",
      recipient: "test@example.com",
      body: "Test message",
      metadata: {},
    });

    const validStatuses = ["ACCEPTED", "REJECTED", "DUPLICATE"];
    if (!validStatuses.includes(response.body.status)) {
      throw new Error(`Invalid status: ${response.body.status}`);
    }
  });
}

async function testHealthCheck() {
  console.log("\n[HEALTH CHECK TESTS]");

  await runTest("API health check endpoint", async () => {
    const response = await makeRequest("GET", "/health", null);
    assertEquals(response.status, 200, "Health check should return 200");
  });
}

// ==================== Main Test Runner ====================

async function runAllTests() {
  console.log("==========================================================");
  console.log("     NOTIFICATION AGGREGATOR - INTEGRATION TEST SUITE       ");
  console.log("==========================================================");

  console.log(
    "\n[waiting] Connecting to API at http://" + API_HOST + ":" + API_PORT
  );

  try {
    // Run test suites
    await testHealthCheck();
    await testEmailChannel();
    await testSmsChannel();
    await testWhatsAppChannel();
    await testDuplicateDetection();
    await testValidationErrors();
    await testMultiTenant();
    await testMetadata();
    await testResponseFormat();

    // Print summary
    console.log(
      "\n"
    );
    console.log(
      "                        TEST SUMMARY                         "
    );
    console.log("==========================================================");
    console.log(`  Total Tests:   ${testResults.total}`);
    console.log(`  [PASS] Passed:     ${testResults.passed}`);
    console.log(`  [FAIL] Failed:     ${testResults.failed}`);
    console.log("==========================================================");

    if (testResults.failed > 0) {
      console.log("  FAILED TESTS:");
      for (const error of testResults.errors) {
        console.log(`   ${error.test.substring(0, 56)}`);
        console.log(`    ${error.error.substring(0, 54)}`);
      }
      console.log("==========================================================");
      process.exit(1);
    } else {
      const percentage =
        testResults.total > 0
          ? Math.round((testResults.passed / testResults.total) * 100)
          : 0;
      console.log(`  Success Rate:  ${percentage}%`);
      console.log(
        "                                                                "
      );
      console.log("  ALL TESTS PASSED!");
      console.log("==========================================================");
      process.exit(0);
    }
  } catch (error) {
    console.error("\n[FAIL] Fatal error:", error.message);
    console.error(error.stack);
    process.exit(1);
  }
}

// Run tests
if (require.main === module) {
  runAllTests();
}

module.exports = {
  makeRequest,
  runTest,
  assertEquals,
  assertExists,
  assertFieldsExist,
};
