#!/bin/bash

# Test Suite for Notification Aggregator System
# This script tests the complete message flow end-to-end

set -e

BASE_URL="http://localhost:3001"
ELASTICSEARCH_URL="http://localhost:9200"
KAFKA_BROKER="notification-kafka:29092"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  Notification Aggregator System - End-to-End Test Suite    ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}\n"

# Test 1: Check if API is accessible
echo -e "${YELLOW}[Test 1] Checking if task-router API is accessible...${NC}"
if curl -s "${BASE_URL}/messages" -X POST -H "Content-Type: application/json" -d '{}' > /dev/null 2>&1; then
    echo -e "${GREEN}✓ API is accessible${NC}\n"
else
    echo -e "${RED}✗ API is not accessible${NC}\n"
    exit 1
fi

# Test 2: Send Email Message
echo -e "${YELLOW}[Test 2] Sending email message...${NC}"
EMAIL_RESPONSE=$(curl -s -X POST "${BASE_URL}/messages" \
  -H "Content-Type: application/json" \
  -d '{
    "tenantId": "test-tenant-001",
    "userId": "user-email-123",
    "idempotencyKey": "test-email-'$(date +%s)'",
    "channel": "email",
    "recipient": "john.doe@example.com",
    "body": "Hello! This is a test email message from the notification system."
  }')

EMAIL_MESSAGE_ID=$(echo "$EMAIL_RESPONSE" | grep -o '"messageId":"[^"]*' | head -1 | cut -d'"' -f4)
EMAIL_TRACE_ID=$(echo "$EMAIL_RESPONSE" | grep -o '"traceId":"[^"]*' | head -1 | cut -d'"' -f4)

if [ ! -z "$EMAIL_MESSAGE_ID" ]; then
    echo -e "${GREEN}✓ Email message sent${NC}"
    echo "  Message ID: $EMAIL_MESSAGE_ID"
    echo "  Trace ID: $EMAIL_TRACE_ID"
else
    echo -e "${RED}✗ Failed to send email${NC}"
    echo "Response: $EMAIL_RESPONSE"
fi
echo ""

# Test 3: Send SMS Message
echo -e "${YELLOW}[Test 3] Sending SMS message...${NC}"
SMS_RESPONSE=$(curl -s -X POST "${BASE_URL}/messages" \
  -H "Content-Type: application/json" \
  -d '{
    "tenantId": "test-tenant-001",
    "userId": "user-sms-456",
    "idempotencyKey": "test-sms-'$(date +%s)'",
    "channel": "sms",
    "recipient": "+1234567890",
    "body": "Hi! This is a test SMS message from the notification system."
  }')

SMS_MESSAGE_ID=$(echo "$SMS_RESPONSE" | grep -o '"messageId":"[^"]*' | head -1 | cut -d'"' -f4)
SMS_TRACE_ID=$(echo "$SMS_RESPONSE" | grep -o '"traceId":"[^"]*' | head -1 | cut -d'"' -f4)

if [ ! -z "$SMS_MESSAGE_ID" ]; then
    echo -e "${GREEN}✓ SMS message sent${NC}"
    echo "  Message ID: $SMS_MESSAGE_ID"
    echo "  Trace ID: $SMS_TRACE_ID"
else
    echo -e "${RED}✗ Failed to send SMS${NC}"
    echo "Response: $SMS_RESPONSE"
fi
echo ""

# Test 4: Send WhatsApp Message
echo -e "${YELLOW}[Test 4] Sending WhatsApp message...${NC}"
WHATSAPP_RESPONSE=$(curl -s -X POST "${BASE_URL}/messages" \
  -H "Content-Type: application/json" \
  -d '{
    "tenantId": "test-tenant-001",
    "userId": "user-whatsapp-789",
    "idempotencyKey": "test-whatsapp-'$(date +%s)'",
    "channel": "whatsapp",
    "recipient": "+1987654321",
    "body": "Hey! This is a test WhatsApp message from the notification system."
  }')

WHATSAPP_MESSAGE_ID=$(echo "$WHATSAPP_RESPONSE" | grep -o '"messageId":"[^"]*' | head -1 | cut -d'"' -f4)
WHATSAPP_TRACE_ID=$(echo "$WHATSAPP_RESPONSE" | grep -o '"traceId":"[^"]*' | head -1 | cut -d'"' -f4)

if [ ! -z "$WHATSAPP_MESSAGE_ID" ]; then
    echo -e "${GREEN}✓ WhatsApp message sent${NC}"
    echo "  Message ID: $WHATSAPP_MESSAGE_ID"
    echo "  Trace ID: $WHATSAPP_TRACE_ID"
else
    echo -e "${RED}✗ Failed to send WhatsApp${NC}"
    echo "Response: $WHATSAPP_RESPONSE"
fi
echo ""

# Test 5: Check Kafka Topic Content
echo -e "${YELLOW}[Test 5] Checking Kafka topics...${NC}"
MESSAGES_EMAIL=$(docker compose exec kafka kafka-console-consumer --topic messages.email --from-beginning --max-messages 1 --bootstrap-server ${KAFKA_BROKER} 2>/dev/null || echo "")
if [ ! -z "$MESSAGES_EMAIL" ]; then
    echo -e "${GREEN}✓ Messages in messages.email topic${NC}"
else
    echo -e "${YELLOW}⚠ Waiting for messages in messages.email (may take a moment)${NC}"
fi
echo ""

# Test 6: Check Consumer Group Lag
echo -e "${YELLOW}[Test 6] Checking consumer group status...${NC}"
CONSUMER_STATUS=$(docker compose exec kafka kafka-consumer-groups --describe --group delivery-workers-group --bootstrap-server ${KAFKA_BROKER} 2>/dev/null | head -1)
if echo "$CONSUMER_STATUS" | grep -q "delivery-workers-group"; then
    echo -e "${GREEN}✓ delivery-workers-group is active${NC}"
    docker compose exec kafka kafka-consumer-groups --describe --group delivery-workers-group --bootstrap-server ${KAFKA_BROKER} 2>/dev/null | head -5
else
    echo -e "${RED}✗ delivery-workers-group status check failed${NC}"
fi
echo ""

# Test 7: Check Elasticsearch for Logs
echo -e "${YELLOW}[Test 7] Checking Elasticsearch for logs...${NC}"
LOGS_COUNT=$(curl -s "${ELASTICSEARCH_URL}/logs-*/_count?pretty" 2>/dev/null | grep -o '"count" : [0-9]*' | grep -o '[0-9]*' || echo "0")
if [ "$LOGS_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✓ Found $LOGS_COUNT log entries in Elasticsearch${NC}"
else
    echo -e "${YELLOW}⚠ No logs in Elasticsearch yet (may take a moment)${NC}"
fi
echo ""

# Test 8: Check for Duplicates (Idempotency Test)
echo -e "${YELLOW}[Test 8] Testing duplicate detection (sending same message twice)...${NC}"
IDEMPOTENCY_KEY="test-duplicate-$(date +%s)"
RESPONSE_1=$(curl -s -X POST "${BASE_URL}/messages" \
  -H "Content-Type: application/json" \
  -d '{
    "tenantId": "test-tenant-001",
    "userId": "user-dedup-123",
    "idempotencyKey": "'${IDEMPOTENCY_KEY}'",
    "channel": "email",
    "recipient": "dedup.test@example.com",
    "body": "First message"
  }')

MESSAGE_ID_1=$(echo "$RESPONSE_1" | grep -o '"messageId":"[^"]*' | head -1 | cut -d'"' -f4)
STATUS_1=$(echo "$RESPONSE_1" | grep -o '"status":"[^"]*' | head -1 | cut -d'"' -f4)

sleep 1

RESPONSE_2=$(curl -s -X POST "${BASE_URL}/messages" \
  -H "Content-Type: application/json" \
  -d '{
    "tenantId": "test-tenant-001",
    "userId": "user-dedup-123",
    "idempotencyKey": "'${IDEMPOTENCY_KEY}'",
    "channel": "email",
    "recipient": "dedup.test@example.com",
    "body": "First message"
  }')

MESSAGE_ID_2=$(echo "$RESPONSE_2" | grep -o '"messageId":"[^"]*' | head -1 | cut -d'"' -f4)
STATUS_2=$(echo "$RESPONSE_2" | grep -o '"status":"[^"]*' | head -1 | cut -d'"' -f4)

if [ "$STATUS_1" = "ACCEPTED" ] && [ "$STATUS_2" = "DUPLICATE" ]; then
    echo -e "${GREEN}✓ Duplicate detection working correctly${NC}"
    echo "  First send: $STATUS_1 (ID: $MESSAGE_ID_1)"
    echo "  Duplicate: $STATUS_2 (ID: $MESSAGE_ID_2)"
else
    echo -e "${YELLOW}⚠ Duplicate detection status: $STATUS_1 / $STATUS_2${NC}"
fi
echo ""

# Summary
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                    Test Summary                             ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}\n"

echo -e "${GREEN}✓ Test Suite Complete${NC}\n"

echo "Messages Sent:"
echo "  1. Email   - ID: $EMAIL_MESSAGE_ID"
echo "  2. SMS     - ID: $SMS_MESSAGE_ID"
echo "  3. WhatsApp - ID: $WHATSAPP_MESSAGE_ID"
echo ""

echo "Next Steps:"
echo "  1. Monitor logs in Kibana: http://localhost:5601"
echo "  2. Search by traceId in Kibana to trace message flow"
echo "  3. Check consumer group lag:"
echo "     docker compose exec kafka kafka-consumer-groups --describe --group delivery-workers-group --bootstrap-server notification-kafka:29092"
echo ""

echo "View test data:"
echo "  - Raw messages: docker compose exec kafka kafka-console-consumer --topic messages.email --from-beginning --bootstrap-server notification-kafka:29092"
echo "  - Elasticsearch logs: curl http://localhost:9200/logs-*/_search?pretty"
echo ""
