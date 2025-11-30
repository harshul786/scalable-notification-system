#!/bin/bash

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Notification Aggregator System - Topic & Group Verification ===${NC}\n"

# Expected topics
EXPECTED_TOPICS=(
  "messages.email"
  "messages.sms"
  "messages.whatsapp"
  "logs"
  "dlq.email"
  "dlq.sms"
  "dlq.whatsapp"
)

# Expected consumer groups
EXPECTED_GROUPS=(
  "delivery-workers-group"
  "logger-group"
)

# Check if Kafka is running
echo -e "${YELLOW}Checking Kafka connectivity...${NC}"
if docker compose exec kafka kafka-topics --list --bootstrap-server notification-kafka:29092 > /dev/null 2>&1; then
  echo -e "${GREEN}✓ Kafka is accessible${NC}\n"
else
  echo -e "${RED}✗ Kafka is not accessible${NC}"
  exit 1
fi

# Check topics
echo -e "${YELLOW}Verifying Kafka Topics...${NC}"
ACTUAL_TOPICS=$(docker compose exec kafka kafka-topics --list --bootstrap-server notification-kafka:29092)

for topic in "${EXPECTED_TOPICS[@]}"; do
  if echo "$ACTUAL_TOPICS" | grep -q "^${topic}$"; then
    echo -e "${GREEN}✓ Topic '${topic}' exists${NC}"
  else
    echo -e "${RED}✗ Topic '${topic}' NOT FOUND${NC}"
  fi
done

echo ""

# Check topic details
echo -e "${YELLOW}Topic Configuration Details:${NC}"
for topic in "${EXPECTED_TOPICS[@]}"; do
  docker compose exec kafka kafka-topics --describe --topic "$topic" --bootstrap-server notification-kafka:29092 2>/dev/null | head -1
done

echo ""

# Check consumer groups
echo -e "${YELLOW}Verifying Consumer Groups...${NC}"
ACTUAL_GROUPS=$(docker compose exec kafka kafka-consumer-groups --list --bootstrap-server notification-kafka:29092)

for group in "${EXPECTED_GROUPS[@]}"; do
  if echo "$ACTUAL_GROUPS" | grep -q "^${group}$"; then
    echo -e "${GREEN}✓ Consumer Group '${group}' exists${NC}"
  else
    echo -e "${YELLOW}⚠ Consumer Group '${group}' not yet created (will be created when service starts)${NC}"
  fi
done

echo ""

# Check service environment variables
echo -e "${YELLOW}Verifying Service Environment Variables...${NC}"

services=("notification-task-router" "notification-aggregator-service" "notification-logger-service")

for service in "${services[@]}"; do
  echo -e "\n${BLUE}Service: ${service}${NC}"
  
  KAFKAJS_WARNING=$(docker compose exec "$service" printenv KAFKAJS_NO_PARTITIONER_WARNING 2>/dev/null || echo "NOT SET")
  KAFKA_BROKER=$(docker compose exec "$service" printenv KAFKA_BROKER 2>/dev/null || echo "NOT SET")
  
  if [ "$KAFKAJS_WARNING" == "1" ]; then
    echo -e "  ${GREEN}✓ KAFKAJS_NO_PARTITIONER_WARNING = 1${NC}"
  else
    echo -e "  ${YELLOW}⚠ KAFKAJS_NO_PARTITIONER_WARNING = ${KAFKAJS_WARNING}${NC}"
  fi
  
  if [[ "$KAFKA_BROKER" == *"notification-kafka:29092"* ]]; then
    echo -e "  ${GREEN}✓ KAFKA_BROKER = ${KAFKA_BROKER}${NC}"
  else
    echo -e "  ${RED}✗ KAFKA_BROKER = ${KAFKA_BROKER} (expected: notification-kafka:29092)${NC}"
  fi
done

echo ""
echo -e "${BLUE}=== Verification Complete ===${NC}"
echo -e "\n${YELLOW}To monitor consumer group lag:${NC}"
echo "docker compose exec kafka kafka-consumer-groups --describe --group delivery-workers-group --bootstrap-server notification-kafka:29092"
echo ""
echo -e "${YELLOW}To view topic contents:${NC}"
echo "docker compose exec kafka kafka-console-consumer --topic logs --from-beginning --bootstrap-server notification-kafka:29092 --max-messages 10"
echo ""
