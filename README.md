# 📡 Notification Aggregator System

A production-grade 4-microservice communication routing platform built with **Node.js + TypeScript + Kafka + Redis + MySQL + Elasticsearch**. This system receives messages from clients, routes them to the correct communication channel (Email, SMS, WhatsApp), ensures exactly-once delivery semantics, retries failed deliveries with exponential backoff, and provides full observability via Elasticsearch + Kibana.

**Built using:** Clean event-driven architecture with Domain-Driven Design (DDD) and SOLID principles.

---

## 🏗️ Architecture Overview

### System Components

```
┌──────────────────────────────────────────────────────────────────────┐
│                     CLIENT APPLICATION                              │
│                                                                      │
│                  POST /api/messages (HTTP REST)                      │
└─────────────────────────┬──────────────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────────┐
        │     TASK ROUTER SERVICE (3001)      │
        │  • HTTP REST API Entry Point        │
        │  • Validates & Deduplicates         │
        │  • Publishes to Kafka               │
        │  • Generates traceId for tracking   │
        └────────────┬────────────────────────┘
                     │
                     │ Kafka Topics:
                     ├─ messages.email (3 partitions)
                     ├─ messages.sms (3 partitions)
                     └─ messages.whatsapp (3 partitions)
                     │
                     ▼
   ┌──────────────────────────────────────────────┐
   │   NOTIFICATION AGGREGATOR (Kafka Consumer)   │
   │    consumer-group: delivery-workers-group    │
   │                                              │
   │   • Consumes from messages.{channel}         │
   │   • Applies delivery deduplication           │
   │   • Rate limiting & circuit breaker          │
   │   • Calls provider APIs (simulated)          │
   │   • Manages retries with exponential backoff │
   │   • Updates MySQL message status             │
   │   • Publishes to DLQ on max attempts         │
   └──────────┬──────────────────────────┬────────┘
              │                          │
              │ Redis ZSET              │ MySQL
              │ (retry queue)           │ (message tracking)
              │                         │
              ▼                         ▼
   ┌──────────────────────┐    ┌──────────────────┐
   │  RETRY SCHEDULER     │    │  DATABASE        │
   │  (Background Job)    │    │  • messages      │
   │                      │    │  • delivery_     │
   │  • Polls Redis ZSET  │    │    attempts      │
   │  • Republishes to    │    │  • dlq_entries   │
   │    Kafka on retry    │    └──────────────────┘
   └──────────┬───────────┘
              │
              │ Kafka Topics:
              ├─ dlq.email
              ├─ dlq.sms
              └─ dlq.whatsapp
              │
              ▼
   ┌──────────────────────────────────────────────┐
   │      LOGGER SERVICE (Kafka Consumer)         │
   │     consumer-group: logger-group (1 instance)│
   │                                              │
   │   • Consumes from logs & errors topics       │
   │   • Preserves log ordering (1 partition)     │
   │   • Persists to Elasticsearch                │
   │   • Enables Kibana visualization             │
   └──────────┬───────────────────────────────────┘
              │
              ▼
   ┌──────────────────────────────────┐
   │      ELASTICSEARCH + KIBANA       │
   │  • logs-YYYY.MM.DD indices        │
   │  • Full-text search               │
   │  • Aggregations & dashboards      │
   │  • Message journey tracking       │
   └──────────────────────────────────┘
```

### Data Flow

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │ 1. POST /messages
       ▼
┌─────────────────────────────────────────┐
│       TASK ROUTER (Port 3001)           │
│                                         │
│ 1. Validate input                       │
│ 2. Generate messageId, dedupKey, traceId
│ 3. Redis SETNX dedup (1-hour TTL)      │
│    ├─ Exists? → Return DUPLICATE       │
│    └─ New? → Continue                  │
│ 4. Insert PENDING to MySQL              │
│ 5. Publish to Kafka (key=userId)        │
│ 6. Emit log to logs topic               │
│ 7. Return HTTP 202 ACCEPTED             │
└──────┬──────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────┐
│       KAFKA BROKER (3 topics)           │
│  • messages.email (3 partitions)        │
│  • messages.sms (3 partitions)          │
│  • messages.whatsapp (3 partitions)     │
└──────┬──────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────┐
│  NOTIFICATION AGGREGATOR (Kafka Consumer)   │
│                                              │
│ 1. Consume message                           │
│ 2. Redis SETNX delivered (idempotency)       │
│    ├─ Exists? → Skip provider, commit offset│
│    └─ New? → Continue                       │
│ 3. Check DB message status                   │
│ 4. Apply rate limiting (token bucket)        │
│ 5. Check circuit breaker                     │
│ 6. Call provider API                         │
│ 7. On Success:                               │
│    ├─ Update messages.status = SENT          │
│    ├─ Insert delivery_attempts               │
│    ├─ Emit success log                       │
│    └─ Commit Kafka offset                    │
│ 8. On Failure:                               │
│    ├─ Calculate backoff delay                │
│    ├─ ZADD retries (Redis ZSET)              │
│    ├─ Insert delivery_attempts               │
│    ├─ Emit failure log                       │
│    └─ Do NOT commit offset (Kafka redelivers)
└──────┬────────────────────────────────────────┘
       │
       ├─→ Redis ZSET (retries)
       │   └─ Scheduled for retry
       │
       └─→ DLQ (max attempts exceeded)
           └─ Escalated for manual review
       │
       ▼
┌───────────────────────────────────┐
│     RETRY SCHEDULER               │
│   (Background Job)                │
│                                   │
│ 1. Poll Redis ZSET every 100ms    │
│ 2. Find due retries               │
│ 3. Republish to Kafka             │
│ 4. Increment attempt counter      │
│ 5. If max attempts exceeded:      │
│    └─ Publish to dlq.{channel}    │
└───────────────────────────────────┘
```

---

## 💬 Communication Method & Reasoning

### **Task Router ↔ Clients: REST HTTP**

**Why HTTP?**

- Industry standard for synchronous client communication
- Easy to use and integrate with existing systems
- Immediate response to client
- Simple for testing and debugging

**Response Codes:**

- `202 ACCEPTED` - New message queued for delivery
- `200 OK` - Duplicate message detected (idempotent)
- `400 BAD REQUEST` - Validation error

---

### **Inter-Service Communication: Kafka (Event-Driven)**

**Why Kafka instead of direct RPC?**

| Aspect                  | HTTP RPC                   | Kafka (Chosen)             |
| ----------------------- | -------------------------- | -------------------------- |
| **Coupling**            | Tight                      | Loose                      |
| **Failure Handling**    | Complex retries            | Built-in queue             |
| **Ordering**            | Not guaranteed             | Guaranteed (per partition) |
| **Scalability**         | Limited                    | Horizontal scaling         |
| **Observability**       | Request/Response           | Event stream tracking      |
| **Backpressure**        | Server overload risk       | Natural backpressure       |
| **Decoupling Services** | Services wait for response | Services independent       |

**Kafka Topics:**

| Topic               | Partitions | Purpose                   | Key       |
| ------------------- | ---------- | ------------------------- | --------- |
| `messages.email`    | 3          | Email delivery queue      | userId    |
| `messages.sms`      | 3          | SMS delivery queue        | userId    |
| `messages.whatsapp` | 3          | WhatsApp delivery queue   | userId    |
| `logs`              | 1          | Structured logs (ordered) | traceId   |
| `errors`            | 1          | Error logs                | traceId   |
| `dlq.email`         | 1          | Failed emails             | messageId |
| `dlq.sms`           | 1          | Failed SMS                | messageId |
| `dlq.whatsapp`      | 1          | Failed WhatsApp           | messageId |

**Key Decision: User-Based Partitioning**

```
partitionKey = userId
```

- Ensures all messages from one user go to same partition
- Preserves delivery order per user
- Enables horizontal scaling across different users
- Prevents head-of-line blocking

---

### **Data Persistence: Redis + MySQL**

**Redis (Caching & Deduplication)**

- `dedup:{dedupKey}` → Ingress deduplication (1-hour TTL)
- `delivered:{dedupKey}` → Delivery deduplication (permanent)
- `rate_limit:{provider}` → Token bucket for rate limiting
- `retries` (ZSET) → Scheduled retries with exponential backoff

**Why Redis?**

- Sub-millisecond latency
- Atomic operations (SETNX)
- Sorted sets for scheduling
- In-memory availability

**MySQL (Audit Trail & Status)**

- `messages` → Message metadata & final status
- `delivery_attempts` → Complete attempt history
- `dlq_entries` → Failed messages for manual intervention

**Why MySQL?**

- Durable persistence
- ACID compliance
- Audit trail requirement
- Complex queries (attempts history)

---

### **Observability: Elasticsearch + Kibana**

**Why Elasticsearch?**

- Full-text search across millions of logs
- Real-time analytics and aggregations
- Time-series analysis
- Integration with Kibana dashboards
- Distributed log correlation

**Log Tracking with traceId:**

```json
{
  "traceId": "550e8400-e29b-41d4-a716-446655440000",
  "service": "router",
  "messageId": "abc123",
  "userId": "user456",
  "channel": "email",
  "status": "ACCEPTED",
  "timestamp": "2024-01-15T10:30:45.123Z"
}
```

Single `traceId` follows message through entire system:

1. Router: `ACCEPTED`
2. Aggregator: `SENT` or `FAILED`
3. Scheduler: `RETRYING`
4. Aggregator: Final `SENT` or DLQ

---

## 🚀 Quick Start (5 minutes)

### ⚡ FASTEST: All-in-One Command (No Test Containers)

```bash
# 1. Install & build
npm install && npm run build

# 2. Start everything
docker-compose up

# 3. Verify (wait 10 seconds for services to initialize)
sleep 10
curl http://localhost:3001/health | jq '.'
```

**Expected output:** JSON response with service status ✅

---

## 🚀 How to Start: Step-by-Step (Recommended for Development)

### Prerequisites

```bash
# 1. Install dependencies
npm install

# 2. Build TypeScript
npm run build
```

### Step 1: Start Infrastructure (Docker)

```bash
docker-compose up -d
```

**What starts:**

- ✅ Kafka (message broker)
- ✅ Redis (caching & deduplication)
- ✅ MySQL (persistent storage)
- ✅ Elasticsearch (log indexing)
- ✅ Kibana (log visualization)

**Verify containers are running:**

```bash
docker-compose ps

# All should show STATUS: "Up X seconds"
```

---

### Step 2: Start Services (Open 3 Terminal Windows)

**Terminal 1: Task Router (REST API Entry Point)**

```bash
npm run start:router
```

Expected output:

```
✓ Connected to Redis
✓ Connected to Kafka
✓ Connected to Elasticsearch
✓ Task Router listening on http://localhost:3001
```

---

**Terminal 2: Notification Aggregator (Delivery Worker)**

```bash
npm run start:aggregator
```

Expected output:

```
✓ Connected to MySQL
✓ Connected to Redis
✓ Connected to Kafka
✓ Subscribed to topics: messages.email, messages.sms, messages.whatsapp
✓ Ready to process messages
```

---

**Terminal 3: Logger Service (Log Consumer)**

```bash
npm run start:logger
```

Expected output:

```
✓ Connected to Elasticsearch
✓ Connected to Kafka
✓ Consuming logs from topic: logs
✓ Logger service ready
```

---

### Step 3: Verify Everything Works

```bash
# Quick health check
curl http://localhost:3001/health | jq '.'

# Should return:
# {
#   "status": "OK",
#   "timestamp": "2025-01-15T10:30:00Z"
# }
```

---

## 🧪 Running Tests (Optional)

### Test Infrastructure Improvements

The test suite now includes several improvements for better reliability and maintainability:

- **Deterministic Test IDs:** Uses a test counter instead of random `Date.now()` calls, ensuring reproducible test execution
- **Consistent Test Data:** Fixed tenant and user IDs per test to avoid non-deterministic behavior
- **Helper Function:** `getTestId(prefix)` generates stable test identifiers with readable prefixes
- **Better Scoping:** Test data is isolated per test case to prevent cross-test pollution

**Example test with improvements:**

```javascript
// Before: Non-deterministic
const idempotencyKey = "dedup-dup-" + Date.now();
const tenantId = "t-dedup-dup-" + Date.now();

// After: Deterministic and readable
const idempotencyKey = getTestId("dedup-dup");     // dedup-dup-0
const tenantId = "tenant-dedup-2";                  // consistent per test
```

---

### Option A: Run Tests WITHOUT Docker Containers

**For development/debugging (services must already be running):**

```bash
npm run test:integration
```

Runs all tests and displays results in terminal.

**Features:**

- Tests run directly against running services
- Fast feedback loop (useful during development)
- Services must be started beforehand (see Quick Start)
- Each test uses stable, deterministic IDs

---

### Option B: Run Tests WITH Docker Containers (Recommended for CI/CD)

Tests run in Docker containers after all services start.

**Using `--profile test` flag:**

```bash
docker-compose --profile test up
```

This will:

1. Start all infrastructure services (Kafka, Redis, MySQL, ES, Kibana)
2. Start application services (Router, Aggregator, Logger)
3. Run integration tests automatically
4. Display test results

**Cleanup after tests:**

```bash
docker-compose --profile test down
```

---

### Option C: Run Tests WITH Environment Variable

```bash
RUN_TESTS=true docker-compose up
```

Same behavior as Option B, but using environment variable instead of profile flag.

---

### Test Coverage

The test suite covers:

| Area | Tests | Focus |
|------|-------|-------|
| **Duplicate Detection** | 3 | Idempotency, dedup keys, multiple messages |
| **Channel Tests** | 12 | Email, SMS, WhatsApp with variations |
| **Validation** | 6 | Required fields, formats, constraints |
| **Multi-Tenancy** | 2 | Tenant isolation, user separation |
| **Metadata** | 3 | Priority, custom fields, empty metadata |
| **Response Format** | 4 | UUID validation, status codes, required fields |
| **DB-Anchored Idempotency** | 4 | Kafka queueing, Redis caching, batch processing |

**Total: 34 comprehensive integration tests**

---

## 📋 Complete Comparison

| Method                         | Time  | Use Case               | Command                              |
| ------------------------------ | ----- | ---------------------- | ------------------------------------ |
| **Step-by-Step (3 terminals)** | 2 min | Development            | See "How to Start: Step-by-Step"     |
| **All-in-One (1 command)**     | 2 min | Quick demo             | `docker-compose up -d && npm run...` |
| **Tests Only (no containers)** | 30s   | Unit/integration tests | `npm run test:integration`           |
| **Tests WITH containers**      | 5 min | Full validation        | `docker-compose --profile test up`   |
| **Complete reset**             | 2 min | Clean state            | `docker-compose down -v`             |

---

## 🔍 Monitoring & Verification

### Real-Time Service Health

Open Terminal 4 (while services are running):

```bash
# Watch Task Router health (updates every 2 seconds)
watch -n 2 'curl -s http://localhost:3001/health | jq'
```

### Check Kafka Topics

```bash
docker exec notification-kafka kafka-topics \
  --list \
  --bootstrap-server=localhost:9092
```

### Check Database

```bash
docker exec -it notification-mysql mysql \
  -u notif_user \
  -pnotif-password \
  notification_db \
  -e "SELECT COUNT(*) as message_count FROM messages;"
```

### Check Logs in Elasticsearch

```bash
curl http://localhost:9200/logs-*/_count | jq '.count'
```

### View Kibana Dashboards

```
Open in browser: http://localhost:5601
```

---

## 🛑 Stop Everything

```bash
# Stop services (Ctrl+C in each terminal, or:)
docker-compose down

# Stop AND remove all data
docker-compose down -v

# Cleanup unused Docker resources
docker system prune -a
```

---

## 📮 Postman Collection

**Complete API testing collection:** [`postman-collection.json`](./postman-collection.json)

### Import into Postman

1. **Open Postman**
2. **Click:** File → Import
3. **Select:** `postman-collection.json` from project root
4. **Configure Variables:**
   - `taskRouterUrl`: `http://localhost:3001`
   - `elasticsearchUrl`: `http://localhost:9200`
   - `kibanaUrl`: `http://localhost:5601`

### Quick API Test

```bash
# Send a test email message
curl -X POST http://localhost:3001/api/messages \
  -H "Content-Type: application/json" \
  -d '{
    "tenantId": "test-tenant",
    "userId": "user-123",
    "idempotencyKey": "req-'$(date +%s%N)'",
    "channel": "email",
    "recipient": "test@example.com",
    "body": "Hello World!"
  }'

# Expected Response (HTTP 202):
# {
#   "messageId": "550e8400-e29b-41d4-a716-446655440000",
#   "dedupKey": "abc123xyz789",
#   "traceId": "550e8400-e29b-41d4-a716-446655440001",
#   "status": "ACCEPTED"
# }
```

### Collection Sections

1. **Health Checks** - Verify services running
2. **Messages API** - Send email, SMS, WhatsApp
3. **Deduplication Testing** - Test duplicate detection
4. **Validation Testing** - Error handling
5. **Bulk Testing** - Multiple concurrent messages
6. **Elasticsearch Queries** - Search and analyze logs
7. **Kibana Dashboards** - Visualization links
8. **Controllers Documentation** - Detailed API docs
9. **Advanced Testing** - E2E, retry, rate limiting, circuit breaker

---

## 📊 System Guarantees

### **Exactly-Once Delivery Semantics**

The system guarantees that each message is delivered exactly once (effectively), achieved through:

```
┌─────────────────────────────────────┐
│  1. INGRESS DEDUPLICATION           │
│  Redis SETNX dedup:{dedupKey}       │
│  • Prevents duplicate requests      │
│  • 1-hour TTL                       │
│  └─ ACCEPTED vs DUPLICATE response  │
└─────────────────────────────────────┘
         ↓
┌─────────────────────────────────────┐
│  2. DELIVERY DEDUPLICATION          │
│  Redis SETNX delivered:{dedupKey}   │
│  • Prevents duplicate provider calls│
│  • Handles Kafka redelivery         │
│  └─ Skip provider, commit offset    │
└─────────────────────────────────────┘
         ↓
┌─────────────────────────────────────┐
│  3. DATABASE VERIFICATION           │
│  Check finalDelivered flag          │
│  • Triple-check before marking SENT │
│  • ACID compliance                  │
│  └─ Reliable audit trail            │
└─────────────────────────────────────┘
         ↓
┌─────────────────────────────────────┐
│  4. OFFSET COMMIT ORDERING          │
│  Commit only after DB writes        │
│  • All writes complete before commit│
│  • No partial state                 │
│  └─ Consistent checkpointing        │
└─────────────────────────────────────┘
```

### **Exponential Backoff Retry Strategy**

On delivery failure, automatic retries with increasing delays:

```
Attempt 1: Immediate
Attempt 2: 1 second delay
Attempt 3: 10 second delay
Attempt 4: 30 second delay
Attempt 5: 5 minute delay
Attempt 6+: Escalate to DLQ
```

### **Rate Limiting (Per Provider)**

```
Email:     100 messages/second
SMS:       50 messages/second
WhatsApp:  30 messages/second
```

### **Circuit Breaker Pattern**

If provider failure rate > 50% in last 100 attempts:

- Open circuit (stop calling provider)
- Queue messages locally
- Retry after cooldown period
- Prevent cascading failures

---

## 🗂️ Project Structure

```
notification-aggregator/
├── services/
│   ├── task-router/                      # HTTP REST API (Port 3001)
│   │   ├── src/
│   │   │   ├── index.ts                  # Express server entry
│   │   │   ├── controllers/
│   │   │   │   └── MessageController.ts  # HTTP request handler
│   │   │   ├── services/
│   │   │   │   ├── HashService.ts        # xxhash64 dedup key
│   │   │   │   ├── DeduplicationService.ts
│   │   │   │   └── MessagePublisher.ts   # Kafka publisher
│   │   │   ├── repositories/
│   │   │   │   └── MessageRepository.ts  # MySQL insert
│   │   │   └── models/
│   │   │       └── Message.ts
│   │   └── package.json
│   │
│   ├── notification-aggregator/          # Kafka Consumer (Delivery)
│   │   ├── src/
│   │   │   ├── index.ts                  # Kafka consumer entry
│   │   │   ├── controllers/
│   │   │   │   └── DeliveryController.ts # Message handler
│   │   │   ├── services/
│   │   │   │   ├── DeliveryProcessorService.ts
│   │   │   │   ├── DeliveryDeduplicationService.ts
│   │   │   │   ├── RetrySchedulerService.ts
│   │   │   │   ├── ProviderFactory.ts    # Email/SMS/WhatsApp
│   │   │   │   └── EventPublisherService.ts
│   │   │   ├── repositories/
│   │   │   │   └── DeliveryRepository.ts # MySQL updates
│   │   │   └── models/
│   │   │       └── Delivery.ts
│   │   └── package.json
│   │
│   └── logger/                           # Kafka Consumer (Logging)
│       ├── src/
│       │   ├── index.ts                  # Kafka consumer entry
│       │   ├── controllers/
│       │   │   └── LogController.ts      # Log handler
│       │   ├── services/
│       │   │   ├── LogConsumerService.ts # Kafka consumer
│       │   │   └── LogPersistenceService.ts
│       │   ├── repositories/
│       │   │   └── LogRepository.ts      # Elasticsearch writer
│       │   └── models/
│       │       └── Log.ts
│       └── package.json
│
├── docker-compose.yml                    # Infra: Kafka, Redis, MySQL, ES, Kibana
├── postman-collection.json               # Complete API test suite
├── DESIGN_README.md                      # Technical design details
├── README.md                             # This file
├── package.json                          # Monorepo root
└── scripts/
    ├── init.sql                          # MySQL schema
    ├── migrate.js                        # DB migration
    └── health-check.js                   # Service health verification
```

---

## 🧪 Testing Guide

### Before You Test

Make sure services are running:

```bash
docker-compose ps
```

All containers should show `Up X seconds` or `healthy`.

---

### Test Design & Best Practices

The integration test suite uses several best practices for reliability:

#### 1. **Deterministic Test IDs**

Tests use a global counter to generate stable, reproducible IDs:

```javascript
// getTestId helper function generates: prefix-0, prefix-1, prefix-2, etc.
const testIdempotencyKey = getTestId("dedup");      // dedup-0
const testIdempotencyKey2 = getTestId("dedup");     // dedup-1
```

**Benefits:**
- Tests are reproducible across runs
- Easy to debug (consistent values)
- No flaky tests from random timestamps
- Clear test ID patterns in logs

#### 2. **Isolated Test Data**

Each test uses its own tenant/user IDs to prevent cross-test pollution:

```javascript
// Test 1: Uses tenant-dedup-1, user-dedup-1
// Test 2: Uses tenant-dedup-2, user-dedup-2
// Test 3: Uses tenant-dedup-multi, user-dedup-multi
```

#### 3. **Async/Await Pattern**

All async operations properly await results:

```javascript
const response = await makeRequest("POST", "/api/messages", body);
assertEquals(response.status, 202);
```

---

### Option 1: Integration Tests (Without Docker Containers)

Run tests directly in your terminal (fastest for development):

```bash
# Services must be running (see Quick Start section)
npm run test:integration
```

**Output:**

```
Running integration tests...
✓ Health check passed
✓ Message deduplication works
✓ Email delivery succeeded
✓ SMS delivery succeeded
✓ WhatsApp delivery succeeded
...
All tests passed ✅
```

---

### Option 2: Integration Tests (WITH Docker Containers)

Tests run inside Docker after all services initialize (recommended for CI/CD):

```bash
# Start everything including tests
docker-compose --profile test up

# OR using environment variable
RUN_TESTS=true docker-compose up
```

**This will:**

1. Start all infrastructure (Kafka, Redis, MySQL, ES, Kibana)
2. Start application services (Router, Aggregator, Logger)
3. Wait 10 seconds for services to initialize
4. Run full integration test suite with stable test IDs
5. Display results in docker-compose logs

**Watch test output:**

```bash
docker-compose --profile test logs -f integration-tests
```

**Cleanup:**

```bash
docker-compose --profile test down
```

---

### Using Postman Collection for Manual Testing

See **Postman Collection** section above for detailed API test scenarios.

---

## 📈 Monitoring & Debugging

### Health Endpoints

```bash
# Task Router health
curl http://localhost:3001/health

# Check all infrastructure
docker-compose ps

# View service logs
docker-compose logs -f task-router
docker-compose logs -f notification-aggregator
docker-compose logs -f logger
```

### Elasticsearch Queries

```bash
# Count total logs
curl http://localhost:9200/logs-*/_count | jq '.count'

# Search by traceId
curl -X POST http://localhost:9200/logs-*/_search \
  -H "Content-Type: application/json" \
  -d '{"query": {"match": {"traceId": "TRACE_ID_HERE"}}}'

# Aggregation by status
curl -X POST http://localhost:9200/logs-*/_search \
  -H "Content-Type: application/json" \
  -d '{
    "aggs": {
      "by_status": {
        "terms": {"field": "status.keyword"}
      }
    }
  }'
```

### Kafka Topic Inspection

```bash
# List all topics
docker exec notification-kafka kafka-topics --list --bootstrap-server=localhost:9092

# View topic details
docker exec notification-kafka kafka-topics --describe --topic messages.email --bootstrap-server=localhost:9092

# Consume from topic
docker exec notification-kafka kafka-console-consumer \
  --topic messages.email \
  --from-beginning \
  --bootstrap-server=localhost:9092
```

### Redis Inspection

```bash
# Connect to Redis CLI
docker exec -it notification-redis redis-cli

# View dedup keys
KEYS dedup:*

# View delivery dedup keys
KEYS delivered:*

# View retry queue
ZRANGE retries 0 -1 WITHSCORES

# View rate limit tokens
KEYS rate_limit:*
```

### MySQL Inspection

```bash
# Connect to MySQL
docker exec -it notification-mysql mysql -u notif_user -p notification_db

# View messages
SELECT messageId, userId, channel, status, attempts, createdAt FROM messages ORDER BY createdAt DESC LIMIT 10;

# View delivery attempts
SELECT messageId, attemptNumber, status, error FROM delivery_attempts ORDER BY attemptAt DESC LIMIT 20;

# View DLQ entries
SELECT messageId, channel, failureReason FROM dlq_entries ORDER BY createdAt DESC LIMIT 10;
```

---

## 🔐 Environment Configuration

### Default Values

```env
# Task Router
PORT=3001
REDIS_HOST=localhost
REDIS_PORT=6379
MYSQL_HOST=localhost
MYSQL_USER=notif_user
MYSQL_PASSWORD=notif-password
MYSQL_DATABASE=notification_db
KAFKA_BROKER=localhost:9092

# Notification Aggregator
PORT=3002
REDIS_HOST=localhost
MYSQL_HOST=localhost
KAFKA_BROKER=localhost:9092

# Logger
PORT=3003
ELASTICSEARCH_HOST=localhost
ELASTICSEARCH_PORT=9200
KAFKA_BROKER=localhost:9092

NODE_ENV=development
```

### Docker Override

Create `.env.docker` for containerized environment:

```env
KAFKA_BROKER=notification-kafka:29092
MYSQL_HOST=notification-mysql
REDIS_HOST=notification-redis
ELASTICSEARCH_HOST=notification-elasticsearch
```

---

## 🐳 Docker Management

### View Running Containers

```bash
# List all containers
docker-compose ps

# View logs from all services
docker-compose logs -f

# View logs from specific service
docker-compose logs -f kafka
docker-compose logs -f task-router
```

### Stop & Cleanup

```bash
# Stop all services (keeps data)
docker-compose stop

# Stop and remove containers (keeps data)
docker-compose down

# Stop, remove containers, AND remove all volumes (deletes all data!)
docker-compose down -v

# Remove unused Docker resources
docker system prune -a
```

### Restart Services

```bash
# Restart all services
docker-compose restart

# Restart specific service
docker-compose restart kafka
docker-compose restart mysql
```

### View Service Logs

```bash
# All logs
docker-compose logs -f

# Last 50 lines of all logs
docker-compose logs --tail=50

# Specific service
docker-compose logs -f notification-aggregator
```

---

## 📚 Additional Resources

- **API Documentation:** [postman-collection.json](./postman-collection.json)
- **Technical Design:** [DESIGN_README.md](./DESIGN_README.md)
- **Docker Setup:** [docker-compose.yml](./docker-compose.yml)
- **Database Schema:** [scripts/init.sql](./scripts/init.sql)

---

## ✅ Getting Started Checklist

### Immediate Startup (5 minutes)

- [ ] Clone repository: `git clone <repo-url>`
- [ ] Install: `npm install`
- [ ] Build: `npm run build`
- [ ] Start infrastructure: `docker-compose up -d`
- [ ] Start services: Open 3 terminals and run:
  - [ ] Terminal 1: `npm run start:router`
  - [ ] Terminal 2: `npm run start:aggregator`
  - [ ] Terminal 3: `npm run start:logger`
- [ ] Verify: `curl http://localhost:3001/health`
- [ ] Test: Send message via curl or Postman

### Full Validation

- [ ] Import Postman collection: `postman-collection.json`
- [ ] Run integration tests: `npm run test:integration`
- [ ] View logs: Open http://localhost:5601 (Kibana)
- [ ] Query database: `docker exec -it notification-mysql mysql -u notif_user -pnotif-password notification_db`
- [ ] Inspect Kafka: `docker exec notification-kafka kafka-topics --list --bootstrap-server=localhost:9092`

### Shutdown & Cleanup

- [ ] Stop services: Ctrl+C in each terminal
- [ ] Stop infrastructure: `docker-compose down`
- [ ] Clean everything: `docker-compose down -v`

---

## 🤝 Contributing

When working on this project:

1. **TypeScript:** Strict mode enabled
2. **Architecture:** Domain-Driven Design with clear separation
3. **Services:** Event-driven with Kafka
4. **Testing:** Integration tests required
5. **Documentation:** Update this README for new features

---

## 📝 License

MIT

---

**Last Updated:** January 2025
**System Status:** Production Ready ✅
