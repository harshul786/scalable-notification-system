# 📡 Notification Aggregator System - Technical Design Document

A production-grade 4-microservice communication routing platform built with **Node.js + TypeScript + Kafka + Redis + MySQL + Elasticsearch**.

This system receives messages from clients, routes them to the correct communication channel (Email, SMS, WhatsApp), ensures that each message is delivered exactly once (effectively), retries failed deliveries with exponential backoff, and provides full observability via Elasticsearch + Kibana.

**Built using:** Clean event-driven architecture with Domain-Driven Design (DDD) and SOLID principles.

---

## 🏗️ System Architecture

### 4-Microservice Architecture

The system is composed of **4 independently running microservices**, each responsible for a clear part of the flow:

1. **Task Router Service**

- Exposes REST API → POST /messages
- Validates request payloads
- Generates: messageId, idempotencyKey, traceId
- Performs ingress dedup via Redis cache (72h TTL):
  ```
  SETNX idem:tenant:{tenantId}:key:{idempotencyKey} <cached_response> EX 259200
  ```
- Publishes messages to Kafka unconditionally (Topic: messages.{channel}, Key: userId)
- Emits structured logs to Logger Service
- Does NOT write to MySQL (async processing in consumer)
- Stateless, horizontally scalable entry point

2. **Notification Aggregator Service (Delivery Workers with Batch Processing)**

- Consumes from Kafka topics in batches:
  ```
  messages.email (3 partitions)
  messages.sms (3 partitions)
  messages.whatsapp (3 partitions)
  ```

**Responsibilities:**

- **Batch bulk DB idempotency check:**
  ```
  SELECT * FROM messages WHERE (tenantId, idempotencyKey) IN (...)
  ```
  → Single query per batch, not N per-message
- **Partitions batch** into NEW vs DUPLICATE messages
- **Atomic insert** of NEW messages with status=IN_PROGRESS
- **Skips DUPLICATE** messages entirely (no side-effects)
- Stores tracking metadata & attempts in MySQL
- Applies:
  - Rate-limiting (Redis token bucket)
  - Circuit breaker (Redis)
- Calls provider APIs with idempotency tokens (messageId)
- On success: Updates status to SENT
- On failure: Schedules retry in Redis ZSET
- On max attempts: Publishes to dlq.{channel}
- Emits logs to Logger Service
- Commits Kafka offsets only after all processing completes
- Runs as a scalable group:
  ```
  consumer-group: delivery-workers-group
  ```

3. **Logger Service**

- Single consumer of logs topic
- Topic is single-partition to preserve total ordering
- Writes structured logs to Elasticsearch
- Visualizes message traces, retries, failures, rate-limit events, and DLQs in Kibana
  ```
  consumer-group: logger-group (1 instance only)
  ```

## 🧠 End-to-End Message Flow

- Client → Task Router (POST /messages)
- Router validates request
- Router computes idempotencyKey and checks Redis cache (72h TTL)
- **If Redis HIT:** Returns DUPLICATE, does NOT enqueue
- **If Redis MISS:** Publishes to Kafka unconditionally
- Delivery Worker consumes message batch
- **Bulk DB check:** One query for entire batch's idempotency keys
- **Partition batch:** NEW (needs processing) vs DUPLICATE (skip)
- **Atomic insert:** NEW messages with IN_PROGRESS status
- Worker processes only NEW messages (calls providers)
- Worker skips DUPLICATE messages (no side-effects)
- Worker logs metadata & attempts to MySQL
- Worker calls provider with idempotency token (messageId)
- On success: UPDATE status = SENT
- On failure: ZADD to Redis retry ZSET
- On max attempts: publish to DLQ
- All services stream logs → Logger → Elasticsearch → Kibana

🔑 Idempotency (DB-Anchored with Soft Cache)

**Ingress Cache (Task Router - Fast Path)**
Prevents duplicate API requests with Redis cache (72h TTL):

```
SETNX idem:tenant:{tenantId}:key:{idempotencyKey} <cached_response> EX 259200
└─ Hit: Returns DUPLICATE, does NOT enqueue to Kafka
└─ Miss: Publishes to Kafka unconditionally
└─ Safe-fail: Returns true on Redis errors, lets DB handle dedup
```

**Batch DB Idempotency Check (Aggregator - Authoritative)**
Single bulk query per batch, MySQL is source of truth:

```
SELECT * FROM messages WHERE (tenantId, idempotencyKey) IN (...)
└─ UNIQUE constraint prevents duplicate rows
└─ Partitions batch into NEW (for processing) vs DUPLICATE (skip)
└─ Atomic insert of NEW messages with status=IN_PROGRESS
└─ Database is truth even if Redis fails or crashes
```

**Delivery Cache (Aggregator - Protection Layer)**
Prevents duplicate provider calls during retries:

```
SETNX delivered:{dedupKey} 1 EX 86400
└─ Warmed only AFTER DB commit succeeds
└─ Protects against Kafka redelivery scenarios
└─ TTL: 24 hours (covers retry window)
```

**Provider Idempotency Tokens**
Send messageId to provider for their idempotency check:

```
provider.send(recipient, body, {idempotencyToken: messageId})
└─ Provider can deduplicate on their side
└─ Safe to retry even if network fails mid-call
└─ Industry standard pattern
```

**Key Insight:** MySQL is authority, Redis is optimization

If this fails → skip provider call → commit offset.
This guarantees exactly-once effect for each user message.

🗝 Kafka Partitioning (UserID-based)

Every message uses:

```
partitionKey = userId
```

This ensures:

- All messages for one user → same Kafka partition
- Delivery ordering is always preserved
- Parallelism scales across users

Worker pool scales horizontally via:

```
consumer-group: delivery-workers-group
```

**Topics:**

```
messages.email      → 3 partitions
messages.sms        → 3 partitions
messages.whatsapp   → 3 partitions
logs                → 1 partition (global ordering)
dlq.*               → 1 partition (simple operator flow)
```

## ⏱ Retry Strategy

Kafka doesn't support delayed messages natively, so we use **Redis Sorted Sets** as a distributed scheduler:

### Retry Mechanism

```
ZADD retries <nextRetryAt> <payload>
```

The **Retry Scheduler** service runs continuously:

1. **Poll Phase:** Every 100ms, query Redis ZSET

   ```
   ZRANGEBYSCORE retries 0 <now>
   ```

   Finds all messages due for retry

2. **Republish Phase:** For each due message

   - Increment attempt counter
   - Republish to Kafka `messages.{channel}`
   - Include attempt number in message headers

3. **Escalation Phase:** If max attempts exceeded
   - Publish to DLQ topic: `dlq.{channel}`
   - Insert into MySQL `dlq_entries` table
   - Manual operator intervention required

### Exponential Backoff Strategy

```
Attempt 1: Immediate (0 seconds)
Attempt 2: 1 second delay
Attempt 3: 10 second delay
Attempt 4: 30 second delay
Attempt 5: 5 minutes delay
Attempt 6+: Escalate to DLQ
```

### Why This Approach?

| Aspect                | Kafka Native     | Redis ZSET (Chosen)       |
| --------------------- | ---------------- | ------------------------- |
| Delayed delivery      | ❌ Not supported | ✅ Native support         |
| Sub-second resolution | -                | ✅ Millisecond precision  |
| Distributed scheduler | -                | ✅ Single source of truth |
| Simple queries        | -                | ✅ O(log N) operations    |
| Idempotent republish  | -                | ✅ Can safely retry       |

## 🗃️ Database Schema & Storage

### MySQL Tables (Persistent Audit Trail)

The database stores complete audit trail and message metadata:

#### **messages Table**

Primary message tracking:

```sql
CREATE TABLE messages (
  messageId VARCHAR(36) PRIMARY KEY,
  dedupKey VARCHAR(128) UNIQUE NOT NULL,
  userId VARCHAR(128) NOT NULL,
  tenantId VARCHAR(128) NOT NULL,
  channel ENUM('email', 'sms', 'whatsapp') NOT NULL,
  recipient VARCHAR(255) NOT NULL,
  body TEXT NOT NULL,
  metadata JSON,
  status ENUM('PENDING', 'SENT', 'FAILED') DEFAULT 'PENDING',
  finalDelivered BOOLEAN DEFAULT FALSE,
  attempts INT DEFAULT 0,
  createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX(userId, channel),
  INDEX(dedupKey),
  INDEX(createdAt)
);
```

#### **delivery_attempts Table**

Complete retry history:

```sql
CREATE TABLE delivery_attempts (
  id INT AUTO_INCREMENT PRIMARY KEY,
  messageId VARCHAR(36) NOT NULL,
  attemptNumber INT NOT NULL,
  status ENUM('SUCCESS', 'FAILED') NOT NULL,
  error VARCHAR(500),
  providerResponse TEXT,
  attemptAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (messageId) REFERENCES messages(messageId),
  INDEX(messageId),
  INDEX(attemptAt)
);
```

#### **dlq_entries Table**

Dead-letter queue inspection:

```sql
CREATE TABLE dlq_entries (
  id INT AUTO_INCREMENT PRIMARY KEY,
  messageId VARCHAR(36) NOT NULL UNIQUE,
  channel ENUM('email', 'sms', 'whatsapp') NOT NULL,
  failureReason TEXT,
  maxAttemptsReached BOOLEAN DEFAULT TRUE,
  createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (messageId) REFERENCES messages(messageId),
  INDEX(channel),
  INDEX(createdAt)
);
```

### Redis Keys (Fast Caching & Scheduling)

#### **Deduplication Cache**

```
dedup:{dedupKey} → messageId
TTL: 3600 seconds (1 hour)
Purpose: Ingress deduplication
Operation: SETNX (atomic set-if-not-exists)
```

#### **Delivery Deduplication**

```
delivered:{dedupKey} → 1
TTL: None (permanent)
Purpose: Prevent duplicate provider calls from Kafka redelivery
Operation: SETNX (atomic set-if-not-exists)
```

#### **Rate Limiting Token Bucket**

```
rate_limit:{provider} → tokens_remaining
TTL: Varies (refill rate per provider)
Purpose: Enforce max messages per second per provider
Operations: INCRBY, PEXPIRE

Rate Limits:
- Email: 100 msgs/sec
- SMS: 50 msgs/sec
- WhatsApp: 30 msgs/sec
```

#### **Retry Queue (Sorted Set)**

```
retries (ZSET)
└─ Score: nextRetryTimestamp (Unix milliseconds)
└─ Member: messageId:channel:attempt

Examples:
ZADD retries 1700000100000 "msg-123:email:1"
ZADD retries 1700000110000 "msg-456:sms:2"
ZADD retries 1700000130000 "msg-789:whatsapp:3"

Poll Query: ZRANGEBYSCORE retries 0 {now}
```

### Data Flow Through Storage

```
┌─────────────────┐
│ Task Router     │
├─────────────────┤
│ 1. Check Redis  │
│    dedup key    │
└────┬────────────┘
     │
     ├─ Exists? → Return DUPLICATE
     │
     └─ New? ↓

┌─────────────────────┐
│ MySQL: INSERT       │
├─────────────────────┤
│ messages table      │
│ status=PENDING      │
└────┬────────────────┘
     │
     ↓
┌──────────────────────┐
│ Redis: SET dedup key │
├──────────────────────┤
│ SETNX dedup:{key}    │
│ EX 3600              │
└────┬─────────────────┘
     │
     ↓ Success

┌────────────────────┐
│ Publish to Kafka   │
└────────────────────┘
     │
     ↓
┌──────────────────────────┐
│ Notification Aggregator  │
├──────────────────────────┤
│ 1. Check Redis delivery  │
│    dedup key             │
└────┬─────────────────────┘
     │
     ├─ Exists? → Skip provider, commit offset
     │
     └─ New? ↓

┌──────────────────────────┐
│ 2. Call Provider API     │
├──────────────────────────┤
│ Email/SMS/WhatsApp       │
└────┬─────────────────────┘
     │
     ├─ Success? ↓
     │
     │  ┌───────────────────────┐
     │  │ MySQL: UPDATE + INSERT│
     │  ├───────────────────────┤
     │  │ messages.status=SENT  │
     │  │ delivery_attempts     │
     │  └───────────────────────┘
     │
     └─ Failure? ↓

┌──────────────────────────┐
│ Redis: ZADD retries      │
├──────────────────────────┤
│ Score: nextRetryTime     │
│ Member: msg:channel:att  │
└──────────────────────────┘
     │
     ↓
┌──────────────────────────┐
│ Retry Scheduler          │
├──────────────────────────┤
│ Poll every 100ms         │
│ Republish due messages   │
│ to Kafka                 │
└──────────────────────────┘
     │
     ├─ Max attempts exceeded? ↓
     │
     │  ┌───────────────────────┐
     │  │ DLQ: Publish + Insert │
     │  ├───────────────────────┤
     │  │ dlq.{channel} topic   │
     │  │ dlq_entries table     │
     │  └───────────────────────┘
```

## 📊 Logging & Observability

Each service writes structured logs:

```
service
traceId
spanId
parentSpanId
messageId
userId
dedupKey
channel
status
attempt
timestamp
```

**Logger Service:**

- Consumes from logs topic (1 partition)
- Writes into Elasticsearch
- Visualizable in Kibana using:
  - Trace timelines
  - Delivery attempts
  - Failure heatmaps
  - DLQ dashboards

## 🧪 Example Request

**Request**

```json
{
  "tenantId": "t1",
  "userId": "u123",
  "idempotencyKey": "req-789",
  "channel": "email",
  "recipient": "user@example.com",
  "body": "Welcome!",
  "metadata": { "priority": "high" }
}
```

**Response**

```json
{
  "messageId": "uuid-xyz",
  "dedupKey": "xxh64-hash",
  "traceId": "trace-abc",
  "status": "ACCEPTED"
}
```

## 📁 Project Structure & Implementation

```
notification-aggregator/
├── services/
│   ├── task-router/                           # REST API Service (Port 3001)
│   │   ├── src/
│   │   │   ├── index.ts                       # Express.js entry point
│   │   │   ├── controllers/
│   │   │   │   └── MessageController.ts       # HTTP request handler
│   │   │   │       └── Methods:
│   │   │   │           • createMessage(req, res)
│   │   │   │           • validateRequest(request)
│   │   │   │
│   │   │   ├── services/
│   │   │   │   ├── HashService.ts             # xxhash64 dedup key generation
│   │   │   │   ├── DeduplicationService.ts    # Redis dedup logic
│   │   │   │   └── MessagePublisher.ts        # Kafka publisher
│   │   │   │
│   │   │   ├── repositories/
│   │   │   │   └── MessageRepository.ts       # MySQL message insertion
│   │   │   │
│   │   │   ├── routes/
│   │   │   │   └── messageRoutes.ts           # Express route definitions
│   │   │   │
│   │   │   └── models/
│   │   │       ├── Message.ts                 # Message entity
│   │   │       └── CreateMessageRequest.ts    # Request DTO
│   │   │
│   │   ├── package.json
│   │   └── Dockerfile
│   │
│   ├── notification-aggregator/               # Kafka Consumer Service
│   │   ├── src/
│   │   │   ├── index.ts                       # Kafka consumer entry point
│   │   │   ├── controllers/
│   │   │   │   └── DeliveryController.ts      # Message handler
│   │   │   │       └── Methods:
│   │   │   │           • handleMessage(message)
│   │   │   │
│   │   │   ├── services/
│   │   │   │   ├── DeliveryProcessorService.ts
│   │   │   │   │   └── Methods:
│   │   │   │   │       • processDelivery(message)
│   │   │   │   │       • handleSuccess(message)
│   │   │   │   │       • handleFailure(message)
│   │   │   │   │
│   │   │   │   ├── DeliveryDeduplicationService.ts
│   │   │   │   │   └── Methods:
│   │   │   │   │       • checkAndSetDelivered(dedupKey)
│   │   │   │   │
│   │   │   │   ├── RetrySchedulerService.ts
│   │   │   │   │   └── Methods:
│   │   │   │   │       • getPendingRetries()
│   │   │   │   │       • scheduleRetry(message, delay)
│   │   │   │   │
│   │   │   │   ├── ProviderFactory.ts
│   │   │   │   │   └── Providers:
│   │   │   │   │       • EmailProvider (80% success)
│   │   │   │   │       • SMSProvider (85% success)
│   │   │   │   │       • WhatsAppProvider (90% success)
│   │   │   │   │
│   │   │   │   └── EventPublisherService.ts
│   │   │   │       └── Methods:
│   │   │   │           • publishLog(log)
│   │   │   │           • publishRetryMessage(message)
│   │   │   │           • publishDLQMessage(message)
│   │   │   │
│   │   │   ├── repositories/
│   │   │   │   └── DeliveryRepository.ts      # MySQL updates
│   │   │   │
│   │   │   └── models/
│   │   │       └── Delivery.ts                # Delivery entity
│   │   │
│   │   ├── package.json
│   │   └── Dockerfile
│   │
│   ├── logger/                                # Logger Service (Port 3003)
│   │   ├── src/
│   │   │   ├── index.ts                       # Kafka consumer entry point
│   │   │   ├── controllers/
│   │   │   │   └── LogController.ts           # Log handler
│   │   │   │       └── Methods:
│   │   │   │           • handleLog(log)
│   │   │   │
│   │   │   ├── services/
│   │   │   │   ├── LogConsumerService.ts      # Kafka consumer
│   │   │   │   │   └── Methods:
│   │   │   │   │       • consume(topics, callback)
│   │   │   │   │       • consumeMultiple(topics, callback)
│   │   │   │   │
│   │   │   │   └── LogPersistenceService.ts   # Persistence logic
│   │   │   │       └── Methods:
│   │   │   │           • persistLog(log)
│   │   │   │
│   │   │   ├── repositories/
│   │   │   │   └── LogRepository.ts           # Elasticsearch writer
│   │   │   │       └── Methods:
│   │   │   │           • save(log)
│   │   │   │           • search(query)
│   │   │   │
│   │   │   ├── domain/
│   │   │   │   ├── entities/
│   │   │   │   │   └── Log.ts                 # Log entity
│   │   │   │   ├── interfaces/
│   │   │   │   │   └── index.ts               # ILogRepository, ILogConsumer
│   │   │   │   └── usecases/
│   │   │   │       └── PersistLogUseCase.ts   # Use case logic
│   │   │   │
│   │   │   └── models/
│   │   │       └── Log.ts                     # StructuredLog DTO
│   │   │
│   │   ├── package.json
│   │   └── Dockerfile
│   │
│   └── test/                                  # Test Service (Optional)
│       ├── Dockerfile
│       └── README.md
│
├── docker-compose.yml                         # Infrastructure orchestration
│   ├── Services:
│   │   ├── zookeeper (Kafka coordination)
│   │   ├── kafka (Message broker)
│   │   ├── kafka-init (Topic creation)
│   │   ├── redis (Caching & scheduling)
│   │   ├── mysql (Persistent storage)
│   │   ├── elasticsearch (Log storage)
│   │   ├── kibana (Visualization)
│   │   ├── task-router (REST API)
│   │   ├── notification-aggregator (Worker)
│   │   └── logger (Log consumer)
│   │
│   └── Networks:
│       └── notification-net (Internal communication)
│
├── postman-collection.json                    # API testing suite
├── DESIGN_README.md                           # This file
├── README.md                                  # Quick start guide
├── package.json                               # Monorepo configuration
├── tsconfig.json                              # TypeScript configuration
│
└── scripts/
    ├── init.sql                               # MySQL schema initialization
    ├── migrate.js                             # Database migrations
    ├── health-check.js                        # Service health verification
    └── verify-kafka-setup.sh                  # Kafka topic verification
```

### Service Responsibilities Breakdown

| Service                     | Pattern                   | Language   | Port | Role                              |
| --------------------------- | ------------------------- | ---------- | ---- | --------------------------------- |
| **Task Router**             | REST API (Express)        | TypeScript | 3001 | Entry point, HTTP handling, dedup |
| **Notification Aggregator** | Kafka Consumer            | TypeScript | N/A  | Message delivery, retry logic     |
| **Retry Scheduler**         | Background Job (embedded) | TypeScript | N/A  | Retry orchestration from Redis    |
| **Logger**                  | Kafka Consumer            | TypeScript | 3003 | Log persistence to Elasticsearch  |

---

## 🔐 Security & Idempotency Guarantees

### DB-Anchored Exactly-Once Delivery Semantics

The system implements **four layers of idempotency** to guarantee exactly-once delivery with MySQL as authority and Redis as optimization:

#### Layer 1: Ingress Cache (Task Router - Fast Path)

```
Redis SETNX idem:tenant:{tenantId}:key:{idempotencyKey} <cached_response> EX 259200 (72h)
```

- Prevents duplicate API requests with sub-millisecond cache hit
- Returns `DUPLICATE` status without enqueuing to Kafka
- Safe-fail: If Redis unavailable, lets request through (DB will catch)
- **Cost:** 72-hour Redis TTL balances memory vs coverage
- **Hit rate:** 90%+ for typical retry patterns

#### Layer 2: Batch Bulk DB Idempotency Check (Aggregator - Authoritative)

```
SELECT * FROM messages WHERE (tenantId, idempotencyKey) IN (...)
│
├─ UNIQUE constraint: (tenantId, idempotencyKey) prevents duplicate rows
├─ Bulk query: One query per batch (not N per-message)
├─ Partitions: NEW (for processing) vs DUPLICATE (skip)
└─ Atomic insert: NEW messages with status=IN_PROGRESS
```

- Single source of truth for idempotency
- Scales efficiently: O(log N) for 1000+ item batches
- Database is protected by ACID transactions
- **Cost:** One bulk query per batch instead of N per-message queries

#### Layer 3: Delivery Cache (Aggregator - Retry Protection)

```
Redis SETNX delivered:{dedupKey} 1 EX 86400 (24h)
```

- Prevents duplicate provider calls during Kafka redelivery
- Warmed **only AFTER** DB commit succeeds (safe-first pattern)
- Covers exponential backoff window (0s, 1s, 10s, 30s, 5m)
- **Cost:** 24-hour TTL, sparse memory (only delivered messages)

#### Layer 4: Provider Idempotency Tokens

```
provider.send(recipient, body, {idempotencyToken: messageId, attemptNumber})
```

- Send messageId as idempotency key to provider APIs
- Provider can deduplicate on their side
- Safe to retry even if network fails mid-call
- **Industry standard:** PayPal, Stripe, Twilio all use this pattern

#### Offset Commit Ordering

- Only commit Kafka offset **after** all DB and cache operations complete
- If crash during write, Kafka redelivers on restart
- Prevents "committed but not written" scenarios

### Reasoning Behind Four Layers

| Scenario                            | Layer 1    | Layer 2    | Layer 3    | Layer 4    | Protection    |
| ----------------------------------- | ---------- | ---------- | ---------- | ---------- | ------------- |
| Duplicate client request            | ✅ Catches | -          | -          | -          | ✅            |
| Kafka redelivery (same partition)   | -          | ✅ Catches | -          | -          | ✅            |
| Redis crash (cache loss)            | ⚠️ Risk    | ✅ Catches | -          | -          | ✅            |
| Provider call succeeds, DB fails    | -          | -          | ✅ Catches | -          | ✅            |
| Provider receives duplicate request | -          | -          | -          | ✅ Catches | ✅            |
| All systems fail simultaneously     | ⚠️ Risk    | ⚠️ Risk    | ✅ CATCHES | -          | ✅ (Eventual) |

**Key Insight:** Each layer handles different failure modes. Layer 2 (DB) is the ultimate authority that survives any infrastructure failure.

---

## 📊 Performance Characteristics

### Latency Breakdown

````
Task Router Request → Response: ~10-50ms
├─ Input validation: ~1ms
├─ Dedup key generation: ~0.1ms
├─ Redis SETNX: ~2-5ms
├─ Kafka publish: ~5-30ms
└─ Serialize response: ~1ms

Batch Processing (Aggregator): ~100-500ms per batch
├─ Kafka batch consume: ~2-5ms
├─ Bulk DB idempotency query: ~5-15ms (1000+ items)
├─ Batch partition (NEW vs DUPLICATE): ~1-2ms
├─ Atomic INSERT IN_PROGRESS: ~10-20ms
├─ Provider API calls (NEW only): ~50-300ms per message
├─ MySQL UPDATE status=SENT: ~5-10ms per message
├─ Kafka publish logs: ~10-30ms per batch
├─ Offset commit: ~2-5ms
└─ Total per batch: ~200-500ms
```└─ Offset commit: ~5-10ms
````

### Throughput Targets

**Per Service (Single Instance):**

- Task Router: 5,000+ msgs/sec (HTTP throughput limited by client)
- Aggregator: 1,000 msgs/sec (provider API simulation)
- Logger: 10,000 msgs/sec (ES indexing)

**Kafka Topics (3 partitions):**

- messages.email: ~3,000 msgs/sec
- messages.sms: ~1,500 msgs/sec
- messages.whatsapp: ~1,000 msgs/sec

**Scaling:**

- Add more Aggregator workers → scales linearly with partition count
- Kafka partitions limit parallelism per channel
- Increase from 3 to 12 partitions for 4x throughput per channel

---

## 🚀 Scalability Patterns

### Horizontal Scaling

#### Task Router (Stateless)

```
┌─────────────┐
│ Load        │
│ Balancer    │
└─────┬───────┘
      │
    ┌─┴───┬───────┬───────┐
    │     │       │       │
┌───▼──┐┌─▼──┐ ┌─▼──┐ ┌──▼──┐
│Router││Rout││Rout││Rout│
│Pod 1 ││ 2  ││ 3  ││ 4  │
└──┬───┘└────┘ └────┘ └─────┘
   │
   └─→ Shared Redis (dedup)
   └─→ Shared MySQL (messages)
   └─→ Shared Kafka (topics)
```

**Pros:**

- Stateless design
- Add/remove pods without coordination
- Load balancer distributes requests

#### Aggregator (Consumer Group)

```
┌────────────────────────────────────┐
│ Kafka Consumer Group                │
│ delivery-workers-group              │
├────────────────────────────────────┤
│                                    │
│  ┌──────┐  ┌──────┐  ┌──────┐    │
│  │Pod 1 │  │Pod 2 │  │Pod 3 │    │
│  │Owns: │  │Owns: │  │Owns: │    │
│  │Part.0│  │Part.1│  │Part.2│    │
│  └──────┘  └──────┘  └──────┘    │
│                                    │
└────────────────────────────────────┘
     │
     ├─→ messages.email (3 partitions)
     ├─→ messages.sms (3 partitions)
     └─→ messages.whatsapp (3 partitions)
```

**Auto-scaling:**

- When pod crashes, Kafka reassigns partitions
- Other pods pick up work automatically
- No manual coordination needed

#### Logger (Singleton)

```
┌─────────────────────────────────────┐
│ Logger Pod (1 instance)             │
│ Consumes logs topic (1 partition)   │
├─────────────────────────────────────┤
│                                     │
│  logs (1 partition) ──→ Elasticsearch│
│  errors (1 partition) ──→ ES        │
│                                     │
└─────────────────────────────────────┘
```

**Rationale:**

- 1 partition = Total message ordering
- Prevents log gaps or duplicates
- Single consumer ensures consistency
- If pod crashes, logs queue in Kafka until restart

---

## 🧪 Failure Recovery Scenarios

### Scenario 1: Task Router Crashes During Request Processing

**State:** Request received, but before Kafka publish

```
Action: Crash after MySQL INSERT, before Kafka publish
Result: Message marked PENDING in MySQL, not in Kafka queue

Recovery:
1. Pod restarts
2. Client retries request
3. Redis dedup detects duplicate (key still exists)
4. Returns DUPLICATE status (HTTP 200)
5. Message eventually times out from retry scheduler

Issue: Message never gets delivered
Solution: Implement cleanup job to process stale PENDING messages
```

### Scenario 2: Aggregator Crashes During Provider Call

**State:** Provider call in progress

```
Action: Crash after provider succeeds, before MySQL UPDATE
Result: Message delivered by provider, but marked PENDING in DB

Recovery:
1. Pod restarts
2. Kafka redelivers message (offset not committed)
3. Redis delivery dedup check PASSES (key not set)
4. Provider call succeeds again (idempotent)
5. MySQL UPDATE succeeds
6. Offset committed

Result: Message appears sent twice, but only charged once
Solution: Provider must be idempotent (safe)
```

### Scenario 3: Redis Cache Corruption

**State:** Redis node fails, loses all data

```
Action: Redis crash and data loss
Result: dedup and delivery keys lost in memory

Recovery:
1. Redis restarts (ephemeral, no persistence)
2. Keys empty
3. Ingress dedup: Some duplicates may slip through
4. Delivery dedup: Some provider calls may be duplicated
5. Backup: Database finalDelivered flag catches duplicates

Result: Some duplicate deliveries, but DB prevents marking twice
Solution: Acceptable trade-off (duplicate calls better than lost messages)
```

### Scenario 4: MySQL Crashes During Status Update

**State:** Aggregator has confirmed success, updating DB

```
Action: MySQL crashes before UPDATE commits
Result: Message marked as delivered, still marked PENDING in DB

Recovery:
1. MySQL restarts
2. Aggregator retries offset commit fails
3. Kafka redelivers message
4. Delivery dedup key already exists → skip provider
5. MySQL update succeeds

Result: Correct state eventually

Solution: Delivery dedup key prevents duplicate provider calls
```

### Scenario 5: Kafka Broker Dies

**State:** Services cannot produce or consume messages

```
Action: Kafka broker crashes
Result: All services queue in memory or fail

Recovery:
1. Restart Kafka
2. Services reconnect
3. Consumed offsets still available
4. Services resume from last committed offset
5. Pending messages reprocessed

Result: No message loss, potential duplicates
```

---

## 📊 Kafka Partitioning Strategy

### Why Partition by userId?

```
partitionKey = userId
```

**Alternative Strategies:**

| Strategy                        | Pros               | Cons                        |
| ------------------------------- | ------------------ | --------------------------- |
| **Round-robin**                 | Load balanced      | No ordering                 |
| **Message ID** (Chosen: userId) | Per-user ordering  | May load-imbalance          |
| **Channel-based**               | Channel separation | No parallelism per user     |
| **Random**                      | Simple             | No ordering, load balancing |

**Decision:** userId partitioning provides:

- Per-user message ordering ✅
- Horizontal scalability across users ✅
- Load balancing across users (typically good) ✅
- Prevents head-of-line blocking per user ✅

**Example:**

```
messages.email (3 partitions)

Partition 0 (hash(user123) % 3 == 0):
├─ Message 1 (user123)
├─ Message 2 (user456)
└─ Message 3 (user789)

Partition 1 (hash(user111) % 3 == 1):
├─ Message 4 (user111)
├─ Message 5 (user222)
└─ Message 6 (user333)

Partition 2 (hash(user999) % 3 == 2):
├─ Message 7 (user999)
├─ Message 8 (user888)
└─ Message 9 (user777)
```

**Guarantees:**

- All user123 messages in Partition 0 (strict ordering)
- Multiple partitions processed in parallel (3x throughput)
- Aggregator pods scale independently per partition

---

## 📘 High-Level Diagram (HLD)

                    ┌──────────────────────────┐
                    │          CLIENT          │
                    └─────────────┬────────────┘
                                  │ POST /messages
                                  ▼
                    ┌──────────────────────────┐
                    │      TASK ROUTER         │
                    │ - validate payload       │
                    │ - xxhash dedupKey        │
                    │ - Redis SETNX dedupe     │
                    │ - Publish to Kafka       │
                    │ - Logs → Logger          │
                    └─────────────┬────────────┘
                                  │ key=userId
                                  ▼
                         ┌───────────────────┐
                         │       KAFKA       │
                         │ msg.email (12p)   │
                         │ msg.sms   (12p)   │
                         │ msg.whatsapp(12p) │
                         │ logs       (1p)   │
                         └────────┬──────────┘
                                  │
                                  ▼
       ┌──────────────────────────────────────────────────────────────┐
       │         NOTIFICATION AGGREGATOR (Delivery Workers)           │
       │     consumer-group: delivery-workers-group                   │
       │ - Redis SETNX delivered:{key} (idempotency)                  │
       │ - MySQL metadata & attempts                                  │
       │ - Rate limiting & circuit breaker                            │
       │ - Retry scheduler (Redis ZSET)                               │
       │ - Publish DLQ on max attempts                                │
       │ - Logs → Logger                                              │
       └───────────────┬──────────────────────────────────────────────┘
                       │
                       ▼
                    MySQL

                            Logs
                              │
                              ▼
                ┌──────────────────────────┐
                │       LOGGER SERVICE     │
                │ consumer-group: logger   │
                │ logs topic (1 partition) │
                │ → Elasticsearch → Kibana │
                └──────────────────────────┘

## 📐 Low-Level Diagram (LLD)

Detailed step-by-step message processing:

```
┌─────────────────────────────────────────────────────────────────┐
│ TASK ROUTER REQUEST FLOW                                        │
└─────────────────────────────────────────────────────────────────┘

Client Request
  │
  └─→ POST /api/messages
       │
       ├─ Validate Input
       │  ├─ Check required fields (tenantId, userId, channel, etc.)
       │  ├─ Validate channel enum (email|sms|whatsapp)
       │  ├─ Validate email format
       │  └─ Check body not empty
       │
       ├─ Generate IDs
       │  ├─ messageId = UUID v4
       │  ├─ dedupKey = xxhash64(body + userId + recipient + tenantId)
       │  └─ traceId = UUID v4
       │
       ├─ Redis Dedup Check
       │  └─ SETNX dedup:{dedupKey} <messageId> EX 3600
       │     ├─ EXISTS? → Return DUPLICATE (HTTP 200)
       │     └─ NEW?    → Continue
       │
       ├─ MySQL Insert
       │  └─ INSERT messages (
       │        messageId, dedupKey, userId, tenantId,
       │        channel, recipient, body, metadata,
       │        status='PENDING', finalDelivered=false,
       │        attempts=0, createdAt=NOW(), updatedAt=NOW()
       │      )
       │
       ├─ Kafka Publish
       │  └─ Topic: messages.{channel}
       │     Key: {userId}  ← Partitioning strategy
       │     Headers: {traceId, spanId, messageId, attempt}
       │     Value: {messageId, dedupKey, userId, channel, recipient, body, ...}
       │
       ├─ Emit Log
       │  └─ Publish to logs topic:
       │     {service: "router", status: "ACCEPTED", traceId, messageId, ...}
       │
       └─→ Response HTTP 202 ACCEPTED
            {
              "messageId": "...",
              "dedupKey": "...",
              "traceId": "...",
              "status": "ACCEPTED"
            }


┌─────────────────────────────────────────────────────────────────┐
│ KAFKA BROKER - PARTITION SELECTION                              │
└─────────────────────────────────────────────────────────────────┘

messages.email (3 partitions)
  │
  ├─ Partition 0: hash(userId) % 3 == 0
  ├─ Partition 1: hash(userId) % 3 == 1
  └─ Partition 2: hash(userId) % 3 == 2

Ensures: All messages from user-123 always go to same partition
Result: Per-user message ordering guaranteed


┌─────────────────────────────────────────────────────────────────┐
│ NOTIFICATION AGGREGATOR CONSUMPTION FLOW                        │
└─────────────────────────────────────────────────────────────────┘

Consumer Group: delivery-workers-group
Subscribe To: messages.email, messages.sms, messages.whatsapp
  │
  └─→ eachBatch Handler (Kafka callback)
       │
       ├─ Receive batch of messages
       │
       ├─ For each message:
       │  │
       │  ├─ Parse message JSON
       │  │
       │  ├─ Extract headers (attempt, traceId)
       │  │
       │  ├─ Redis Delivery Dedup
       │  │  └─ SETNX delivered:{dedupKey} 1
       │  │     ├─ EXISTS? → Skip provider call
       │  │     │            ├─ Insert delivery_attempts (SKIPPED)
       │  │     │            ├─ Emit log (SKIPPED)
       │  │     │            └─ Continue to next message
       │  │     └─ NEW?    → Continue
       │  │
       │  ├─ Verify DB Status
       │  │  └─ SELECT finalDelivered FROM messages WHERE messageId
       │  │     ├─ Already sent? → Skip provider
       │  │     └─ Not sent?     → Continue
       │  │
       │  ├─ Rate Limiting
       │  │  └─ Redis Token Bucket: rate_limit:{provider}
       │  │     ├─ Email: 100 tokens/sec
       │  │     ├─ SMS:   50 tokens/sec
       │  │     └─ WhatsApp: 30 tokens/sec
       │  │        If depleted → Queue locally, wait for refill
       │  │
       │  ├─ Circuit Breaker Check
       │  │  └─ If failure_rate > 50% in last 100 attempts:
       │  │     ├─ Circuit OPEN
       │  │     └─ Fail fast without calling provider
       │  │
       │  ├─ Provider API Call
       │  │  ├─ Get provider from ProviderFactory
       │  │  ├─ Call provider.send(recipient, body)
       │  │  └─ Simulate with:
       │  │     ├─ Email: 80% success
       │  │     ├─ SMS:   85% success
       │  │     └─ WhatsApp: 90% success
       │  │
       │  ├─ On SUCCESS:
       │  │  │
       │  │  ├─ MySQL Update
       │  │  │  └─ UPDATE messages SET
       │  │  │       status = 'SENT',
       │  │  │       finalDelivered = true,
       │  │  │       attempts = {attempt},
       │  │  │       updatedAt = NOW()
       │  │  │     WHERE messageId = ...
       │  │  │
       │  │  ├─ MySQL Insert
       │  │  │  └─ INSERT delivery_attempts (
       │  │  │       messageId, attemptNumber,
       │  │  │       status='SUCCESS', error=null,
       │  │  │       providerResponse='{...}',
       │  │  │       attemptAt=NOW()
       │  │  │     )
       │  │  │
       │  │  ├─ Emit Log
       │  │  │  └─ Publish to logs topic:
       │  │  │     {status: "SENT", traceId, messageId, ...}
       │  │  │
       │  │  └─ Commit Offset
       │  │     └─ resolveOffset(message.offset)
       │  │        heartbeat()
       │  │
       │  └─ On FAILURE:
       │     │
       │     ├─ Calculate Backoff
       │     │  └─ backoff = [1s, 10s, 30s, 5m, 5m][attempt]
       │     │
       │     ├─ Redis ZSET Schedule
       │     │  └─ ZADD retries
       │     │       (now + backoff) * 1000  ← milliseconds
       │     │       "messageId:channel:attempt+1"
       │     │
       │     ├─ MySQL Insert
       │     │  └─ INSERT delivery_attempts (
       │     │       messageId, attemptNumber,
       │     │       status='FAILED',
       │     │       error='Provider returned error: ...',
       │     │       providerResponse='{...}',
       │     │       attemptAt=NOW()
       │     │     )
       │     │
       │     ├─ Emit Log
       │     │  └─ Publish to logs topic:
       │     │     {status: "FAILED", traceId, messageId, attempt, ...}
       │     │
       │     └─ DO NOT Commit Offset
       │        └─ Kafka will redeliver on timeout
       │           (Prevents message loss on crash)


┌─────────────────────────────────────────────────────────────────┐
│ RETRY SCHEDULER BACKGROUND JOB                                  │
└─────────────────────────────────────────────────────────────────┘

Continuous Polling (every 100ms)
  │
  ├─ Query Redis ZSET: ZRANGEBYSCORE retries 0 <now>
  │  └─ Finds all messages with nextRetryAt <= now
  │
  ├─ For each due retry:
  │  │
  │  ├─ Parse messageId:channel:attempt
  │  │
  │  ├─ Increment attempt counter
  │  │
  │  ├─ Republish to Kafka
  │  │  └─ Topic: messages.{channel}
  │  │     Key: {userId}
  │  │     Headers: {attempt: attempt+1}
  │  │     Value: {...same message...}
  │  │
  │  ├─ Remove from ZSET
  │  │  └─ ZREM retries "messageId:channel:attempt"
  │  │
  │  ├─ Check max attempts
  │  │  └─ If attempt >= 5:
  │  │     │
  │  │     ├─ Publish to DLQ
  │  │     │  └─ Topic: dlq.{channel}
  │  │     │     Key: {messageId}
  │  │     │     Value: {...message..., failureReason, attempts}
  │  │     │
  │  │     ├─ MySQL Insert DLQ Entry
  │  │     │  └─ INSERT dlq_entries (
       │  │        messageId, channel,
       │  │        failureReason='Max attempts exceeded',
       │  │        maxAttemptsReached=true,
       │  │        createdAt=NOW()
       │  │      )
       │  │
       │  │  ├─ MySQL Update Message
       │  │  │  └─ UPDATE messages SET status='FAILED' WHERE messageId=...
       │  │  │
       │  │  └─ Emit Log
       │  │     └─ Publish to logs topic:
       │  │        {status: "DLQ", traceId, messageId, ...}
       │  │


┌─────────────────────────────────────────────────────────────────┐
│ LOGGER SERVICE LOG CONSUMPTION                                  │
└─────────────────────────────────────────────────────────────────┘

Consumer Group: logger-group (1 instance only)
Subscribe To: logs (1 partition), errors (1 partition)
  │
  └─→ Message Handler
       │
       ├─ Receive log message
       │
       ├─ Parse StructuredLog JSON
       │
       ├─ Validate log structure
       │
       ├─ Elasticsearch Index Document
       │  └─ Index: logs-YYYY.MM.DD  ← Time-based partitioning
       │     Document ID: Auto-generated
       │     Body: {service, traceId, messageId, userId, channel, status, ...}
       │
       ├─ Commit Offset
       │  └─ Guarantees ordering (1 partition)
       │
       └─→ Available in Kibana
            ├─ Search by traceId (full journey)
            ├─ Filter by status (SENT/FAILED/DUPLICATE)
            ├─ Aggregations (by channel, by status, by service)
            └─ Timelines and heatmaps
```
