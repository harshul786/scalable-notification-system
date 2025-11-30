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
- Generates: messageId, dedupKey (xxhash64), traceId
- Performs ingress deduplication via:
  ```
  SETNX dedup:{dedupKey} <messageId> EX 1h
  ```
- Publishes messages to Kafka (Topic: messages.{channel}, Key: userId)
- Emits structured logs to Logger Service
- Does NOT write to MySQL
- Stateless, horizontally scalable entry point

2. **Notification Aggregator Service (Delivery Workers)**

- Consumes from Kafka topics:
  ```
  messages.email (3 partitions)
  messages.sms (3 partitions)
  messages.whatsapp (3 partitions)
  ```

**Responsibilities:**

- Delivery idempotency using Redis:
  ```
  SETNX delivered:{dedupKey} 1
  ```
  → prevents duplicate side-effects
- Stores tracking metadata & attempts in MySQL
- Applies:
  - Rate-limiting (Redis token bucket)
  - Circuit breaker (Redis)
- Simulates provider calls (Email/SMS/WhatsApp)
- On failure: Adds retry entry to Redis ZSET
- On max attempts: Publishes to dlq.{channel}
- Emits logs to Logger Service
- Commits Kafka offsets only after DB write succeeds or duplicate skip completes
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
- Router computes dedupKey and performs Redis dedupe
- Router publishes to Kafka with key = userId
- Delivery Worker consumes message
- Delivery Worker performs Redis delivery-dedupe
- Worker logs metadata & attempts to MySQL
- Worker simulates sending to provider
- On failure → schedule Retry via Redis ZSET
- On max attempts → publish to DLQ
- All services stream logs → Logger → Elasticsearch → Kibana

🔑 Idempotency (Pure Redis Model)
Ingress Dedupe (Task Router)
Prevents API bursts from creating duplicate messages:

```
SETNX dedup:{dedupKey} <messageId> EX 3600
```

Delivery Dedupe (Aggregator)
Prevents duplicate side-effects from Kafka redelivery:

```
SETNX delivered:{dedupKey} 1
```

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

### Exactly-Once Delivery Semantics

The system implements **three levels of deduplication** to guarantee exactly-once delivery:

#### Level 1: Ingress Deduplication (Task Router)

```
Redis SETNX dedup:{dedupKey} <messageId> EX 3600
```

- Prevents duplicate API requests
- 1-hour TTL covers typical API retry patterns
- Returns `DUPLICATE` status for retried requests
- **Cost:** Minimal Redis memory

#### Level 2: Delivery Deduplication (Aggregator)

```
Redis SETNX delivered:{dedupKey} 1
```

- Prevents duplicate provider calls from Kafka redelivery
- Permanent key (no TTL) - never forget delivery
- If exists, skip provider call entirely
- **Cost:** Permanent Redis memory (acceptable for message volume)

#### Level 3: Database Verification (Aggregator)

```
SELECT finalDelivered FROM messages WHERE messageId
```

- Triple-check before marking as SENT
- Database is source of truth
- ACID compliance ensures consistency
- **Cost:** Single MySQL query per delivery attempt

#### Level 4: Offset Commit Ordering

- Only commit Kafka offset **after** all writes complete
- If crash during write, Kafka redelivers on restart
- Prevents "committed but not written" scenarios

### Reasoning Behind Three Levels

| Scenario                         | Level 1    | Level 2    | Level 3    | Protection |
| -------------------------------- | ---------- | ---------- | ---------- | ---------- |
| Duplicate client request         | ✅ Catches | -          | -          | ✅         |
| Kafka broker crash + replay      | -          | ✅ Catches | -          | ✅         |
| Redis crash                      | ⚠️ Risk    | ⚠️ Risk    | ✅ Catches | ✅         |
| Provider call succeeds, DB fails | -          | -          | ✅ Catches | ✅         |

---

## 📊 Performance Characteristics

### Latency Breakdown

```
Task Router Request → Response: ~50-100ms
├─ Input validation: ~1ms
├─ xxhash64 computation: ~0.1ms
├─ Redis SETNX: ~2-5ms
├─ MySQL INSERT: ~10-20ms
├─ Kafka publish: ~10-30ms
└─ Serialize response: ~1-2ms

Message Processing (Aggregator): ~100-500ms
├─ Kafka consume: ~1-2ms
├─ Redis delivery dedup: ~2-5ms
├─ Rate limiting check: ~1-2ms
├─ Provider API call: ~50-300ms (simulated)
├─ MySQL UPDATE/INSERT: ~20-50ms
├─ Kafka publish logs: ~10-30ms
└─ Offset commit: ~5-10ms
```

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
