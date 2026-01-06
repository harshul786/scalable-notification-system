-- Create messages table (DB-anchored idempotency with Redis as fast cache)
CREATE TABLE IF NOT EXISTS messages (
  messageId VARCHAR(36) PRIMARY KEY,
  idempotencyKey VARCHAR(255) NOT NULL,
  tenantId VARCHAR(128) NOT NULL,
  dedupKey VARCHAR(128) NOT NULL,
  userId VARCHAR(128) NOT NULL,
  channel ENUM('email', 'sms', 'whatsapp') NOT NULL,
  recipient VARCHAR(255) NOT NULL,
  body LONGTEXT NOT NULL,
  
  -- Status machine: PENDING -> IN_PROGRESS -> SENT/FAILED/RETRY_SCHEDULED
  status ENUM('PENDING', 'IN_PROGRESS', 'SENT', 'FAILED', 'RETRY_SCHEDULED') DEFAULT 'PENDING',
  
  -- Idempotency tracking (DB is source of truth)
  finalDelivered BOOLEAN DEFAULT FALSE,
  attemptCount INT DEFAULT 0,
  maxAttempts INT DEFAULT 4,
  
  -- Provider interaction
  lastProviderResponse LONGTEXT,
  lastProviderError VARCHAR(500),
  lastAttemptAt TIMESTAMP NULL,
  
  -- Timing and tracking
  createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  
  -- Indexes for fast lookups
  UNIQUE KEY uk_tenant_idempotency (tenantId, idempotencyKey),
  UNIQUE KEY uk_dedup (dedupKey),
  INDEX idx_user_channel (userId, channel),
  INDEX idx_status (status),
  INDEX idx_created (createdAt),
  INDEX idx_delivery_status (finalDelivered, status)
);

-- Create delivery_attempts table (full audit trail)
CREATE TABLE IF NOT EXISTS delivery_attempts (
  id INT AUTO_INCREMENT PRIMARY KEY,
  messageId VARCHAR(36) NOT NULL,
  attemptNumber INT NOT NULL,
  status ENUM('SUCCESS', 'FAILED') NOT NULL,
  providerResponse LONGTEXT,
  providerError VARCHAR(500),
  providerCode VARCHAR(50),
  attemptAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  FOREIGN KEY (messageId) REFERENCES messages(messageId) ON DELETE CASCADE,
  INDEX idx_message (messageId),
  INDEX idx_attempt_time (attemptAt),
  INDEX idx_message_attempt (messageId, attemptNumber)
);

-- Create dlq_entries table (dead letter queue for max-attempt failures)
CREATE TABLE IF NOT EXISTS dlq_entries (
  id INT AUTO_INCREMENT PRIMARY KEY,
  messageId VARCHAR(36) NOT NULL,
  tenantId VARCHAR(128) NOT NULL,
  channel ENUM('email', 'sms', 'whatsapp') NOT NULL,
  failureReason LONGTEXT,
  finalAttemptNumber INT,
  maxAttemptsReached BOOLEAN DEFAULT TRUE,
  manualReviewRequired BOOLEAN DEFAULT TRUE,
  createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  FOREIGN KEY (messageId) REFERENCES messages(messageId) ON DELETE CASCADE,
  INDEX idx_channel (channel),
  INDEX idx_created (createdAt),
  INDEX idx_tenant (tenantId)
);
