-- Create messages table
CREATE TABLE IF NOT EXISTS messages (
  messageId VARCHAR(36) PRIMARY KEY,
  dedupKey VARCHAR(128) NOT NULL UNIQUE,
  userId VARCHAR(128) NOT NULL,
  tenantId VARCHAR(128) NOT NULL,
  channel ENUM('email', 'sms', 'whatsapp') NOT NULL,
  recipient VARCHAR(255) NOT NULL,
  body LONGTEXT NOT NULL,
  status ENUM('PENDING', 'SENT', 'FAILED') DEFAULT 'PENDING',
  finalDelivered BOOLEAN DEFAULT FALSE,
  attempts INT DEFAULT 0,
  createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_user_channel (userId, channel),
  INDEX idx_dedup (dedupKey),
  INDEX idx_created (createdAt),
  INDEX idx_status (status)
);

-- Create delivery_attempts table
CREATE TABLE IF NOT EXISTS delivery_attempts (
  id INT AUTO_INCREMENT PRIMARY KEY,
  messageId VARCHAR(36) NOT NULL,
  attemptNumber INT NOT NULL,
  status ENUM('SUCCESS', 'FAILED') NOT NULL,
  error VARCHAR(500),
  providerResponse LONGTEXT,
  attemptAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (messageId) REFERENCES messages(messageId) ON DELETE CASCADE,
  INDEX idx_message (messageId),
  INDEX idx_attempt_time (attemptAt)
);

-- Create dlq_entries table
CREATE TABLE IF NOT EXISTS dlq_entries (
  id INT AUTO_INCREMENT PRIMARY KEY,
  messageId VARCHAR(36) NOT NULL,
  channel ENUM('email', 'sms', 'whatsapp') NOT NULL,
  failureReason LONGTEXT,
  maxAttemptsReached BOOLEAN DEFAULT TRUE,
  createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (messageId) REFERENCES messages(messageId) ON DELETE CASCADE,
  INDEX idx_channel (channel),
  INDEX idx_created (createdAt)
);
