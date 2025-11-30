FROM node:20-alpine

WORKDIR /app

# Copy root package files
COPY package*.json tsconfig.json ./

# Copy test files
COPY tests/ ./tests/
COPY scripts/ ./scripts/

# Install dependencies
RUN npm ci --prefer-offline --no-audit

# Set healthcheck (optional - tests might take time)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=5 \
  CMD node -e "console.log('test container running')"

# Run tests
CMD ["npm", "run", "test:integration"]
