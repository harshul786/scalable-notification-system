FROM node:18-alpine

WORKDIR /app/services/logger

COPY tsconfig.json /app/
COPY services/logger/package*.json ./
COPY services/logger/tsconfig.json ./

RUN npm install

COPY services/logger/src ./src

RUN npm run build

EXPOSE 3003

CMD ["node", "dist/index.js"]
