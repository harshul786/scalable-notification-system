FROM node:18-alpine

WORKDIR /app/services/notification-aggregator

COPY tsconfig.json /app/
COPY services/notification-aggregator/package*.json ./
COPY services/notification-aggregator/tsconfig.json ./

RUN npm install

COPY services/notification-aggregator/src ./src

RUN npm run build

EXPOSE 3002

CMD ["node", "dist/index.js"]
