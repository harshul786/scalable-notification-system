FROM node:18-alpine

WORKDIR /app/services/task-router

COPY tsconfig.json /app/
COPY services/task-router/package*.json ./
COPY services/task-router/tsconfig.json ./

RUN npm install

COPY services/task-router/src ./src

RUN npm run build

EXPOSE 3001

CMD ["node", "dist/index.js"]
