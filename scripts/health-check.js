#!/usr/bin/env node

/**
 * Health check script - verifies all services are running
 */

const http = require("http");
const { Client } = require("redis");

async function checkService(url, name) {
  return new Promise((resolve) => {
    const urlObj = new URL(url);
    const options = {
      hostname: urlObj.hostname,
      port: urlObj.port,
      path: "/",
      method: "GET",
      timeout: 5000,
    };

    const req = http.request(options, (res) => {
      resolve({
        name,
        status: res.statusCode === 200 ? "✓" : "✗",
        code: res.statusCode,
      });
    });

    req.on("error", (error) => {
      resolve({ name, status: "✗", error: error.message });
    });

    req.on("timeout", () => {
      resolve({ name, status: "✗", error: "timeout" });
      req.destroy();
    });

    req.end();
  });
}

async function checkRedis() {
  return new Promise((resolve) => {
    const client = Client.createClient({
      url: "redis://localhost:6379",
      socket: { timeout: 5000 },
    });

    client
      .connect()
      .then(() => {
        resolve({ name: "Redis", status: "✓", port: 6379 });
      })
      .catch((error) => {
        resolve({ name: "Redis", status: "✗", error: error.message });
      })
      .finally(() => {
        client.quit().catch(() => {});
      });
  });
}

async function checkElasticsearch() {
  return new Promise((resolve) => {
    const options = {
      hostname: "localhost",
      port: 9200,
      path: "/",
      method: "GET",
      timeout: 5000,
    };

    const req = http.request(options, (res) => {
      if (res.statusCode === 200) {
        let data = "";
        res.on("data", (chunk) => {
          data += chunk;
        });
        res.on("end", () => {
          try {
            const json = JSON.parse(data);
            resolve({
              name: "Elasticsearch",
              status: "✓",
              version: json.version?.number,
            });
          } catch {
            resolve({ name: "Elasticsearch", status: "✓", port: 9200 });
          }
        });
      } else {
        resolve({ name: "Elasticsearch", status: "✗", code: res.statusCode });
      }
    });

    req.on("error", (error) => {
      resolve({ name: "Elasticsearch", status: "✗", error: error.message });
    });

    req.on("timeout", () => {
      resolve({ name: "Elasticsearch", status: "✗", error: "timeout" });
      req.destroy();
    });

    req.end();
  });
}

async function runHealthCheck() {
  console.log("\n🏥 System Health Check\n");
  console.log("Checking services...\n");

  const results = await Promise.all([
    checkService("http://localhost:3001/health", "Task Router"),
    checkService("http://localhost:5601/", "Kibana"),
    checkRedis(),
    checkElasticsearch(),
  ]);

  let healthy = 0;
  for (const result of results) {
    const status = result.status === "✓" ? "✅" : "❌";
    const extra = result.version
      ? ` (${result.version})`
      : result.error
      ? ` - ${result.error}`
      : "";
    console.log(`${status} ${result.name}${extra}`);
    if (result.status === "✓") healthy++;
  }

  console.log(`\nStatus: ${healthy}/${results.length} services healthy\n`);

  if (healthy === 4) {
    console.log("✨ All systems operational! Ready to send messages.\n");
    console.log("Try: curl -X POST http://localhost:3001/api/messages ...\n");
    process.exit(0);
  } else {
    console.log(
      "⚠️  Some services are not responding. Check docker-compose logs.\n"
    );
    process.exit(1);
  }
}

runHealthCheck().catch(console.error);
