#!/usr/bin/env node

/**
 * Database migration script
 * Runs initial schema setup
 */

require("dotenv").config();
const mysql = require("mysql2/promise");
const fs = require("fs");
const path = require("path");

async function migrate() {
  const connection = await mysql.createConnection({
    host: process.env.MYSQL_HOST || "localhost",
    user: process.env.MYSQL_USER || "root",
    password: process.env.MYSQL_PASSWORD || "root-password",
    database: process.env.MYSQL_DATABASE || "notification_db",
    multipleStatements: true,
  });

  try {
    const sqlPath = path.join(__dirname, "init.sql");
    const sql = fs.readFileSync(sqlPath, "utf8");

    console.log("Executing migrations...");
    await connection.query(sql);
    console.log("✓ Migrations completed successfully");
  } catch (error) {
    console.error("✗ Migration failed:", error.message);
    process.exit(1);
  } finally {
    await connection.end();
  }
}

migrate();
