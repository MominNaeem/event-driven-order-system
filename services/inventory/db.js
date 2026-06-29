require("dotenv").config();
const { Pool } = require("pg");

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

async function init() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS inventory (
      product_id VARCHAR(255) PRIMARY KEY,
      quantity INTEGER NOT NULL DEFAULT 0,
      reserved INTEGER NOT NULL DEFAULT 0
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS reservations (
      order_id UUID NOT NULL,
      product_id VARCHAR(255) NOT NULL,
      quantity INTEGER NOT NULL,
      PRIMARY KEY (order_id, product_id)
    )
  `);

  await pool.query(`
    INSERT INTO inventory (product_id, quantity, reserved)
    VALUES
      ('PROD-001', 100, 0),
      ('PROD-002', 50, 0),
      ('PROD-003', 25, 0)
    ON CONFLICT (product_id) DO NOTHING
  `);
}

module.exports = { pool, init };
