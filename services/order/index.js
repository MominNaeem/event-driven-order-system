require("dotenv").config();
const express = require("express");
const { v4: uuidv4 } = require("uuid");
const { pool, init } = require("./db");
const {
  connect,
  publish,
  subscribe,
} = require("/shared/rabbitmq");
const {
  ORDER_CREATED,
  PAYMENT_CAPTURED,
  PAYMENT_FAILED,
  ORDER_FULFILLED,
  ORDER_CANCELLED,
} = require("/shared/events");

const EXCHANGE = "orders";
const PORT = process.env.PORT || 3001;
const RABBITMQ_URL = process.env.RABBITMQ_URL;

const app = express();
app.use(express.json());

let channel;

app.post("/orders", async (req, res) => {
  const { customer_id, items } = req.body;
  if (!customer_id || !Array.isArray(items) || items.length === 0) {
    return res.status(400).json({ error: "customer_id and items are required" });
  }

  const total = items.reduce((sum, item) => sum + item.price * item.quantity, 0);
  const orderId = uuidv4();

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    await client.query(
      "INSERT INTO orders (id, customer_id, items, total, status) VALUES ($1, $2, $3, $4, $5)",
      [orderId, customer_id, JSON.stringify(items), total, "PENDING"]
    );

    const eventPayload = { orderId, customer_id, items, total };
    await client.query(
      "INSERT INTO outbox (event_type, payload) VALUES ($1, $2)",
      [ORDER_CREATED, JSON.stringify(eventPayload)]
    );

    await client.query("COMMIT");
    res.status(201).json({ orderId, status: "PENDING", total });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("Order creation failed:", err.message);
    res.status(500).json({ error: "Failed to create order" });
  } finally {
    client.release();
  }
});

app.get("/orders/:id", async (req, res) => {
  const { id } = req.params;
  const result = await pool.query("SELECT * FROM orders WHERE id = $1", [id]);
  if (result.rows.length === 0) {
    return res.status(404).json({ error: "Order not found" });
  }
  res.json(result.rows[0]);
});

async function startOutboxRelay() {
  setInterval(async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query(
        "SELECT * FROM outbox WHERE published = FALSE ORDER BY created_at ASC LIMIT 10 FOR UPDATE SKIP LOCKED"
      );
      for (const row of result.rows) {
        await publish(channel, EXCHANGE, row.event_type, row.payload);
        await client.query("UPDATE outbox SET published = TRUE WHERE id = $1", [row.id]);
      }
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      console.error("Outbox relay error:", err.message);
    } finally {
      client.release();
    }
  }, 1000);
}

async function main() {
  await init();

  channel = await connect(RABBITMQ_URL);

  await subscribe(channel, EXCHANGE, "order.payment.captured", PAYMENT_CAPTURED, async (payload) => {
    console.log("Payment captured for order:", payload.orderId);
    await pool.query("UPDATE orders SET status = $1 WHERE id = $2", [
      "FULFILLED",
      payload.orderId,
    ]);
    await publish(channel, EXCHANGE, ORDER_FULFILLED, payload);
  });

  await subscribe(channel, EXCHANGE, "order.payment.failed", PAYMENT_FAILED, async (payload) => {
    console.log("Payment failed for order:", payload.orderId);
    await pool.query("UPDATE orders SET status = $1 WHERE id = $2", [
      "CANCELLED",
      payload.orderId,
    ]);
    await publish(channel, EXCHANGE, ORDER_CANCELLED, payload);
  });

  await startOutboxRelay();

  app.listen(PORT, () => {
    console.log(`Order service listening on port ${PORT}`);
  });
}

main().catch((err) => {
  console.error("Fatal error:", err.message);
  process.exit(1);
});
