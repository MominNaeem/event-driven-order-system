require("dotenv").config();
const { pool, init } = require("./db");
const { connect, publish, subscribe } = require("/shared/rabbitmq");
const {
  ORDER_CREATED,
  ORDER_CANCELLED,
  INVENTORY_RESERVED,
  INVENTORY_FAILED,
} = require("/shared/events");

const EXCHANGE = "orders";
const RABBITMQ_URL = process.env.RABBITMQ_URL;

async function reserveInventory(channel, payload) {
  const { orderId, items, customer_id, total } = payload;
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    for (const item of items) {
      const result = await client.query(
        "SELECT quantity, reserved FROM inventory WHERE product_id = $1 FOR UPDATE",
        [item.product_id]
      );

      if (result.rows.length === 0) {
        await client.query("ROLLBACK");
        console.log(`Product not found: ${item.product_id} for order ${orderId}`);
        await publish(channel, EXCHANGE, INVENTORY_FAILED, {
          orderId,
          reason: `Product ${item.product_id} not found`,
        });
        return;
      }

      const { quantity, reserved } = result.rows[0];
      const available = quantity - reserved;

      if (available < item.quantity) {
        await client.query("ROLLBACK");
        console.log(`Insufficient stock for ${item.product_id}: need ${item.quantity}, available ${available}`);
        await publish(channel, EXCHANGE, INVENTORY_FAILED, {
          orderId,
          reason: `Insufficient stock for product ${item.product_id}`,
        });
        return;
      }
    }

    for (const item of items) {
      await client.query(
        "UPDATE inventory SET reserved = reserved + $1 WHERE product_id = $2",
        [item.quantity, item.product_id]
      );
      await client.query(
        "INSERT INTO reservations (order_id, product_id, quantity) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
        [orderId, item.product_id, item.quantity]
      );
    }

    await client.query("COMMIT");
    console.log(`Inventory reserved for order ${orderId}`);
    await publish(channel, EXCHANGE, INVENTORY_RESERVED, {
      orderId,
      customer_id,
      items,
      total,
    });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("Reserve error:", err.message);
    await publish(channel, EXCHANGE, INVENTORY_FAILED, {
      orderId,
      reason: "Internal inventory error",
    });
  } finally {
    client.release();
  }
}

async function releaseReservations(orderId) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await client.query(
      "SELECT product_id, quantity FROM reservations WHERE order_id = $1",
      [orderId]
    );
    for (const row of result.rows) {
      await client.query(
        "UPDATE inventory SET reserved = reserved - $1 WHERE product_id = $2",
        [row.quantity, row.product_id]
      );
    }
    await client.query("DELETE FROM reservations WHERE order_id = $1", [orderId]);
    await client.query("COMMIT");
    console.log(`Reservations released for order ${orderId}`);
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("Release error:", err.message);
  } finally {
    client.release();
  }
}

async function main() {
  await init();

  const channel = await connect(RABBITMQ_URL);

  await subscribe(channel, EXCHANGE, "inventory.order.created", ORDER_CREATED, async (payload) => {
    console.log("Processing ORDER_CREATED:", payload.orderId);
    await reserveInventory(channel, payload);
  });

  await subscribe(channel, EXCHANGE, "inventory.order.cancelled", ORDER_CANCELLED, async (payload) => {
    console.log("Processing ORDER_CANCELLED:", payload.orderId);
    await releaseReservations(payload.orderId);
  });

  console.log("Inventory service started");
}

main().catch((err) => {
  console.error("Fatal error:", err.message);
  process.exit(1);
});
