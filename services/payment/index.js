require("dotenv").config();
const { connect, publish, subscribe } = require("/shared/rabbitmq");
const {
  INVENTORY_RESERVED,
  PAYMENT_CAPTURED,
  PAYMENT_FAILED,
} = require("/shared/events");

const EXCHANGE = "orders";
const RABBITMQ_URL = process.env.RABBITMQ_URL;

async function processPayment(channel, payload) {
  const { orderId, total, customer_id } = payload;
  console.log(`Processing payment for order ${orderId}, amount: ${total}`);

  await new Promise((resolve) => setTimeout(resolve, 500));

  const success = Math.random() < 0.9;

  if (success) {
    console.log(`Payment captured for order ${orderId}`);
    await publish(channel, EXCHANGE, PAYMENT_CAPTURED, {
      orderId,
      customer_id,
      amount: total,
    });
  } else {
    console.log(`Payment failed for order ${orderId}`);
    await publish(channel, EXCHANGE, PAYMENT_FAILED, {
      orderId,
      customer_id,
      amount: total,
      reason: "Card declined",
    });
  }
}

async function main() {
  const channel = await connect(RABBITMQ_URL);

  await subscribe(
    channel,
    EXCHANGE,
    "payment.inventory.reserved",
    INVENTORY_RESERVED,
    async (payload) => {
      console.log("Processing INVENTORY_RESERVED:", payload.orderId);
      await processPayment(channel, payload);
    }
  );

  console.log("Payment service started");
}

main().catch((err) => {
  console.error("Fatal error:", err.message);
  process.exit(1);
});
