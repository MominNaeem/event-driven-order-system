require("dotenv").config();
const { connect, subscribe } = require("/shared/rabbitmq");
const {
  ORDER_FULFILLED,
  ORDER_CANCELLED,
  PAYMENT_FAILED,
} = require("/shared/events");

const EXCHANGE = "orders";
const RABBITMQ_URL = process.env.RABBITMQ_URL;

async function main() {
  const channel = await connect(RABBITMQ_URL);

  await subscribe(
    channel,
    EXCHANGE,
    "notification.order.fulfilled",
    ORDER_FULFILLED,
    async (payload) => {
      console.log(
        `[EMAIL] Order confirmed — To: customer ${payload.customer_id}, Order: ${payload.orderId}, Amount: $${payload.amount}`
      );
    }
  );

  await subscribe(
    channel,
    EXCHANGE,
    "notification.order.cancelled",
    ORDER_CANCELLED,
    async (payload) => {
      console.log(
        `[EMAIL] Order cancelled — To: customer ${payload.customer_id}, Order: ${payload.orderId}, Reason: ${payload.reason || "N/A"}`
      );
    }
  );

  await subscribe(
    channel,
    EXCHANGE,
    "notification.payment.failed",
    PAYMENT_FAILED,
    async (payload) => {
      console.log(
        `[EMAIL] Payment failed — To: customer ${payload.customer_id}, Order: ${payload.orderId}, Reason: ${payload.reason}`
      );
    }
  );

  console.log("Notification service started");
}

main().catch((err) => {
  console.error("Fatal error:", err.message);
  process.exit(1);
});
