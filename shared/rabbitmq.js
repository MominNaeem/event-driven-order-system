const amqp = require("amqplib");

async function connect(url) {
  const connection = await amqp.connect(url);
  const channel = await connection.createChannel();
  return channel;
}

async function publish(channel, exchange, routingKey, payload) {
  await channel.assertExchange(exchange, "topic", { durable: true });
  channel.publish(
    exchange,
    routingKey,
    Buffer.from(JSON.stringify(payload)),
    { persistent: true }
  );
}

async function subscribe(channel, exchange, queue, routingKey, handler) {
  await channel.assertExchange(exchange, "topic", { durable: true });
  await channel.assertQueue(queue, { durable: true });
  await channel.bindQueue(queue, exchange, routingKey);
  channel.prefetch(1);
  channel.consume(queue, async (msg) => {
    if (!msg) return;
    try {
      const payload = JSON.parse(msg.content.toString());
      await handler(payload);
      channel.ack(msg);
    } catch (err) {
      console.error("Handler error:", err.message);
      channel.nack(msg, false, false);
    }
  });
}

module.exports = { connect, publish, subscribe };
