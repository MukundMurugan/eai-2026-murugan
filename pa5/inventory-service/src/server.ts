
import type { ConsumeMessage } from "amqplib";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "inventory.queue";
const RESULTS_EXCHANGE = "results.inventory";
const MAX_RETRIES = Number(process.env.MAX_RETRIES) || 3;
const FAIL_RATE = Number(process.env.INVENTORY_FAIL_RATE) || 10;

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);
  await channel.prefetch(1);

  console.log(`[Inventory] Consuming from ${QUEUE}, fail rate: ${FAIL_RATE}%`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const correlationId = msg.properties.headers?.["correlationId"] as string | undefined;
    const retryCount = getRetryCount(msg);

    let order: CanonicalOrder;

    try {
      order = JSON.parse(msg.content.toString()) as CanonicalOrder;
    } catch {
      channel.publish("orders.invalid.exchange", "", msg.content, {
        headers: msg.properties.headers,
        contentType: msg.properties.contentType,
        persistent: true,
      });
      channel.ack(msg);
      console.log("[Inventory] Invalid JSON routed to orders.invalid");
      return;
    }

    console.log(`[Inventory] Processing ${correlationId} (attempt ${retryCount + 1})`);

    try {
      if (Math.random() * 100 < FAIL_RATE) {
        throw new Error("Simulated inventory failure");
      }

      channel.ack(msg);
      channel.publish(
        RESULTS_EXCHANGE,
        "",
        Buffer.from(JSON.stringify({
          correlationId,
          source: "inventory",
          status: "success",
          timestamp: new Date().toISOString(),
          details: { message: `Stock checked for order ${order.orderId}` },
        })),
        {
          headers: { correlationId },
          contentType: "application/json",
          persistent: true,
        },
      );
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);

      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(process.env.DLQ_EXCHANGE || "orders.dlq.exchange", "", msg.content, {
          headers: {
            ...msg.properties.headers,
            originalQueue: QUEUE,
          },
  contentType: msg.properties.contentType,
  persistent: true,
});
        channel.ack(msg);
        console.log(`[Inventory] -> DLQ after ${retryCount + 1} attempts: ${error}`);
      } else {
        channel.nack(msg, false, false);
        console.log(`[Inventory] -> Retry (attempt ${retryCount + 1}): ${error}`);
      }
    }
  });
}

main().catch((err: unknown) => {
  console.error("[Inventory] Startup failed:", err);
  process.exit(1);
});
