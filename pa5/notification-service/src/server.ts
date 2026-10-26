
import type { ConsumeMessage } from "amqplib";
import { appendFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "notifications.queue";
const RESULTS_EXCHANGE = "results.notification";
const MAX_RETRIES = Number(process.env.MAX_RETRIES) || 3;
const DATA_DIR = "/data";
const PROCESSED_FILE = `${DATA_DIR}/processed-ids.json`;
const LOG_FILE = `${DATA_DIR}/notification.log`;

async function main(): Promise<void> {
  await mkdir(DATA_DIR, { recursive: true });

  let processedIds: string[] = [];

  try {
    processedIds = JSON.parse(await readFile(PROCESSED_FILE, "utf8")) as string[];
  } catch {
    await writeFile(PROCESSED_FILE, "[]", "utf8");
  }

  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);
  await channel.prefetch(1);

  console.log(`[Notification] Consuming from ${QUEUE}`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const headers = msg.properties.headers ?? {};
    const correlationId = String(headers["correlationId"] ?? "");
    const retryCount = getRetryCount(msg);

    let order: CanonicalOrder;

    try {
      order = JSON.parse(msg.content.toString()) as CanonicalOrder;
    } catch {
      channel.publish("orders.invalid.exchange", "", msg.content, {
        headers,
        contentType: msg.properties.contentType,
        persistent: true,
      });
      channel.ack(msg);
      console.log("[Notification] Invalid JSON routed to orders.invalid");
      return;
    }

    if (processedIds.includes(correlationId)) {
      console.log(`[Notification] Duplicate skipped: ${correlationId}`);
      channel.ack(msg);
      return;
    }

    try {
      const entry = {
        correlationId,
        orderId: order.orderId,
        customerEmail: order.customer.email,
        timestamp: new Date().toISOString(),
        message: "Order received",
      };

      await appendFile(LOG_FILE, `${JSON.stringify(entry)}\n`, "utf8");

      processedIds.push(correlationId);
      await writeFile(PROCESSED_FILE, JSON.stringify(processedIds, null, 2), "utf8");

      channel.publish(
        RESULTS_EXCHANGE,
        "",
        Buffer.from(JSON.stringify({
          correlationId,
          source: "notification",
          status: "success",
          timestamp: new Date().toISOString(),
          details: { message: `Notification recorded for order ${order.orderId}` },
        })),
        {
          headers: { correlationId },
          contentType: "application/json",
          persistent: true,
        },
      );

      channel.ack(msg);
      console.log(`[Notification] Recorded ${correlationId}`);
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);

      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(
          process.env.DLQ_EXCHANGE || "orders.dlq.exchange",
          "",
          msg.content,
          {
            headers: { ...headers, originalQueue: QUEUE },
            contentType: msg.properties.contentType,
            persistent: true,
          },
        );
        channel.ack(msg);
        console.log(`[Notification] -> DLQ after ${retryCount + 1} attempts: ${error}`);
      } else {
        channel.nack(msg, false, false);
        console.log(`[Notification] -> Retry (attempt ${retryCount + 1}): ${error}`);
      }
    }
  });
}

main().catch((err: unknown) => {
  console.error("[Notification] Startup failed:", err);
  process.exit(1);
});
