
import express from "express";
import { randomUUID } from "node:crypto";
import { connectWithRetry } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const PORT = Number(process.env.PORT) || 3000;
const app = express();
app.use(express.json());

const orders = new Map<string, CanonicalOrder>();

const ALLOWED_QUEUES = new Set([
  "payments.queue",
  "inventory.queue",
  "notifications.queue",
]);

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(
    process.env.RABBITMQ_URL!,
  );

  app.get("/health", (_req, res) => {
    res.status(200).json({ status: "ok" });
  });

  app.post("/orders", (req, res) => {
    const order = req.body as CanonicalOrder;
    const correlationId = randomUUID();

    channel.publish(
      "orders.exchange",
      "",
      Buffer.from(JSON.stringify(order)),
      {
        persistent: true,
        contentType: "application/json",
        headers: { correlationId },
      },
    );

    orders.set(correlationId, order);
    res.status(201).json({ correlationId, status: "accepted" });
  });

  app.get("/orders/:correlationId", (req, res) => {
    const correlationId = req.params.correlationId;
    const order = orders.get(correlationId);

    if (!order) {
      res.status(404).json({ error: "Order not found" });
      return;
    }

    res.status(200).json({ correlationId, order });
  });

  app.post("/dlq/replay", async (_req, res) => {
    let replayed = 0;

    try {
      while (true) {
        const msg = await channel.get("orders.dlq", { noAck: false });

        if (!msg) break;

        const headers = { ...(msg.properties.headers ?? {}) };
        const deathHistory = headers["x-death"] as
          | Array<{ queue?: string }>
          | undefined;

        const originalQueue =
          headers["originalQueue"] ??
          deathHistory?.find((entry) =>
            ALLOWED_QUEUES.has(entry.queue ?? ""),
          )?.queue;

        if (
          typeof originalQueue !== "string" ||
          !ALLOWED_QUEUES.has(originalQueue)
        ) {
          channel.nack(msg, false, true);
          break;
        }

        // Remove previous retry metadata so replay starts a fresh retry cycle.
        delete headers["x-death"];
        delete headers["x-first-death-exchange"];
        delete headers["x-first-death-queue"];
        delete headers["x-first-death-reason"];
        delete headers["x-last-death-exchange"];
        delete headers["x-last-death-queue"];
        delete headers["x-last-death-reason"];
        delete headers["originalQueue"];

        channel.sendToQueue(originalQueue, msg.content, {
          persistent: true,
          contentType: msg.properties.contentType,
          contentEncoding: msg.properties.contentEncoding,
          correlationId: msg.properties.correlationId,
          headers,
        });

        channel.ack(msg);
        replayed++;
      }

      res.status(200).json({ replayed });
    } catch (err) {
      console.error("[Order] DLQ replay failed:", err);
      res.status(500).json({ error: "DLQ replay failed", replayed });
    }
  });

  app.listen(PORT, () => {
    console.log(`[Order] HTTP server listening on ${PORT}`);
  });
}

main().catch((err: unknown) => {
  console.error("[Order] Startup failed:", err);
  process.exit(1);
});
