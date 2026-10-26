/**
 * Connection helper + retry-count reader, shared by all four services.
 *
 * Ported line-for-line from the proven JS lab's shared/rabbit.js — this file
 * carries no assignment-specific logic and is not part of what any service
 * "implements". It is mounted read-only into every container at
 * /app/shared (see ../docker-compose.yml), exactly like the original.
 */

import amqp, { type Channel, type ChannelModel, type ConsumeMessage } from "amqplib";

const DEFAULT_RETRIES = 10;
const DEFAULT_DELAY = 3000;
const MAX_DELAY = 30000;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export interface RabbitConnection {
  connection: ChannelModel;
  channel: Channel;
}

/**
 * Connects to RabbitMQ with exponential backoff retry. Returns the AMQP
 * connection and a single channel. Handles the common startup race
 * condition where a Node service starts before RabbitMQ is fully ready.
 */
export async function connectWithRetry(
  url: string,
  retries = DEFAULT_RETRIES,
  delay = DEFAULT_DELAY,
): Promise<RabbitConnection> {
  let attempt = 0;
  let lastError: Error | undefined;

  while (attempt < retries) {
    try {
      const connection = await amqp.connect(url);
      const channel = await connection.createChannel();

      connection.on("error", (err: Error) => {
        console.error(`[RabbitMQ] Connection error: ${err.message}`);
      });

      connection.on("close", () => {
        console.warn("[RabbitMQ] Connection closed");
      });

      return { connection, channel };
    } catch (err) {
      lastError = err as Error;
      attempt += 1;

      const waitMs = Math.min(delay * Math.pow(2, attempt - 1), MAX_DELAY);
      console.warn(
        `[RabbitMQ] Connection attempt ${attempt}/${retries} failed: ${lastError.message}. Retrying in ${waitMs}ms`,
      );

      if (attempt >= retries) {
        break;
      }

      await sleep(waitMs);
    }
  }

  throw new Error(
    `[RabbitMQ] Unable to connect after ${retries} attempts: ${lastError ? lastError.message : "Unknown error"}`,
  );
}

/**
 * Parses the `x-death` header array RabbitMQ attaches once a message has
 * been dead-lettered at least once, and sums the retry counts across every
 * exchange/queue path it travelled through. Returns 0 for a message that
 * has never been retried.
 */
export function getRetryCount(msg: ConsumeMessage): number {
  const xDeath = msg.properties.headers?.["x-death"] as
    | Array<{ count?: number }>
    | undefined;

  if (!Array.isArray(xDeath)) {
    return 0;
  }

  return xDeath.reduce((sum, entry) => sum + (Number(entry?.count) || 0), 0);
}
