"""
Aggregator Service
==================
Collects worker results by order, publishes complete results when all
items have responded, and publishes partial results after an idle timeout.
"""

import json
import os
import threading
import time

import pika


def get_rabbitmq_connection():
    """Connect to RabbitMQ using the configured host."""
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.environ.get("RABBITMQ_HOST", "localhost")
        )
    )


# Each order is tracked independently.
# Results are keyed by itemIndex to prevent duplicate counting.
in_flight = {}
completed_orders = set()
lock = threading.Lock()

IDLE_TIMEOUT_SECONDS = float(
    os.environ.get("AGGREGATOR_IDLE_TIMEOUT_SECONDS", "5")
)
SWEEP_INTERVAL_SECONDS = 1.0


def build_completion(order_id, order_state, status):
    """Build the required complete or partial result message."""
    total_items = order_state["totalItems"]
    results_by_index = order_state["results"]

    missing_indexes = [
        index
        for index in range(total_items)
        if index not in results_by_index
    ]

    return {
        "orderId": order_id,
        "correlationId": order_state["correlationId"],
        "status": status,
        "totalItems": total_items,
        "receivedItems": len(results_by_index),
        "itemResults": list(results_by_index.values()),
        "missingItemIndexes": missing_indexes,
    }


def publish_completion(message):
    """Publish a persistent completion message."""
    connection = get_rabbitmq_connection()

    try:
        channel = connection.channel()
        channel.queue_declare(queue="orders.complete", durable=True)

        channel.basic_publish(
            exchange="",
            routing_key="orders.complete",
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2),
        )
    finally:
        if connection.is_open:
            connection.close()


def aggregate_result(ch, method, properties, body):
    """Record a result and publish a completion when appropriate."""
    completion = None

    try:
        result = json.loads(body)

        order_id = result["orderId"]
        correlation_id = result.get("correlationId", order_id)
        item_index = int(result["itemIndex"])
        total_items = int(result["totalItems"])

        if total_items < 0 or not 0 <= item_index < total_items:
            raise ValueError("Invalid itemIndex or totalItems")

        with lock:

            if order_id in completed_orders:
                print(
                    f"[Aggregator] Ignoring late result for "
                    f"completed order {order_id}"
                )
                return




            # Ignore late results after this order has already completed.
            if order_id not in in_flight:
                in_flight[order_id] = {
                    "correlationId": correlation_id,
                    "totalItems": total_items,
                    "results": {},
                    "lastActivity": time.monotonic(),
                }

            state = in_flight[order_id]

            # Refresh the idle timeout whenever a result arrives,
            # including a duplicate delivery.
            state["lastActivity"] = time.monotonic()

            # Keep the first result for each item index.
            if item_index not in state["results"]:
                state["results"][item_index] = result
                state["lastActivity"] = time.monotonic()

            if len(state["results"]) == state["totalItems"]:
                completion = build_completion(
                    order_id, state, "complete"
                )
                del in_flight[order_id]
                completed_orders.add(order_id)

        if completion is not None:
            publish_completion(completion)
            print(f"[Aggregator] Order {order_id} completed")

    except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        print(f"[Aggregator] Ignoring invalid result: {exc}")

    except Exception as exc:
        print(f"[Aggregator] Failed to process result: {exc}")

    finally:
        # Acknowledge the result so malformed messages don't block the queue.
        ch.basic_ack(delivery_tag=method.delivery_tag)


def sweep_timeouts():
    """Publish partial results for orders that have gone idle."""
    while True:
        time.sleep(SWEEP_INTERVAL_SECONDS)
        now = time.monotonic()
        expired_completions = []

        with lock:
            expired_order_ids = [
                order_id
                for order_id, state in in_flight.items()
                if now - state["lastActivity"] >= IDLE_TIMEOUT_SECONDS
            ]

            for order_id in expired_order_ids:
                state = in_flight.pop(order_id)
                completed_orders.add(order_id)

                # If all items arrived just before the sweep, mark complete.
                status = (
                    "complete"
                    if len(state["results"]) == state["totalItems"]
                    else "partial"
                )

                message = build_completion(order_id, state, status)
                expired_completions.append(message)

        # Never hold the state lock while doing network I/O.
        for message in expired_completions:
            try:
                publish_completion(message)
                print(
                    f"[Aggregator] Order {message['orderId']} "
                    f"timed out: {message['status']}"
                )
            except Exception as exc:
                print(
                    f"[Aggregator] Failed to publish timeout result: {exc}"
                )


def main():
    """Start the timeout sweeper and consume worker results."""
    connection = get_rabbitmq_connection()
    channel = connection.channel()

    channel.queue_declare(queue="orders.results", durable=True)
    channel.queue_declare(queue="orders.complete", durable=True)

    channel.basic_qos(prefetch_count=1)

    sweeper = threading.Thread(target=sweep_timeouts, daemon=True)
    sweeper.start()

    channel.basic_consume(
        queue="orders.results",
        on_message_callback=aggregate_result,
    )

    print("[Aggregator] Waiting for results...")
    channel.start_consuming()


if __name__ == "__main__":
    main()
