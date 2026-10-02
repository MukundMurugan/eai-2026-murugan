"""
Router Service
==============
Splits each incoming order into individual item messages and routes them
to the appropriate worker queue.
"""

import json
import os

import pika


def get_rabbitmq_connection():
    """Connect to RabbitMQ using the configured host."""
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.environ.get("RABBITMQ_HOST", "localhost")
        )
    )


ROUTES = {
    "physical": "orders.physical",
    "digital": "orders.digital",
    "subscription": "orders.subscription",
}


def route_order(ch, method, properties, body):
    """Split an order, route each item, and acknowledge the original."""
    connection = None

    try:
        order = json.loads(body)
        order_id = order["orderId"]
        correlation_id = order.get("correlationId", order_id)
        items = order.get("items", [])

        print(f"[Router] Processing order {order_id}")

        connection = get_rabbitmq_connection()
        channel = connection.channel()

        # Declare all output queues before publishing.
        for queue_name in ROUTES.values():
            channel.queue_declare(queue=queue_name, durable=True)

        total_items = len(items)

        for index, item in enumerate(items):
            item_type = item.get("type")

            routing_key = ROUTES.get(item_type)

            if routing_key is None:
                # Unknown types are routed to the physical worker so that
                # the item is not silently discarded.
                print(
                    f"[Router] Unknown item type {item_type!r} "
                    f"for order {order_id}; routing to orders.physical"
                )
                routing_key = "orders.physical"

            item_message = {
                "orderId": order_id,
                "correlationId": correlation_id,
                "itemIndex": index,
                "totalItems": total_items,
                "item": item,
            }

            channel.basic_publish(
                exchange="",
                routing_key=routing_key,
                body=json.dumps(item_message),
                properties=pika.BasicProperties(delivery_mode=2),
            )

        # Acknowledge the original only after all items were published.
        ch.basic_ack(delivery_tag=method.delivery_tag)

        print(
            f"[Router] Order {order_id} split into "
            f"{total_items} items and routed"
        )

    except Exception as exc:
        print(f"[Router] Failed to route order: {exc}")
        # Requeue the original message so a transient failure can be retried.
        ch.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=True,
        )

    finally:
        if connection is not None and connection.is_open:
            connection.close()


def main():
    """Connect and consume incoming orders."""
    connection = get_rabbitmq_connection()
    channel = connection.channel()

    channel.queue_declare(queue="orders.incoming", durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
        queue="orders.incoming",
        on_message_callback=route_order,
    )

    print("[Router] Waiting for orders...")
    channel.start_consuming()


if __name__ == "__main__":
    main()