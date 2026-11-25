# Messaging layer - the two Kafka consumers
#
#  topic "transaction-events"  -> processor  -> updates balance, sends a log message
#  topic "transaction-logs"    -> log writer -> saves the log message to the database
import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

from app.config import settings
from app.data import repository
from app.data.database import SessionLocal
from app.messaging import kafka_client

log = logging.getLogger("finlog.consumer")


def handle_transaction_event(event: dict) -> dict | None:
    """Process one transaction event. Returns the log message that should be
    sent to the log topic (or None if the event was skipped)."""
    with SessionLocal() as db:
        tx = repository.process_transaction(db, int(event["transaction_id"]))
        if tx is None:
            return None
        if tx.status == "COMPLETED":
            msg = f"{tx.type} of {tx.amount} completed, new balance {tx.account.balance}"
        else:
            msg = f"{tx.type} of {tx.amount} rejected: {tx.reject_reason}"
        return {"transaction_id": tx.id, "event": tx.status, "message": msg,
                "source": "processor"}


def handle_log_event(event: dict) -> bool:
    with SessionLocal() as db:
        saved = repository.add_log(db, int(event["transaction_id"]), event["event"],
                                   event["message"][:300], event.get("source", "kafka"))
        return saved is not None


async def run_processor():
    async def on_message(value: dict):
        log_msg = await asyncio.to_thread(handle_transaction_event, value)
        if log_msg:
            await kafka_client.publish(settings.topic_logs, log_msg, key=log_msg["transaction_id"])

    await _consume_forever(settings.topic_transactions, settings.consumer_group, on_message)


async def run_log_writer():
    async def on_message(value: dict):
        await asyncio.to_thread(handle_log_event, value)

    await _consume_forever(settings.topic_logs, settings.log_consumer_group, on_message)


async def _consume_forever(topic: str, group: str, on_message):
    # if the broker goes away we wait a bit and connect again
    while True:
        consumer = AIOKafkaConsumer(
            topic,
            **kafka_client.connection_options(),
            group_id=group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        )
        try:
            await consumer.start()
            log.info("consumer %s listening on %s", group, topic)
            async for msg in consumer:
                kafka_client.remember("received", topic, msg.value)
                try:
                    await on_message(msg.value)
                except Exception:
                    # a bad message must not stop the whole consumer
                    log.exception("could not handle message %s", msg.value)
                # commit only after we handled it -> at-least-once delivery
                await consumer.commit()
        except asyncio.CancelledError:
            break
        except Exception:
            log.exception("consumer %s crashed, retrying in 5s", group)
            await asyncio.sleep(5)
        finally:
            await consumer.stop()
