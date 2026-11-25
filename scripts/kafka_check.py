# Quick check that the Kafka settings in .env work.
# Run from the project folder:  python -m scripts.kafka_check
import asyncio
import json
import uuid

from aiokafka import AIOKafkaConsumer

from app.config import settings
from app.messaging import kafka_client


async def main():
    print("connecting to", settings.kafka_bootstrap_servers, "with", settings.kafka_security_protocol)
    await kafka_client.start_producer()
    test_id = str(uuid.uuid4())[:8]
    await kafka_client.publish(settings.topic_logs, {"check": test_id}, key="check")
    print("sent test message", test_id, "to", settings.topic_logs)

    consumer = AIOKafkaConsumer(settings.topic_logs, **kafka_client.connection_options(),
                                group_id="finlog-check-" + test_id, auto_offset_reset="earliest",
                                value_deserializer=lambda b: json.loads(b))
    await consumer.start()
    try:
        async for msg in consumer:
            if msg.value.get("check") == test_id:
                print("got it back -> Kafka works")
                break
    finally:
        await consumer.stop()
        await kafka_client.stop_producer()


if __name__ == "__main__":
    asyncio.run(asyncio.wait_for(main(), timeout=60))
