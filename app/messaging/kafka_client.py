# Messaging layer - Kafka connection and the producer
import json
import logging
import os
import tempfile
from collections import deque
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from aiokafka.helpers import create_ssl_context

from app.config import settings

log = logging.getLogger("finlog.kafka")

producer: AIOKafkaProducer | None = None

# last messages that went in/out, only to show the flow in the UI demo
recent_activity: deque = deque(maxlen=40)


class KafkaUnavailable(Exception):
    pass


def remember(direction: str, topic: str, value: dict):
    recent_activity.appendleft({
        "time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "direction": direction,   # "sent" or "received"
        "topic": topic,
        "value": value,
    })


def _write_temp(text: str) -> str:
    # aiokafka wants file paths for certificates, env vars only have the text
    f = tempfile.NamedTemporaryFile("w", delete=False, suffix=".pem")
    f.write(text.replace("\\n", "\n"))
    f.close()
    return f.name


def connection_options() -> dict:
    """Same options for producer and consumers."""
    opts = {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "security_protocol": settings.kafka_security_protocol,
    }
    if settings.kafka_security_protocol in ("SSL", "SASL_SSL"):
        cafile = _write_temp(settings.kafka_ca_cert) if settings.kafka_ca_cert else None
        if settings.kafka_client_cert and settings.kafka_client_key:
            # Aiven default: client certificate authentication
            ctx = create_ssl_context(cafile=cafile,
                                     certfile=_write_temp(settings.kafka_client_cert),
                                     keyfile=_write_temp(settings.kafka_client_key))
        else:
            ctx = create_ssl_context(cafile=cafile)
        opts["ssl_context"] = ctx
    if settings.kafka_security_protocol.startswith("SASL"):
        opts["sasl_mechanism"] = settings.kafka_sasl_mechanism
        opts["sasl_plain_username"] = settings.kafka_username
        opts["sasl_plain_password"] = settings.kafka_password
    return opts


async def start_producer():
    global producer
    p = AIOKafkaProducer(
        **connection_options(),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="all",  # wait until the broker really stored the message
    )
    await p.start()
    producer = p
    log.info("Kafka producer connected to %s", settings.kafka_bootstrap_servers)


async def stop_producer():
    global producer
    if producer is not None:
        await producer.stop()
        producer = None


async def publish(topic: str, value: dict, key=None):
    if producer is None:
        raise KafkaUnavailable("Kafka producer is not connected")
    try:
        await producer.send_and_wait(topic, value=value, key=key)
    except Exception as e:  # broker down, timeout, ...
        raise KafkaUnavailable(str(e)) from e
    remember("sent", topic, value)


def is_connected() -> bool:
    return producer is not None and not producer._closed
