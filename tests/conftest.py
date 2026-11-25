import os
import tempfile

# use a fresh SQLite file and no real broker for the tests
_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DATABASE_URL"] = f"sqlite:///{_db.name}"
os.environ["KAFKA_ENABLED"] = "false"

import pytest
from fastapi.testclient import TestClient

from app.data.database import Base, engine
from app.main import app
from app.messaging import consumer, kafka_client


@pytest.fixture(autouse=True)
def clean_db():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    kafka_client.recent_activity.clear()
    yield


class FakeBroker:
    """Works like Kafka for the tests: every published message goes to a list
    and is given to the right consumer function right away."""

    def __init__(self):
        self.messages = []   # (topic, key, value)
        self.down = False

    async def publish(self, topic, value, key=None):
        if self.down:
            raise kafka_client.KafkaUnavailable("broker is down (test)")
        self.messages.append((topic, key, value))
        if topic == "transaction-events":
            log_msg = consumer.handle_transaction_event(value)
            if log_msg:
                await self.publish("transaction-logs", log_msg, key=log_msg["transaction_id"])
        elif topic == "transaction-logs":
            consumer.handle_log_event(value)

    def topic(self, name):
        return [m for m in self.messages if m[0] == name]


@pytest.fixture
def broker(monkeypatch):
    b = FakeBroker()
    monkeypatch.setattr(kafka_client, "publish", b.publish)
    return b


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c
