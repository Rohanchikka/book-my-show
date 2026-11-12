"""Event publishers. BookingService never talks to Kafka directly; it only writes outbox rows.
The outbox poller hands those rows to whichever publisher is configured."""
import json
from typing import Protocol

import structlog

log = structlog.get_logger()


class EventPublisher(Protocol):
    async def publish(self, topic: str, key: str, value: dict) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...


class KafkaEventPublisher:
    def __init__(self, bootstrap_servers: str):
        from aiokafka import AIOKafkaProducer  # imported lazily so tests don't need Kafka

        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",  # wait for all in-sync replicas: don't lose events on broker failure
            enable_idempotence=True,  # broker de-duplicates producer retries
            value_serializer=lambda v: json.dumps(v).encode(),
            key_serializer=lambda k: k.encode(),
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish(self, topic: str, key: str, value: dict) -> None:
        # Same key -> same partition -> events for one booking stay in order.
        await self._producer.send_and_wait(topic, value=value, key=key)


class LoggingEventPublisher:
    """Used when Kafka is disabled (local dev). Events are logged instead of sent."""

    async def start(self) -> None: ...
    async def stop(self) -> None: ...

    async def publish(self, topic: str, key: str, value: dict) -> None:
        log.info("event_published_to_log", topic=topic, key=key, event_type=value.get("type"))


class InMemoryEventPublisher:
    """Collects events in a list. For tests."""

    def __init__(self):
        self.events: list[tuple[str, str, dict]] = []

    async def start(self) -> None: ...
    async def stop(self) -> None: ...

    async def publish(self, topic: str, key: str, value: dict) -> None:
        self.events.append((topic, key, value))
