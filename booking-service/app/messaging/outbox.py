"""Transactional outbox.

The problem:
    db.save(booking)      # committed
    kafka.send(event)     # process dies here -> the event is lost forever

Two systems, no shared transaction. The fix: write the event to a table IN THE SAME
TRANSACTION as the state change, and let a separate poller publish it. The write is now
atomic. Delivery becomes at-least-once (a crash after Kafka accepted but before we mark the
row published means a re-send), which is why every consumer must be idempotent.
"""
import uuid
from datetime import datetime

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.models import OutboxEvent, utcnow
from app.messaging.publisher import EventPublisher

log = structlog.get_logger()


def add_outbox_event(session: AsyncSession, topic: str, key: str, data: dict) -> None:
    """Stage an event inside the caller's transaction. It is committed (or rolled back) with it."""
    event_id = uuid.uuid4()
    session.add(
        OutboxEvent(
            event_id=event_id,
            topic=topic,
            key=key,
            payload={
                "eventId": str(event_id),
                "type": topic,
                "occurredAt": utcnow().isoformat(),
                "data": data,
            },
        )
    )


async def publish_pending_events(
    session_factory: async_sessionmaker, publisher: EventPublisher, batch_size: int = 100
) -> int:
    """Publish unpublished outbox rows in insertion order. Returns how many were sent.

    SKIP LOCKED lets several instances poll without blocking each other. (Caveat: with several
    pollers, strict per-key ordering is no longer guaranteed. Run one poller if order matters.)
    """
    async with session_factory() as session, session.begin():
        rows = (
            (
                await session.execute(
                    select(OutboxEvent)
                    .where(OutboxEvent.published_at.is_(None))
                    .order_by(OutboxEvent.id)
                    .limit(batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            .scalars()
            .all()
        )
        for row in rows:
            await publisher.publish(row.topic, row.key, row.payload)
            row.published_at = utcnow()
    if rows:
        log.info("outbox_published", count=len(rows))
    return len(rows)
