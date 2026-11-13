"""Seat map: the read-heavy endpoint, cached in Redis for 5 seconds.

Trade-off, stated plainly: a user may see a seat as free for up to 5s after someone took it.
That is acceptable because POST /bookings re-checks under a lock and answers 409 honestly.
We protect the database from thousands of seat-map refreshes and accept slightly stale reads.
"""
import json
import uuid

from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.api.errors import ShowNotFound
from app.api.schemas import SeatOut
from app.domain.models import Show, ShowSeat

CACHE_TTL_SECONDS = 5


def _natural_key(seat_id: str) -> tuple[str, int]:
    row = seat_id.rstrip("0123456789")
    return row, int(seat_id[len(row):])  # so A2 sorts before A10


class SeatMapService:
    def __init__(self, session_factory: async_sessionmaker, redis: Redis):
        self.session_factory = session_factory
        self.redis = redis

    async def get_seat_map(self, show_id: uuid.UUID) -> list[SeatOut]:
        cache_key = f"seatmap:{show_id}"
        cached = await self.redis.get(cache_key)
        if cached:
            return [SeatOut.model_validate(item) for item in json.loads(cached)]

        async with self.session_factory() as session:
            if await session.get(Show, show_id) is None:
                raise ShowNotFound("Show not found")
            rows = (await session.execute(select(ShowSeat).where(ShowSeat.show_id == show_id))).scalars().all()

        seats = [SeatOut(seat_id=r.seat_id, price=r.price, status=r.status) for r in sorted(rows, key=lambda r: _natural_key(r.seat_id))]
        await self.redis.set(cache_key, json.dumps([s.model_dump(mode="json", by_alias=True) for s in seats]), ex=CACHE_TTL_SECONDS)
        return seats
