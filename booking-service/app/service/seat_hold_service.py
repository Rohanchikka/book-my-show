"""Redis seat holds: the FAST PATH that keeps contention away from the database.

Redis is not the source of truth (Postgres is). It exists so that 10,000 people hammering the
same popular seat get an instant 409 from memory, rather than 10,000 row-lock waits in Postgres.
"""
import uuid
from pathlib import Path
from typing import Protocol

from redis.asyncio import Redis

LUA_DIR = Path(__file__).resolve().parent.parent / "lua"


def seat_key(show_id: uuid.UUID, seat_id: str) -> str:
    return f"seat:{show_id}:{seat_id}"


class SeatHolds(Protocol):
    """What BookingService needs. A Protocol is Python's interface: any class with these
    methods qualifies, with no 'implements' keyword. Tests use it to swap Redis out."""

    async def acquire(self, show_id: uuid.UUID, seat_ids: list[str], token: uuid.UUID, ttl_seconds: int) -> list[str]: ...

    async def release(self, show_id: uuid.UUID, seat_ids: list[str], token: uuid.UUID) -> int: ...


class SeatHoldService:
    def __init__(self, redis: Redis):
        # register_script uploads the script once and calls it by SHA afterwards (EVALSHA).
        self._acquire = redis.register_script((LUA_DIR / "acquire.lua").read_text())
        self._release = redis.register_script((LUA_DIR / "release.lua").read_text())

    async def acquire(self, show_id, seat_ids, token, ttl_seconds) -> list[str]:
        """Returns the seat ids that were already held. An empty list means we got them all."""
        keys = [seat_key(show_id, s) for s in seat_ids]
        conflicting_keys = await self._acquire(keys=keys, args=[str(token), ttl_seconds * 1000])
        return [k.rsplit(":", 1)[1] for k in conflicting_keys]

    async def release(self, show_id, seat_ids, token) -> int:
        keys = [seat_key(show_id, s) for s in seat_ids]
        return await self._release(keys=keys, args=[str(token)])


class NoopSeatHolds:
    """Pretends every acquire succeeds. Used in tests to prove Postgres alone prevents double booking."""

    async def acquire(self, show_id, seat_ids, token, ttl_seconds) -> list[str]:
        return []

    async def release(self, show_id, seat_ids, token) -> int:
        return 0
