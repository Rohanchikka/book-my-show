"""The Lua scripts, tested directly against real Redis."""
import asyncio
import uuid

import pytest_asyncio
from redis.asyncio import Redis

from app.service.seat_hold_service import SeatHoldService
from tests.conftest import TEST_REDIS_URL


@pytest_asyncio.fixture
async def redis():
    r = Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    await r.flushdb()
    yield r
    await r.aclose()


async def test_acquire_is_all_or_nothing(redis):
    holds, show = SeatHoldService(redis), uuid.uuid4()
    assert await holds.acquire(show, ["A1", "A2"], uuid.uuid4(), 60) == []

    assert await holds.acquire(show, ["A2", "A3"], uuid.uuid4(), 60) == ["A2"]
    assert await redis.exists(f"seat:{show}:A3") == 0  # A3 was free, but we got nothing because A2 failed


async def test_release_with_wrong_token_does_not_delete_someone_elses_hold(redis):
    holds, show = SeatHoldService(redis), uuid.uuid4()
    mine, theirs = uuid.uuid4(), uuid.uuid4()
    await holds.acquire(show, ["A1"], theirs, 60)

    assert await holds.release(show, ["A1"], mine) == 0  # my stale cleanup must not free their seat
    assert await redis.exists(f"seat:{show}:A1") == 1
    assert await holds.release(show, ["A1"], theirs) == 1


async def test_hold_expires_by_itself(redis):
    holds, show = SeatHoldService(redis), uuid.uuid4()
    await redis.set(f"seat:{show}:A1", "x", px=50)
    await asyncio.sleep(0.1)
    assert await holds.acquire(show, ["A1"], uuid.uuid4(), 60) == []  # lock freed by TTL alone
