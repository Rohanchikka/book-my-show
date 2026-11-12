"""Test fixtures. Tests run against REAL Postgres and Redis (not mocks), because the whole point of
this service is locking behaviour that mocks cannot reproduce.

Locally:  docker compose up -d postgres redis   (then create a `booking_test` database)
CI:       GitHub Actions service containers (see .github/workflows/ci.yml)
Override with TEST_DATABASE_URL / TEST_REDIS_URL.
"""
import asyncio
import os
import subprocess
import sys
import uuid
from datetime import timedelta
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from jose import jwt
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import Settings
from app.domain.models import Show, ShowSeat, utcnow

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TEST_DB_URL = os.environ.get("TEST_DATABASE_URL", "postgresql+asyncpg://bms:bms@localhost:5432/booking_test")
TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/1")


def make_settings(**overrides) -> Settings:
    return Settings(
        database_url=TEST_DB_URL,
        redis_url=TEST_REDIS_URL,
        scheduler_enabled=False,  # tests call the sweeper / outbox poller directly: deterministic
        kafka_enabled=False,
        **overrides,
    )


async def _reset_schema() -> None:
    engine = create_async_engine(TEST_DB_URL, isolation_level="AUTOCOMMIT")
    async with engine.connect() as conn:
        await conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
    await engine.dispose()


@pytest.fixture(scope="session", autouse=True)
def migrated_db():
    """Rebuild the schema from scratch by running the REAL Alembic migrations, so migrations are tested too.

    We wipe the schema instead of running `alembic downgrade base`: a downgrade assumes the database
    matches the migration history, and a half-broken test database (say, after an experiment) would
    make it fail. Wiping always works.
    """
    asyncio.run(_reset_schema())
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        env={**os.environ, "DATABASE_URL": TEST_DB_URL},
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"alembic upgrade failed:\n{result.stderr}")


@pytest_asyncio.fixture
async def app():
    from app.main import create_app  # lazy: lets early tests run before the API layer exists

    application = create_app(make_settings())
    async with application.router.lifespan_context(application):  # runs startup/shutdown like a real server
        async with application.state.session_factory() as s, s.begin():
            await s.execute(
                text(
                    "TRUNCATE outbox_event, idempotency_record, booking_seat, show_seat, booking, show "
                    "RESTART IDENTITY CASCADE"
                )
            )
        await application.state.redis.flushdb()
        yield application


@pytest_asyncio.fixture
async def client(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture
async def show_id(app) -> uuid.UUID:
    """A show with seats A1-A5 (price 100) and B1-B5 (price 200)."""
    sid = uuid.uuid4()
    async with app.state.session_factory() as s, s.begin():
        s.add(Show(id=sid, movie_title="Test Movie", theatre_name="Test Theatre", starts_at=utcnow() + timedelta(days=1)))
        await s.flush()
        s.add_all(
            ShowSeat(show_id=sid, seat_id=f"{row}{n}", price=price, status="AVAILABLE")
            for row, price in (("A", 100), ("B", 200))
            for n in range(1, 6)
        )
    return sid


def auth(user: str = "alice") -> dict:
    settings = make_settings()
    token = jwt.encode(
        {"sub": user, "exp": utcnow() + timedelta(hours=1)}, settings.jwt_secret, algorithm=settings.jwt_algorithm
    )
    return {"Authorization": f"Bearer {token}"}


async def post_booking(client, user: str, show_id, seats: list[str], key: str | None = None):
    return await client.post(
        "/api/v1/bookings",
        json={"showId": str(show_id), "seatIds": seats},
        headers={**auth(user), "Idempotency-Key": key or uuid.uuid4().hex},
    )


async def db_scalar(app, sql: str, **params):
    async with app.state.session_factory() as s:
        return (await s.execute(text(sql), params)).scalar()


async def outbox_topics(app) -> list[str]:
    async with app.state.session_factory() as s:
        return [r[0] for r in (await s.execute(text("SELECT topic FROM outbox_event ORDER BY id"))).all()]
