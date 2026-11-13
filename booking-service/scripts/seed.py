"""Create a demo show with 30 seats so you can try the API.   Run:  python -m scripts.seed"""
import asyncio
import uuid
from datetime import timedelta

from app.core.config import get_settings
from app.core.db import create_engine, create_session_factory
from app.domain.models import Show, ShowSeat, utcnow

DEMO_SHOW_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")
ROW_PRICES = {"A": 500, "B": 300, "C": 200}  # row A = recliners


async def main() -> None:
    engine = create_engine(get_settings())
    factory = create_session_factory(engine)
    async with factory() as session, session.begin():
        if await session.get(Show, DEMO_SHOW_ID):
            print(f"Demo show already exists: {DEMO_SHOW_ID}")
        else:
            session.add(Show(id=DEMO_SHOW_ID, movie_title="Demo Movie", theatre_name="Demo Theatre, Screen 1", starts_at=utcnow() + timedelta(days=1)))
            await session.flush()
            session.add_all(
                ShowSeat(show_id=DEMO_SHOW_ID, seat_id=f"{row}{n}", price=price, status="AVAILABLE")
                for row, price in ROW_PRICES.items()
                for n in range(1, 11)
            )
            print(f"Created demo show with 30 seats: {DEMO_SHOW_ID}")
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
