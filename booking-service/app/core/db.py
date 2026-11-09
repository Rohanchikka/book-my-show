from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.core.config import Settings


def create_engine(settings: Settings) -> AsyncEngine:
    return create_async_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,  # drop dead connections instead of failing a request
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker:
    # expire_on_commit=False: after commit, objects keep their loaded values.
    # With the default (True), touching an attribute after commit triggers a lazy
    # reload, which is I/O and fails in async code.
    return async_sessionmaker(engine, expire_on_commit=False)
