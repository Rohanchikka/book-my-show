"""Application factory. create_app() builds a fully wired FastAPI app; tests call it with their
own Settings, uvicorn uses the module-level `app` at the bottom."""
import asyncio
import uuid
from contextlib import asynccontextmanager

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from redis.asyncio import Redis

from app.api.errors import AppError
from app.api.routers import bookings, health, shows
from app.core.config import Settings, get_settings
from app.core.db import create_engine, create_session_factory
from app.core.logging import configure_logging
from app.domain.models import utcnow
from app.domain.state_machine import InvalidTransition
from app.messaging.outbox import publish_pending_events
from app.messaging.publisher import KafkaEventPublisher, LoggingEventPublisher
from app.service.booking_service import BookingService
from app.service.seat_hold_service import SeatHoldService
from app.service.seat_map_service import SeatMapService

log = structlog.get_logger()


def _envelope(request: Request, status: int, code: str, message: str, **extra) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={
            "code": code,
            "message": message,
            "traceId": getattr(request.state, "request_id", None),
            "timestamp": utcnow().isoformat(),
            **extra,
        },
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # ---- startup: build long-lived resources once ----
        configure_logging()
        engine = create_engine(settings)
        session_factory = create_session_factory(engine)
        redis = Redis.from_url(settings.redis_url, decode_responses=True)
        publisher = KafkaEventPublisher(settings.kafka_bootstrap_servers) if settings.kafka_enabled else LoggingEventPublisher()
        await publisher.start()

        booking_service = BookingService(session_factory, SeatHoldService(redis), settings)
        app.state.engine = engine
        app.state.session_factory = session_factory
        app.state.redis = redis
        app.state.publisher = publisher
        app.state.booking_service = booking_service
        app.state.seat_map_service = SeatMapService(session_factory, redis)

        scheduler = None
        if settings.scheduler_enabled:
            scheduler = AsyncIOScheduler()
            # max_instances=1 + coalesce: if a run is slow, don't stack up overlapping runs.
            scheduler.add_job(
                publish_pending_events, "interval", seconds=settings.outbox_poll_seconds,
                args=[session_factory, publisher], max_instances=1, coalesce=True, id="outbox",
            )
            scheduler.add_job(
                booking_service.expire_overdue, "interval", seconds=settings.expiry_sweep_seconds,
                max_instances=1, coalesce=True, id="expiry",
            )
            scheduler.start()

        consumer_task = None
        if settings.kafka_enabled:
            from app.messaging.consumer import run_consumer  # imported only when needed

            consumer_task = asyncio.create_task(run_consumer(settings, booking_service, publisher))

        log.info("booking_service_started")
        yield

        # ---- shutdown, in reverse order ----
        if consumer_task:
            consumer_task.cancel()
            await asyncio.gather(consumer_task, return_exceptions=True)
        if scheduler:
            scheduler.shutdown(wait=False)
        await publisher.stop()
        await redis.aclose()
        await engine.dispose()

    app = FastAPI(title="Booking Service", version="0.1.0", lifespan=lifespan)
    app.state.settings = settings

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        request.state.request_id = request_id
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=request_id)  # now on every log line of this request
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response

    @app.exception_handler(AppError)
    async def handle_app_error(request: Request, exc: AppError):
        return _envelope(request, exc.status_code, exc.code, exc.message, **exc.extra)

    @app.exception_handler(InvalidTransition)
    async def handle_invalid_transition(request: Request, exc: InvalidTransition):
        return _envelope(request, 409, "INVALID_BOOKING_STATE", str(exc))

    @app.exception_handler(RequestValidationError)
    async def handle_validation(request: Request, exc: RequestValidationError):
        details = [{"field": ".".join(str(p) for p in e["loc"]), "message": e["msg"]} for e in exc.errors()]
        return _envelope(request, 422, "VALIDATION_ERROR", "Request validation failed", details=details)

    @app.exception_handler(Exception)
    async def handle_unexpected(request: Request, exc: Exception):
        log.error("unhandled_exception", exc_info=exc)
        # Never leak internals to the client; the traceId lets you find the stack trace in the logs.
        return _envelope(request, 500, "INTERNAL_ERROR", "Something went wrong")

    app.include_router(bookings.router)
    app.include_router(shows.router)
    app.include_router(health.router)
    return app


app = create_app()
