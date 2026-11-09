"""SQLAlchemy models (2.0 style: typed Mapped[...] columns).

Statuses are stored as plain strings guarded by CHECK constraints rather than native
Postgres ENUMs, because altering a native ENUM in a migration is painful.
"""
import uuid
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    String,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class Show(Base):
    """Read-model of a show, replicated from catalog-service. Booking never edits it."""

    __tablename__ = "show"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    movie_title: Mapped[str] = mapped_column(String(200))
    theatre_name: Mapped[str] = mapped_column(String(200))
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ShowSeat(Base):
    """One row per (show, seat). This table IS the seat inventory."""

    __tablename__ = "show_seat"
    __table_args__ = (
        UniqueConstraint("show_id", "seat_id", name="uq_show_seat"),
        CheckConstraint("status IN ('AVAILABLE','HELD','BOOKED')", name="ck_show_seat_status"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    show_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("show.id"))
    seat_id: Mapped[str] = mapped_column(String(8))
    price: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    status: Mapped[str] = mapped_column(String(16), default="AVAILABLE")
    booking_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("booking.id"), default=None)


class Booking(Base):
    __tablename__ = "booking"
    __table_args__ = (
        CheckConstraint(
            "status IN ('AWAITING_PAYMENT','CONFIRMED','PAYMENT_FAILED','EXPIRED','CANCELLED')",
            name="ck_booking_status",
        ),
        # Partial index: the sweeper only ever looks at bookings that are still awaiting payment.
        Index(
            "ix_booking_sweeper",
            "hold_expires_at",
            postgresql_where=text("status = 'AWAITING_PAYMENT'"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    show_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("show.id"))
    status: Mapped[str] = mapped_column(String(32))
    amount: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    hold_token: Mapped[uuid.UUID] = mapped_column(Uuid)
    hold_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    # lazy="selectin": async code cannot lazy-load (there is no implicit I/O in async),
    # so seats are loaded eagerly with one extra query whenever a Booking is loaded.
    seats: Mapped[list["BookingSeat"]] = relationship(
        back_populates="booking", lazy="selectin", cascade="all, delete-orphan"
    )


class BookingSeat(Base):
    __tablename__ = "booking_seat"
    __table_args__ = (
        CheckConstraint("status IN ('HELD','BOOKED','RELEASED')", name="ck_booking_seat_status"),
        # THE BACKSTOP. Even if the Redis lock and all application logic are wrong, Postgres
        # refuses a second active claim on the same seat of the same show.
        Index(
            "uq_booking_seat_active",
            "show_id",
            "seat_id",
            unique=True,
            postgresql_where=text("status IN ('HELD','BOOKED')"),
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    booking_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("booking.id"), index=True)
    show_id: Mapped[uuid.UUID] = mapped_column(Uuid)
    seat_id: Mapped[str] = mapped_column(String(8))
    status: Mapped[str] = mapped_column(String(16))

    booking: Mapped[Booking] = relationship(back_populates="seats")


class OutboxEvent(Base):
    """Events waiting to be published to Kafka. Written in the SAME transaction as the state change."""

    __tablename__ = "outbox_event"
    __table_args__ = (
        Index("ix_outbox_unpublished", "id", postgresql_where=text("published_at IS NULL")),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[uuid.UUID] = mapped_column(Uuid, unique=True)
    topic: Mapped[str] = mapped_column(String(100))
    key: Mapped[str] = mapped_column(String(100))
    payload: Mapped[dict] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)


class IdempotencyRecord(Base):
    __tablename__ = "idempotency_record"
    __table_args__ = (
        CheckConstraint("status IN ('IN_PROGRESS','COMPLETED')", name="ck_idempotency_status"),
    )

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16))
    response: Mapped[dict | None] = mapped_column(JSONB, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
