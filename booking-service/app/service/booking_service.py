"""BookingService: the only code allowed to change seat inventory.

Read the create_booking flow top to bottom; it is the story you tell in an interview:

    1. Claim the idempotency key           (a retry must not create a second booking)
    2. Redis: acquire seat holds, all or none   (fast path, absorbs contention)
    3. Postgres, ONE transaction:
         lock seat rows in sorted order (FOR UPDATE), verify AVAILABLE,
         insert booking, mark seats HELD, stage outbox event, complete idempotency record
       The partial unique index is the last line of defence if everything above is wrong.
    4. On ANY failure after step 2, release the Redis holds.
"""
import hashlib
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

import structlog
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.errors import (
    BookingNotFound,
    IdempotencyKeyReused,
    InvalidBookingState,
    RequestInFlight,
    SeatNotFound,
    SeatsUnavailable,
)
from app.api.schemas import BookingOut
from app.core.config import Settings
from app.domain.enums import BookingSeatStatus, BookingStatus, SeatStatus
from app.domain.models import Booking, BookingSeat, IdempotencyRecord, ShowSeat, utcnow
from app.domain.state_machine import assert_can_transition
from app.messaging.outbox import add_outbox_event
from app.service.seat_hold_service import SeatHolds

log = structlog.get_logger()


class BookingService:
    def __init__(self, session_factory: async_sessionmaker, holds: SeatHolds, settings: Settings):
        self.session_factory = session_factory
        self.holds = holds
        self.settings = settings

    # ------------------------------------------------------------------ create

    async def create_booking(
        self, *, user_id: str, show_id: uuid.UUID, seat_ids: list[str], idempotency_key: str
    ) -> tuple[BookingOut, bool]:
        """Returns (booking, replayed). replayed=True means we returned a stored result."""
        request_hash = hashlib.sha256(f"{show_id}:{','.join(seat_ids)}".encode()).hexdigest()

        replay = await self._claim_idempotency_key(idempotency_key, user_id, request_hash)
        if replay is not None:
            return replay, True

        try:
            booking = await self._hold_and_persist(user_id, show_id, seat_ids, idempotency_key)
        except Exception:
            # The attempt failed, so the key must not stay claimed, or the client's retry would be
            # told "in flight" forever.
            await self._drop_idempotency_key(idempotency_key, user_id)
            raise
        return booking, False

    async def _hold_and_persist(
        self, user_id: str, show_id: uuid.UUID, seat_ids: list[str], idempotency_key: str
    ) -> BookingOut:
        hold_token = uuid.uuid4()

        # Step 2: Redis fast path.
        conflicts = await self.holds.acquire(show_id, seat_ids, hold_token, self.settings.hold_ttl_seconds)
        if conflicts:
            raise SeatsUnavailable(conflicts)

        # Steps 3-4: Postgres. If anything fails from here on, give the Redis holds back.
        # (If the process itself dies, the holds simply expire via their TTL.)
        try:
            return await self._persist_booking(user_id, show_id, seat_ids, idempotency_key, hold_token)
        except IntegrityError as exc:
            # The partial unique index fired: the database refused a double booking.
            log.warning("unique_index_blocked_double_booking", show_id=str(show_id), seats=seat_ids)
            await self.holds.release(show_id, seat_ids, hold_token)
            raise SeatsUnavailable(seat_ids) from exc
        except Exception:
            await self.holds.release(show_id, seat_ids, hold_token)
            raise

    async def _persist_booking(
        self,
        user_id: str,
        show_id: uuid.UUID,
        seat_ids: list[str],
        idempotency_key: str,
        hold_token: uuid.UUID,
    ) -> BookingOut:
        now = utcnow()
        async with self.session_factory() as session, session.begin():
            # Lock the seat rows in SORTED order (seat_ids arrive sorted from the schema).
            # Two transactions locking the same rows in opposite orders deadlock; one consistent
            # order makes that impossible. FOR UPDATE makes competing transactions wait here.
            rows = (
                (
                    await session.execute(
                        select(ShowSeat)
                        .where(ShowSeat.show_id == show_id, ShowSeat.seat_id.in_(seat_ids))
                        .order_by(ShowSeat.seat_id)
                        .with_for_update()
                    )
                )
                .scalars()
                .all()
            )

            missing = set(seat_ids) - {r.seat_id for r in rows}
            if missing:
                raise SeatNotFound(f"Unknown seats for this show: {sorted(missing)}")

            # We only see these values AFTER acquiring the lock, so they cannot be stale.
            unavailable = [r.seat_id for r in rows if r.status != SeatStatus.AVAILABLE]
            if unavailable:
                raise SeatsUnavailable(unavailable)

            booking = Booking(
                id=uuid.uuid4(),
                user_id=user_id,
                show_id=show_id,
                status=BookingStatus.AWAITING_PAYMENT,
                amount=sum((r.price for r in rows), Decimal("0")),
                hold_token=hold_token,
                hold_expires_at=now + timedelta(seconds=self.settings.hold_ttl_seconds),
                created_at=now,
                updated_at=now,
                seats=[BookingSeat(show_id=show_id, seat_id=r.seat_id, status=BookingSeatStatus.HELD) for r in rows],
            )
            session.add(booking)
            await session.flush()  # booking must exist before show_seat rows reference it

            for r in rows:
                r.status = SeatStatus.HELD
                r.booking_id = booking.id

            out = self._to_out(booking)
            add_outbox_event(
                session,
                "booking.created",
                str(booking.id),
                {
                    "bookingId": str(booking.id),
                    "userId": user_id,
                    "showId": str(show_id),
                    "seats": out.seats,
                    "amount": str(booking.amount),
                    "holdExpiresAt": booking.hold_expires_at.isoformat(),
                },
            )

            # Completing the idempotency record in the SAME transaction means: either the booking
            # AND its stored response exist, or neither does.
            record = await session.get(IdempotencyRecord, (idempotency_key, user_id))
            record.status = "COMPLETED"
            record.response = out.model_dump(mode="json", by_alias=True)

        log.info("booking_created", booking_id=str(out.booking_id), seats=out.seats)
        return out

    # ------------------------------------------------------------ idempotency

    async def _claim_idempotency_key(self, key: str, user_id: str, request_hash: str) -> BookingOut | None:
        """None  -> we own the key and must do the work.
        BookingOut -> an earlier identical request already finished; replay its response."""
        for _ in range(3):
            async with self.session_factory() as session, session.begin():
                inserted = (
                    await session.execute(
                        pg_insert(IdempotencyRecord)
                        .values(key=key, user_id=user_id, request_hash=request_hash, status="IN_PROGRESS", created_at=utcnow())
                        .on_conflict_do_nothing()
                        .returning(IdempotencyRecord.key)
                    )
                ).first()
                if inserted:
                    return None

                record = await session.get(IdempotencyRecord, (key, user_id), with_for_update=True)
                if record is None:
                    continue  # the previous owner failed and deleted it between our two statements; retry
                if record.request_hash != request_hash:
                    raise IdempotencyKeyReused("This Idempotency-Key was already used with a different request")
                if record.status == "COMPLETED":
                    return BookingOut.model_validate(record.response)

                # IN_PROGRESS: either a live request is running, or its process died.
                age = (utcnow() - record.created_at).total_seconds()
                if age > self.settings.idempotency_stale_seconds:
                    record.created_at = utcnow()  # take over the dead owner's claim
                    return None
                raise RequestInFlight("A request with this Idempotency-Key is still being processed")
        raise RequestInFlight("Could not claim the Idempotency-Key, please retry")

    async def _drop_idempotency_key(self, key: str, user_id: str) -> None:
        async with self.session_factory() as session, session.begin():
            await session.execute(
                delete(IdempotencyRecord).where(
                    IdempotencyRecord.key == key,
                    IdempotencyRecord.user_id == user_id,
                    IdempotencyRecord.status == "IN_PROGRESS",
                )
            )

    # ------------------------------------------------------------------- read

    async def get_booking(self, booking_id: uuid.UUID, user_id: str) -> BookingOut:
        async with self.session_factory() as session:
            booking = await session.get(Booking, booking_id)
        if booking is None or booking.user_id != user_id:
            # Same 404 for "doesn't exist" and "not yours": don't reveal which bookings exist.
            raise BookingNotFound("Booking not found")
        return self._to_out(booking)

    # ----------------------------------------------------------------- cancel

    async def cancel_booking(self, booking_id: uuid.UUID, user_id: str) -> BookingOut:
        async with self.session_factory() as session, session.begin():
            booking = await self._lock_booking(session, booking_id)
            if booking is None or booking.user_id != user_id:
                raise BookingNotFound("Booking not found")
            if booking.status != BookingStatus.AWAITING_PAYMENT:
                raise InvalidBookingState(f"Cannot cancel a booking that is {booking.status}")
            await self._settle(session, booking, BookingStatus.CANCELLED)
            add_outbox_event(session, "booking.cancelled", str(booking.id), {"bookingId": str(booking.id)})
            out = self._to_out(booking)
        await self._release_holds(booking)
        return out

    # ------------------------------------------------- saga: payment results

    async def confirm_payment(self, booking_id: uuid.UUID, payment_id: str | None = None) -> str:
        """Handle payment.succeeded. Safe to call repeatedly with the same event."""
        async with self.session_factory() as session, session.begin():
            booking = await self._lock_booking(session, booking_id)
            if booking is None:
                log.warning("payment_for_unknown_booking", booking_id=str(booking_id))
                return "unknown_booking"
            if booking.status == BookingStatus.CONFIRMED:
                return "duplicate"  # Kafka delivered it twice. Harmless.
            if booking.status != BookingStatus.AWAITING_PAYMENT:
                # Money was taken but the seats were already released (hold expired / cancelled).
                # We cannot honour the booking, so ask for the money back.
                add_outbox_event(
                    session,
                    "booking.refund_requested",
                    str(booking.id),
                    {"bookingId": str(booking.id), "paymentId": payment_id, "reason": f"booking_{booking.status}"},
                )
                log.warning("late_payment_refund_requested", booking_id=str(booking.id), status=booking.status)
                return "refund_requested"

            await self._settle(session, booking, BookingStatus.CONFIRMED)
            add_outbox_event(
                session,
                "booking.confirmed",
                str(booking.id),
                {"bookingId": str(booking.id), "userId": booking.user_id, "seats": self._to_out(booking).seats},
            )
        log.info("booking_confirmed", booking_id=str(booking_id))
        return "confirmed"

    async def fail_payment(self, booking_id: uuid.UUID, reason: str | None = None) -> str:
        """Handle payment.failed: compensate by releasing the seats."""
        async with self.session_factory() as session, session.begin():
            booking = await self._lock_booking(session, booking_id)
            if booking is None:
                return "unknown_booking"
            if booking.status != BookingStatus.AWAITING_PAYMENT:
                return "ignored"  # already terminal: duplicate event, or the sweeper got here first
            await self._settle(session, booking, BookingStatus.PAYMENT_FAILED)
        await self._release_holds(booking)
        log.info("booking_payment_failed", booking_id=str(booking_id), reason=reason)
        return "payment_failed"

    # ---------------------------------------------------------------- sweeper

    async def expire_overdue(self, now: datetime | None = None, batch_size: int = 50) -> int:
        """Expire bookings whose hold ran out. Redis TTL frees the LOCK on its own, but the database
        rows would still say HELD forever without this job.

        SKIP LOCKED: if a payment callback is mid-flight on a booking, skip it rather than wait.
        Whichever of the two takes the row lock first wins; the other sees a terminal state."""
        now = now or utcnow()
        async with self.session_factory() as session, session.begin():
            bookings = (
                (
                    await session.execute(
                        select(Booking)
                        .where(Booking.status == BookingStatus.AWAITING_PAYMENT, Booking.hold_expires_at <= now)
                        .order_by(Booking.hold_expires_at)
                        .limit(batch_size)
                        .with_for_update(skip_locked=True)
                    )
                )
                .scalars()
                .all()
            )
            for booking in bookings:
                await self._settle(session, booking, BookingStatus.EXPIRED)
                add_outbox_event(session, "booking.expired", str(booking.id), {"bookingId": str(booking.id)})
        for booking in bookings:
            await self._release_holds(booking)
        if bookings:
            log.info("bookings_expired", count=len(bookings))
        return len(bookings)

    # --------------------------------------------------------------- helpers

    @staticmethod
    async def _lock_booking(session: AsyncSession, booking_id: uuid.UUID) -> Booking | None:
        """SELECT ... FOR UPDATE on the booking row: serialises every state change on it."""
        return (
            await session.execute(select(Booking).where(Booking.id == booking_id).with_for_update())
        ).scalar_one_or_none()

    @staticmethod
    async def _settle(session: AsyncSession, booking: Booking, new_status: BookingStatus) -> None:
        """Move a booking to a terminal state and update its seats to match."""
        assert_can_transition(booking.status, new_status)

        # Same sorted lock order as create_booking, for the same deadlock-avoidance reason.
        seat_rows = (
            (
                await session.execute(
                    select(ShowSeat)
                    .where(ShowSeat.booking_id == booking.id)
                    .order_by(ShowSeat.seat_id)
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
        confirmed = new_status == BookingStatus.CONFIRMED
        for row in seat_rows:
            row.status = SeatStatus.BOOKED if confirmed else SeatStatus.AVAILABLE
            if not confirmed:
                row.booking_id = None
        for bs in booking.seats:
            bs.status = BookingSeatStatus.BOOKED if confirmed else BookingSeatStatus.RELEASED
        booking.status = new_status

    async def _release_holds(self, booking: Booking) -> None:
        # Best effort: the TTL is the backstop, so a Redis hiccup here must not fail the request.
        try:
            await self.holds.release(booking.show_id, [bs.seat_id for bs in booking.seats], booking.hold_token)
        except Exception:
            log.warning("redis_release_failed", booking_id=str(booking.id), exc_info=True)

    @staticmethod
    def _to_out(booking: Booking) -> BookingOut:
        return BookingOut(
            booking_id=booking.id,
            show_id=booking.show_id,
            status=booking.status,
            seats=sorted(bs.seat_id for bs in booking.seats),
            amount=booking.amount,
            hold_expires_at=booking.hold_expires_at,
            created_at=booking.created_at,
        )
