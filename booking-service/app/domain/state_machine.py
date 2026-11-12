"""Booking state machine. Every status change goes through assert_can_transition.

The expiry sweeper and the payment callback genuinely race in production. Both lock the
booking row (SELECT ... FOR UPDATE) and then call this guard, so exactly one of them wins
and the loser sees a terminal state and backs off.
"""
from app.domain.enums import BookingStatus

ALLOWED: dict[BookingStatus, set[BookingStatus]] = {
    BookingStatus.AWAITING_PAYMENT: {
        BookingStatus.CONFIRMED,
        BookingStatus.PAYMENT_FAILED,
        BookingStatus.EXPIRED,
        BookingStatus.CANCELLED,
    },
    # Everything else is terminal.
    BookingStatus.CONFIRMED: set(),
    BookingStatus.PAYMENT_FAILED: set(),
    BookingStatus.EXPIRED: set(),
    BookingStatus.CANCELLED: set(),
}


class InvalidTransition(Exception):
    def __init__(self, current: str, target: str):
        super().__init__(f"Cannot move booking from {current} to {target}")
        self.current = current
        self.target = target


def assert_can_transition(current: str, target: BookingStatus) -> None:
    if target not in ALLOWED[BookingStatus(current)]:
        raise InvalidTransition(current, target)
