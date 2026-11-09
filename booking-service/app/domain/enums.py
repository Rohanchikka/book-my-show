from enum import StrEnum  # Python 3.11+: members ARE strings, so they compare equal to "HELD" etc.


class BookingStatus(StrEnum):
    AWAITING_PAYMENT = "AWAITING_PAYMENT"
    CONFIRMED = "CONFIRMED"
    PAYMENT_FAILED = "PAYMENT_FAILED"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"


class SeatStatus(StrEnum):
    AVAILABLE = "AVAILABLE"
    HELD = "HELD"
    BOOKED = "BOOKED"


class BookingSeatStatus(StrEnum):
    HELD = "HELD"
    BOOKED = "BOOKED"
    RELEASED = "RELEASED"
