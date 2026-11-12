"""Request/response models. Pydantic validates incoming JSON and serialises outgoing JSON.

CamelModel gives us snake_case in Python and camelCase on the wire (showId, seatIds)
without writing any mapping code.
"""
import re
import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic.alias_generators import to_camel

MAX_SEATS_PER_BOOKING = 6
_SEAT_RE = re.compile(r"^[A-Z]{1,2}\d{1,3}$")


class CamelModel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class CreateBookingRequest(CamelModel):
    show_id: uuid.UUID
    seat_ids: list[str] = Field(min_length=1, max_length=MAX_SEATS_PER_BOOKING)

    @field_validator("seat_ids")
    @classmethod
    def normalise_seats(cls, seats: list[str]) -> list[str]:
        cleaned = [s.strip().upper() for s in seats]
        for s in cleaned:
            if not _SEAT_RE.match(s):
                raise ValueError(f"Invalid seat id: {s!r}")
        # Sorted + de-duplicated. Sorting also fixes a consistent lock order (see BookingService).
        return sorted(set(cleaned))


class BookingOut(CamelModel):
    booking_id: uuid.UUID
    show_id: uuid.UUID
    status: str
    seats: list[str]
    amount: Decimal
    hold_expires_at: datetime
    created_at: datetime


class SeatOut(CamelModel):
    seat_id: str
    price: Decimal
    status: str
