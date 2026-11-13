import uuid
from typing import Annotated

from fastapi import APIRouter, Header, Response

from app.api.deps import BookingServiceDep
from app.api.schemas import BookingOut, CreateBookingRequest
from app.core.security import CurrentUser

router = APIRouter(prefix="/api/v1/bookings", tags=["bookings"])


@router.post("", response_model=BookingOut, status_code=201)
async def create_booking(
    body: CreateBookingRequest,
    response: Response,
    user_id: CurrentUser,  # comes from the JWT, never from the body
    service: BookingServiceDep,
    idempotency_key: Annotated[str, Header(min_length=8, max_length=128)],  # header "Idempotency-Key"
):
    booking, replayed = await service.create_booking(
        user_id=user_id, show_id=body.show_id, seat_ids=body.seat_ids, idempotency_key=idempotency_key
    )
    if replayed:
        response.headers["Idempotent-Replayed"] = "true"
    return booking


@router.get("/{booking_id}", response_model=BookingOut)
async def get_booking(booking_id: uuid.UUID, user_id: CurrentUser, service: BookingServiceDep):
    return await service.get_booking(booking_id, user_id)


@router.post("/{booking_id}/cancel", response_model=BookingOut)
async def cancel_booking(booking_id: uuid.UUID, user_id: CurrentUser, service: BookingServiceDep):
    return await service.cancel_booking(booking_id, user_id)
