"""Dependency injection. FastAPI calls these functions per request and passes the result into the
route. Everything long-lived is built once in main.py's lifespan and stored on app.state."""
from typing import Annotated

from fastapi import Depends, Request

from app.service.booking_service import BookingService
from app.service.seat_map_service import SeatMapService


def get_booking_service(request: Request) -> BookingService:
    return request.app.state.booking_service


def get_seat_map_service(request: Request) -> SeatMapService:
    return request.app.state.seat_map_service


BookingServiceDep = Annotated[BookingService, Depends(get_booking_service)]
SeatMapServiceDep = Annotated[SeatMapService, Depends(get_seat_map_service)]
