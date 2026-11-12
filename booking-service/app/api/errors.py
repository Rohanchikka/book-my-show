"""One error envelope for the whole API:
    {"code": "...", "message": "...", "traceId": "...", "timestamp": "...", ...extra}
Services raise AppError subclasses; the handlers in main.py turn them into JSON.
"""


class AppError(Exception):
    status_code = 400
    code = "BAD_REQUEST"

    def __init__(self, message: str, **extra):
        super().__init__(message)
        self.message = message
        self.extra = extra


class Unauthorized(AppError):
    status_code = 401
    code = "UNAUTHORIZED"


class BookingNotFound(AppError):
    status_code = 404
    code = "BOOKING_NOT_FOUND"


class ShowNotFound(AppError):
    status_code = 404
    code = "SHOW_NOT_FOUND"


class SeatNotFound(AppError):
    status_code = 404
    code = "SEAT_NOT_FOUND"


class SeatsUnavailable(AppError):
    status_code = 409
    code = "SEATS_UNAVAILABLE"

    def __init__(self, conflicting_seats: list[str]):
        super().__init__("One or more seats are no longer available", conflictingSeats=sorted(conflicting_seats))


class InvalidBookingState(AppError):
    status_code = 409
    code = "INVALID_BOOKING_STATE"


class RequestInFlight(AppError):
    status_code = 409
    code = "REQUEST_IN_FLIGHT"


class IdempotencyKeyReused(AppError):
    status_code = 422
    code = "IDEMPOTENCY_KEY_REUSED"
