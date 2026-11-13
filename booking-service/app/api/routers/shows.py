import uuid

from fastapi import APIRouter

from app.api.deps import SeatMapServiceDep
from app.api.schemas import SeatOut

router = APIRouter(prefix="/api/v1/shows", tags=["shows"])


@router.get("/{show_id}/seats", response_model=list[SeatOut])
async def get_seat_map(show_id: uuid.UUID, service: SeatMapServiceDep):
    return await service.get_seat_map(show_id)
