from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live")
async def live():
    """Liveness: the process is up. Kubernetes restarts the pod if this fails."""
    return {"status": "ok"}


@router.get("/ready")
async def ready(request: Request):
    """Readiness: dependencies are reachable. Kubernetes stops sending traffic if this fails."""
    checks: dict[str, str] = {}
    try:
        async with request.app.state.session_factory() as session:
            await session.execute(text("SELECT 1"))
        checks["postgres"] = "up"
    except Exception:
        checks["postgres"] = "down"
    try:
        await request.app.state.redis.ping()
        checks["redis"] = "up"
    except Exception:
        checks["redis"] = "down"
    ok = all(v == "up" for v in checks.values())
    return JSONResponse(status_code=200 if ok else 503, content={"status": "ok" if ok else "degraded", **checks})
