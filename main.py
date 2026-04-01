"""
Main application — wires everything together.
Rate limiting middleware preserved from original.
Data layer: Motor (async MongoDB).
"""
import os
from collections import defaultdict, deque
from time import monotonic, time

from dotenv import load_dotenv
from fastapi import FastAPI, Request, Query, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorDatabase

from db.mongo import connect, disconnect, get_db
from routers.endpoints import (
    match_router,
    player_router,
    permissions_router,
    modules_router,
    run_list_matches,
)
from services.permissions_service import PermissionsService

# Load environment variables from .env file
load_dotenv()

# ─── CONFIG ──────────────────────────────────────────────────────────────────

RATE_LIMIT     = int(os.getenv("RATE_LIMIT", "600"))
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "60"))
REDIS_URL      = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PORT           = int(os.getenv("PORT", "8001"))
CORS_ORIGINS   = os.getenv("CORS_ORIGINS", "http://localhost:5174").split(",")

# ─── APP ─────────────────────────────────────────────────────────────────────

app = FastAPI(title="Centre Pitch Analytics API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in CORS_ORIGINS],
    allow_methods=["*"],
    allow_headers=["*"],
)

_request_log: dict[str, deque[float]] = defaultdict(deque)
redis_client = None


@app.on_event("startup")
async def startup():
    global redis_client
    await connect()
    # Seed role tiers + module registry (safe to call every startup — $setOnInsert)
    async for db in get_db():
        await PermissionsService.seed_defaults(db)
        break
    # Redis (optional — falls back to in-memory rate limiting)
    try:
        from redis.asyncio import Redis
        redis_client = Redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
        await redis_client.ping()
    except Exception:
        redis_client = None


@app.on_event("shutdown")
async def shutdown():
    if redis_client:
        await redis_client.aclose()
    await disconnect()


# ─── RATE LIMITING ────────────────────────────────────────────────────────────

async def _is_rate_limited_redis(client_ip: str) -> bool:
    if redis_client is None:
        return False
    key = f"rl:{client_ip}:{int(time() // WINDOW_SECONDS)}"
    current = await redis_client.incr(key)
    if current == 1:
        await redis_client.expire(key, WINDOW_SECONDS)
    return current > RATE_LIMIT


def _is_rate_limited_memory(client_ip: str) -> bool:
    now = monotonic()
    window_start = now - WINDOW_SECONDS
    hits = _request_log[client_ip]
    while hits and hits[0] < window_start:
        hits.popleft()
    if len(hits) >= RATE_LIMIT:
        return True
    hits.append(now)
    return False


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    forwarded_for = request.headers.get("x-forwarded-for")
    client_ip = (
        forwarded_for.split(",")[0].strip() if forwarded_for
        else (request.client.host if request.client else "unknown")
    )
    try:
        limited = await _is_rate_limited_redis(client_ip) if redis_client \
                  else _is_rate_limited_memory(client_ip)
    except Exception:
        limited = _is_rate_limited_memory(client_ip)

    if limited:
        return JSONResponse(
            status_code=429,
            content={"detail": "Rate limit exceeded. Please retry shortly."},
            headers={"Retry-After": str(WINDOW_SECONDS)},
        )
    response = await call_next(request)
    response.headers["X-RateLimit-Limit"] = str(RATE_LIMIT)
    return response


# ─── CACHE CONTROL ───────────────────────────────────────────────────────────
# Analytics results are computed once on ingest and never change.
# Tell browsers and proxies they can cache them for 24 h.

_IMMUTABLE_PATHS = (
    "/score-progression", "/win-probability", "/momentum",
    "/turning-points",    "/rally-analysis",  "/ending-types",
    "/pressure-analysis", "/shot-stats",      "/shot-sequences",
    "/full-analytics",    "/sets/",
)


@app.middleware("http")
async def cache_control_middleware(request: Request, call_next):
    response = await call_next(request)
    if (
        request.method == "GET"
        and response.status_code == 200
        and any(p in request.url.path for p in _IMMUTABLE_PATHS)
    ):
        response.headers["Cache-Control"] = "public, max-age=86400, stale-while-revalidate=3600"
    return response


# ─── ROUTERS ─────────────────────────────────────────────────────────────────
#
# GET /matches MUST be registered *before* match_router. The router also defines
# POST /matches; Starlette matches path /matches to the first route. If POST is
# registered first, GET receives 405 Method Not Allowed (detail from FastAPI).

@app.get("/matches", tags=["matches"])
async def list_matches_get(
    search: str = Query(default="", description="Filter by match id / player id / player name"),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Paginated match explorer list (newest first)."""
    return await run_list_matches(search, limit, offset, db)


app.include_router(match_router)
app.include_router(player_router)
app.include_router(permissions_router)
app.include_router(modules_router)


@app.get("/health")
async def health():
    return {"status": "ok", "version": "2.0.0"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=True)
