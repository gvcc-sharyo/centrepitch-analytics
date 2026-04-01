"""
Motor (async MongoDB) connection — replaces SQLAlchemy/SQLite.
Connects to the same centrepitch_local database used by the main platform.
All analytics data lives in dedicated analytics_* collections.
"""
import os
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/centrepitch_local")

_client: AsyncIOMotorClient | None = None


async def connect():
    global _client
    _client = AsyncIOMotorClient(MONGODB_URI)
    db = _client.get_default_database()
    await _ensure_indexes(db)


async def disconnect():
    global _client
    if _client:
        _client.close()
        _client = None


async def _ensure_indexes(db: AsyncIOMotorDatabase):
    """Idempotent — safe to run on every startup.

    Note: analytics_matches collection is no longer used.
    All match data is fetched from the CentrePitch backend API.
    Only computed analytics results are cached locally.
    """
    await db.analytics_players.create_index("name")

    # Match data comes from backend API — only cache analytics results
    await db.analytics_sets.create_index("match_id")
    await db.analytics_sets.create_index([("match_id", 1), ("set_number", 1)])

    await db.analytics_points.create_index("match_id")
    await db.analytics_points.create_index("set_id")
    await db.analytics_points.create_index([("set_id", 1), ("point_number", 1)])

    await db.analytics_shots.create_index("match_id")
    await db.analytics_shots.create_index("set_id")
    await db.analytics_shots.create_index("player_id")
    await db.analytics_shots.create_index([("player_id", 1), ("shot_type", 1)])
    await db.analytics_shots.create_index([("player_id", 1), ("is_winning_shot", 1)])

    # Unique per (match_id, module_name) — upsert relies on this
    await db.analytics_cache.create_index(
        [("match_id", 1), ("module_name", 1)], unique=True
    )


async def get_db() -> AsyncIOMotorDatabase:
    """FastAPI dependency — yields the centrepitch_local database handle."""
    yield _client.get_default_database()
