"""
Routers — all endpoints.
Routers only validate inputs and call services.
Zero business logic here. Zero DB queries here.
"""
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from bson import ObjectId
from db.mongo import get_db
from motor.motor_asyncio import AsyncIOMotorDatabase
from services.match_service import MatchService, AnalyticsService, PlayerService
from services.permissions_service import PermissionsService, ModulesService
from services.data_fetcher import DataFetcher
from services.matchstats_aggregate import (
    fetch_participants_by_match_ids,
    build_display_row,
    count_distinct_match_ids_for_explorer,
    paginate_distinct_match_ids_for_explorer,
)
from engine.analytics import run_all


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def serialize_id(value):
    """Convert ObjectId to string for JSON serialization."""
    if isinstance(value, ObjectId):
        return str(value)
    return value


def serialize_doc(doc):
    """Recursively convert all ObjectId objects to strings in a document."""
    if doc is None:
        return None
    if isinstance(doc, ObjectId):
        return str(doc)
    if isinstance(doc, dict):
        return {k: serialize_doc(v) for k, v in doc.items()}
    if isinstance(doc, list):
        return [serialize_doc(item) for item in doc]
    return doc


async def get_actual_match_id(db: AsyncIOMotorDatabase, match_stat_id: str) -> str:
    """
    Convert a matchstat _id to the actual match matchId.
    matchstat._id is the individual record ID,
    matchstat.matchId is the actual match ObjectId.
    """
    match = await MatchService.get(db, match_stat_id)
    if match and match.get("matchId"):
        return str(match.get("matchId"))
    return match_stat_id

# ─── MATCH ROUTER ─────────────────────────────────────────────────────────────

match_router = APIRouter(prefix="/matches", tags=["matches"])


async def run_list_matches(
    search: str,
    limit: int,
    offset: int,
    db: AsyncIOMotorDatabase,
):
    """
    Paginated match explorer list (newest first).
    Registered on the FastAPI app at GET /matches (not only on APIRouter) so GET always
    binds reliably behind proxies; see main.py.

    Data source: matchstats collection (single source of truth).
    Paginates by distinct matchId (not raw participant rows), so total matches the DB.
    """
    total = await count_distinct_match_ids_for_explorer(db, search)
    mids = await paginate_distinct_match_ids_for_explorer(
        db, search=search, limit=limit, offset=offset
    )
    by_mid = await fetch_participants_by_match_ids(db, mids)

    formatted_matches = []
    for mid in mids:
        recs = by_mid.get(mid, [])
        if not recs:
            continue
        recs_sorted = sorted(
            recs,
            key=lambda r: str((r.get("participant") or {}).get("refId") or ""),
        )
        first = recs_sorted[0]
        pa_id = str((first.get("participant") or {}).get("refId") or "")
        disp = build_display_row(pa_id, first, recs_sorted)

        formatted_matches.append({
            "match_id": disp.get("match_id"),
            "matchId": serialize_id(mid),
            "date": disp.get("date") or first.get("createdAt"),
            "sport_id": disp.get("sport_id"),
            "player_a_id": serialize_id(disp.get("player_a_id")),
            "player_b_id": serialize_id(disp.get("player_b_id")),
            "player_a_name": disp.get("player_a_name"),
            "player_b_name": disp.get("player_b_name"),
            "winner_id": serialize_id(disp.get("winner_id")),
            "total_sets": disp.get("total_sets"),
            "sets_won_a": disp.get("sets_won_a"),
            "sets_won_b": disp.get("sets_won_b"),
            "event": serialize_id(disp.get("centrepitch_event_id")),
            "participant_count": len(recs_sorted),
        })

    return serialize_doc({
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": offset + len(formatted_matches) < total,
        "matches": formatted_matches,
    })


@match_router.get("/{match_id}/summary")
async def get_match_summary(match_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Core match facts from matchstats (teamTotals.sets) — no analytics_sets required."""
    match_data = await DataFetcher.get_match_with_participants(db, match_id)
    if not match_data:
        raise HTTPException(404, "Match not found")

    parts = match_data.get("participants") or []
    if len(parts) < 1:
        raise HTTPException(404, "Match not found")

    ordered = sorted(
        parts,
        key=lambda r: str((r.get("participant") or {}).get("refId") or ""),
    )
    pa = (ordered[0].get("participant") or {}) if ordered else {}
    pb = (ordered[1].get("participant") or {}) if len(ordered) > 1 else {}
    pa_id = serialize_id(pa.get("refId"))
    pb_id = serialize_id(pb.get("refId"))
    pa_name = pa.get("name") or pa_id
    pb_name = pb.get("name") or pb_id

    sets_raw = (ordered[0].get("teamTotals") or {}).get("sets") or []
    if not sets_raw and len(ordered) > 1:
        sets_raw = (ordered[1].get("teamTotals") or {}).get("sets") or []

    sets_out = []
    sw_a = sw_b = 0
    for s in sets_raw:
        sca = int(s.get("score_a") or 0)
        scb = int(s.get("score_b") or 0)
        wid = s.get("winner")
        wids = serialize_id(wid) if wid is not None else None
        if wids == str(pa_id):
            sw_a += 1
        elif wids == str(pb_id):
            sw_b += 1
        sets_out.append({
            "set_number": s.get("set_number"),
            "score_a": sca,
            "score_b": scb,
            "winner_id": wids,
            "is_deuce": bool(s.get("is_deuce", False)),
        })

    winner_match = pa_id if sw_a > sw_b else (pb_id if sw_b > sw_a else None)
    row0 = ordered[0]
    dt = row0.get("date") or row0.get("createdAt")

    return {
        "match_id": serialize_id(row0.get("_id")),
        "date": dt.isoformat() if hasattr(dt, "isoformat") else dt,
        "player_a_id": pa_id,
        "player_b_id": pb_id,
        "player_a_name": pa_name,
        "player_b_name": pb_name,
        "winner_id": winner_match,
        "total_sets": len(sets_out) or None,
        "sets_won": {"a": sw_a, "b": sw_b},
        "sets": sets_out,
    }


@match_router.get("/{match_id}/score-progression")
async def get_score_progression(match_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Running score after each point. Computed on-demand from matchstats."""
    match_data = await DataFetcher.get_match_with_participants(db, match_id)
    if not match_data:
        raise HTTPException(404, "Match not found")

    actual_match_id = match_data["match_id"]

    # Try cache first
    data = await AnalyticsService.get_cached(db, actual_match_id, "score_progression")
    if data is None:
        # Compute on-demand
        engine_data = await DataFetcher.build_engine_data(db, match_data)
        if not engine_data or not engine_data.get("points"):
            raise HTTPException(404, "No match data available")

        try:
            results = run_all(engine_data)
            await AnalyticsService.compute_and_cache(db, actual_match_id, results)
            data = results.get("score_progression")
        except Exception as e:
            raise HTTPException(500, f"Computation error: {str(e)}")

    return data


@match_router.get("/{match_id}/win-probability")
async def get_win_probability(match_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Win probability per point. Computed on-demand from matchstats."""
    match_data = await DataFetcher.get_match_with_participants(db, match_id)
    if not match_data:
        raise HTTPException(404, "Match not found")

    actual_match_id = match_data["match_id"]

    # Try cache first
    data = await AnalyticsService.get_cached(db, actual_match_id, "win_probability")
    if data is None:
        # Compute on-demand
        engine_data = await DataFetcher.build_engine_data(db, match_data)
        if not engine_data or not engine_data.get("points"):
            raise HTTPException(404, "No match data available")

        try:
            results = run_all(engine_data)
            await AnalyticsService.compute_and_cache(db, actual_match_id, results)
            data = results.get("win_probability")
        except Exception as e:
            raise HTTPException(500, f"Computation error: {str(e)}")

    return data


@match_router.get("/{match_id}/momentum")
async def get_momentum(match_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Rolling 5-point momentum per player. Computed on-demand from matchstats."""
    match_data = await DataFetcher.get_match_with_participants(db, match_id)
    if not match_data:
        raise HTTPException(404, "Match not found")

    actual_match_id = match_data["match_id"]

    # Try cache first
    data = await AnalyticsService.get_cached(db, actual_match_id, "momentum")
    if data is None:
        # Compute on-demand
        engine_data = await DataFetcher.build_engine_data(db, match_data)
        if not engine_data or not engine_data.get("points"):
            raise HTTPException(404, "No match data available")

        try:
            results = run_all(engine_data)
            await AnalyticsService.compute_and_cache(db, actual_match_id, results)
            data = results.get("momentum")
        except Exception as e:
            raise HTTPException(500, f"Computation error: {str(e)}")

    return data


@match_router.get("/{match_id}/turning-points")
async def get_turning_points(match_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Points where win probability shifted >8%. Top 5 returned. Computed on-demand from matchstats."""
    match_data = await DataFetcher.get_match_with_participants(db, match_id)
    if not match_data:
        raise HTTPException(404, "Match not found")

    actual_match_id = match_data["match_id"]

    # Try cache first
    data = await AnalyticsService.get_cached(db, actual_match_id, "turning_points")
    if data is None:
        # Compute on-demand
        engine_data = await DataFetcher.build_engine_data(db, match_data)
        if not engine_data or not engine_data.get("points"):
            raise HTTPException(404, "No match data available")

        try:
            results = run_all(engine_data)
            await AnalyticsService.compute_and_cache(db, actual_match_id, results)
            data = results.get("turning_points")
        except Exception as e:
            raise HTTPException(500, f"Computation error: {str(e)}")

    return data


@match_router.get("/{match_id}/rally-analysis")
async def get_rally_analysis(match_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Rally length distribution and win rates by bucket. Computed on-demand from matchstats."""
    match_data = await DataFetcher.get_match_with_participants(db, match_id)
    if not match_data:
        raise HTTPException(404, "Match not found")

    actual_match_id = match_data["match_id"]

    # Try cache first
    data = await AnalyticsService.get_cached(db, actual_match_id, "rally_analysis")
    if data is None:
        # Compute on-demand
        engine_data = await DataFetcher.build_engine_data(db, match_data)
        if not engine_data or not engine_data.get("points"):
            raise HTTPException(404, "No match data available")

        try:
            results = run_all(engine_data)
            await AnalyticsService.compute_and_cache(db, actual_match_id, results)
            data = results.get("rally_analysis")
        except Exception as e:
            raise HTTPException(500, f"Computation error: {str(e)}")

    return data


@match_router.get("/{match_id}/ending-types")
async def get_ending_types(match_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """How points ended — smash winner, forced error, net kill etc. Computed on-demand from matchstats."""
    match_data = await DataFetcher.get_match_with_participants(db, match_id)
    if not match_data:
        raise HTTPException(404, "Match not found")

    actual_match_id = match_data["match_id"]

    # Try cache first
    data = await AnalyticsService.get_cached(db, actual_match_id, "ending_analysis")
    if data is None:
        # Compute on-demand
        engine_data = await DataFetcher.build_engine_data(db, match_data)
        if not engine_data or not engine_data.get("points"):
            raise HTTPException(404, "No match data available")

        try:
            results = run_all(engine_data)
            await AnalyticsService.compute_and_cache(db, actual_match_id, results)
            data = results.get("ending_analysis")
        except Exception as e:
            raise HTTPException(500, f"Computation error: {str(e)}")

    return data


@match_router.get("/{match_id}/pressure-analysis")
async def get_pressure_analysis(match_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Win rate at close scores and late game. Computed on-demand from matchstats."""
    match_data = await DataFetcher.get_match_with_participants(db, match_id)
    if not match_data:
        raise HTTPException(404, "Match not found")

    actual_match_id = match_data["match_id"]

    # Try cache first
    data = await AnalyticsService.get_cached(db, actual_match_id, "pressure_analysis")
    if data is None:
        # Compute on-demand
        engine_data = await DataFetcher.build_engine_data(db, match_data)
        if not engine_data or not engine_data.get("points"):
            raise HTTPException(404, "No match data available")

        try:
            results = run_all(engine_data)
            await AnalyticsService.compute_and_cache(db, actual_match_id, results)
            data = results.get("pressure_analysis")
        except Exception as e:
            raise HTTPException(500, f"Computation error: {str(e)}")

    return data


@match_router.get("/{match_id}/shot-stats")
async def get_shot_stats(
    match_id:  str,
    player_id: str = None,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Shot frequency and win rate per type. Computed on-demand from matchstats.
    Returns win rate per shot, not just count.
    """
    match_data = await DataFetcher.get_match_with_participants(db, match_id)
    if not match_data:
        raise HTTPException(404, "Match not found")

    actual_match_id = match_data["match_id"]

    # Try cache first
    data = await AnalyticsService.get_cached(db, actual_match_id, "shot_effectiveness")
    if data is None:
        # Compute on-demand
        engine_data = await DataFetcher.build_engine_data(db, match_data)
        if not engine_data or not engine_data.get("points"):
            raise HTTPException(404, "No match data available")

        try:
            results = run_all(engine_data)
            await AnalyticsService.compute_and_cache(db, actual_match_id, results)
            data = results.get("shot_effectiveness")
        except Exception as e:
            raise HTTPException(500, f"Computation error: {str(e)}")

    if data is None:
        raise HTTPException(404, "Shot analytics not found. Requires shot-level data.")

    if player_id:
        # Filter to specific player
        engine_data = await DataFetcher.build_engine_data(db, match_data)
        player_a_id = engine_data.get("match_info", {}).get("player_a_id")
        key = "player_a" if player_a_id == player_id else "player_b"
        return {player_id: data.get(key, [])}
    return data


@match_router.get("/{match_id}/shot-sequences")
async def get_shot_sequences(match_id: str, request: Request, db: AsyncIOMotorDatabase = Depends(get_db)):
    """
    Most frequent 2-shot combos and win rates. Computed on-demand from matchstats.
    Returns 404 with explanation if shot data not available.
    """
    role            = request.headers.get("X-User-Role", "anonymous")
    tier            = await PermissionsService.get_tier(db, role)
    enabled_modules = await ModulesService.get_enabled_set(db)
    role_disabled   = await PermissionsService.get_role_disabled_set(db, role)
    if not PermissionsService.can_view_module("shot_sequences", tier, enabled_modules, role_disabled):
        raise HTTPException(403, "Not permitted to access this analytics module")

    match_data = await DataFetcher.get_match_with_participants(db, match_id)
    if not match_data:
        raise HTTPException(404, "Match not found")

    actual_match_id = match_data["match_id"]

    # Try cache first
    data = await AnalyticsService.get_cached(db, actual_match_id, "shot_sequences")
    if data is None:
        # Compute on-demand
        engine_data = await DataFetcher.build_engine_data(db, match_data)
        if not engine_data or not engine_data.get("points"):
            raise HTTPException(
                404,
                "Shot sequence analysis requires shot-level data. "
                "Ingest match with individual shot records to enable this.",
            )

        try:
            results = run_all(engine_data)
            await AnalyticsService.compute_and_cache(db, actual_match_id, results)
            data = results.get("shot_sequences")
        except Exception as e:
            raise HTTPException(500, f"Computation error: {str(e)}")

    if data is None:
        raise HTTPException(
            404,
            "Shot sequence analysis requires shot-level data. "
            "Ingest match with individual shot records to enable this.",
        )
    return data


@match_router.get("/{match_id}/sets/{set_number}/analytics")
async def get_set_analytics(
    match_id:   str,
    set_number: int,
    request:  Request,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Analytics for one set only — reuses the same analytics engine on filtered data.
    Computed on request (not cached). Typically fast: ~20-40 points per set.
    """
    from engine.analytics import run_all
    set_data = await MatchService.get_set_analytics_data(db, match_id, set_number)
    if not set_data:
        raise HTTPException(404, f"Set {set_number} not found for match {match_id}")
    disabled = await ModulesService.get_disabled_set(db)
    raw = run_all(set_data, disabled_modules=disabled)

    role            = request.headers.get("X-User-Role", "anonymous")
    tier            = await PermissionsService.get_tier(db, role)
    enabled_modules = await ModulesService.get_enabled_set(db)
    role_disabled   = await PermissionsService.get_role_disabled_set(db, role)
    return PermissionsService.filter_analytics(raw, tier, enabled_modules, role_disabled)


@match_router.get("/{match_id}/full-analytics")
async def get_full_analytics(
    match_id: str,
    request:  Request,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Full analytics for a match.
    Fetches from cache if available, otherwise computes on-demand from source data.
    Source: matchstats collection (single source of truth).
    """
    # Get actual match ID for analytics_cache lookup
    match_data = await DataFetcher.get_match_with_participants(db, match_id)
    if not match_data:
        raise HTTPException(404, "Match not found")

    actual_match_id = match_data["match_id"]

    # Try to get from cache first.
    # Note: cache can be partial (e.g. only serve_analysis computed from player trends).
    cached_data = await AnalyticsService.get_full(db, actual_match_id)
    core_keys = {"score_progression", "win_probability", "momentum"}
    cache_has_core = bool(cached_data) and all(k in cached_data for k in core_keys)

    if not cache_has_core:
        # Compute on-demand from source data (fills missing modules).
        engine_data = await DataFetcher.build_engine_data(db, match_data)
        if not engine_data or not engine_data.get("points"):
            raise HTTPException(404, "No match data available for analytics")
        try:
            results = run_all(engine_data)
            await AnalyticsService.compute_and_cache(db, actual_match_id, results)
            # Reload from cache so response reflects the canonical cached shape.
            data = await AnalyticsService.get_full(db, actual_match_id) or results
        except Exception as e:
            raise HTTPException(500, f"Analytics computation error: {str(e)}")
    else:
        data = cached_data

    role            = request.headers.get("X-User-Role", "anonymous")
    tier            = await PermissionsService.get_tier(db, role)
    enabled_modules = await ModulesService.get_enabled_set(db)
    role_disabled   = await PermissionsService.get_role_disabled_set(db, role)

    return PermissionsService.filter_analytics(data, tier, enabled_modules, role_disabled)


@match_router.post("/{match_id}/coaching-brief")
async def get_coaching_brief(
    match_id: str,
    request: Request,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Feed analytics to Claude API. Return plain language coaching insights.
    Requires ANTHROPIC_API_KEY in environment.
    """
    import os, httpx

    role            = request.headers.get("X-User-Role", "anonymous")
    tier            = await PermissionsService.get_tier(db, role)
    enabled_modules = await ModulesService.get_enabled_set(db)
    role_disabled   = await PermissionsService.get_role_disabled_set(db, role)
    if not PermissionsService.can_view_module(
        "coaching_brief", tier, enabled_modules, role_disabled
    ):
        raise HTTPException(403, "AI Coaching Brief is disabled for your role or globally")

    match_data = await DataFetcher.get_match_with_participants(db, match_id)
    if not match_data:
        raise HTTPException(404, "Match not found")
    actual_match_id = match_data["match_id"]

    analytics = await AnalyticsService.get_full(db, actual_match_id)
    if not analytics:
        engine_data = await DataFetcher.build_engine_data(db, match_data)
        if not engine_data or not engine_data.get("points"):
            raise HTTPException(404, "No analytics found for this match")
        try:
            results = run_all(engine_data)
            await AnalyticsService.compute_and_cache(db, actual_match_id, results)
            analytics = results
        except Exception as e:
            raise HTTPException(500, f"Analytics computation error: {str(e)}")

    analytics = PermissionsService.filter_analytics(
        analytics, tier, enabled_modules, role_disabled
    )

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(500, "ANTHROPIC_API_KEY not configured")

    prompt = f"""You are a professional badminton coach.
Analyse this match data and give exactly 3 specific, actionable coaching insights.
Each insight must reference specific numbers from the data.
Format: JSON array of {{insight, metric_referenced, action_recommended}}.

Match analytics:
{analytics}"""

    async with httpx.AsyncClient() as client:
        response = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": api_key, "anthropic-version": "2023-06-01"},
            json={
                "model":      "claude-sonnet-4-6",
                "max_tokens": 1000,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )

    if response.status_code != 200:
        raise HTTPException(502, "Claude API error")

    return {"match_id": match_id, "insights": response.json()["content"][0]["text"]}


@match_router.delete("/{match_id}/cache")
async def delete_match_cache(
    match_id: str,
    request: Request,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Delete cached analytics for a match (superadmin only)."""
    role = str(request.headers.get("X-User-Role", "anonymous")).lower()
    if role != "superadmin":
        raise HTTPException(403, "Superadmin only")

    match_data = await DataFetcher.get_match_with_participants(db, match_id)
    if not match_data:
        raise HTTPException(404, "Match not found")
    actual_match_id = match_data["match_id"]

    deleted = await AnalyticsService.invalidate(db, actual_match_id)
    return {"match_id": match_id, "actual_match_id": actual_match_id, "deleted": deleted}


# ─── PLAYER ROUTER ────────────────────────────────────────────────────────────

player_router = APIRouter(prefix="/players", tags=["players"])


@player_router.get("")
async def list_players(
    search: str = Query(default="",  description="Filter by name"),
    limit:  int = Query(default=20,  ge=1, le=100),
    offset: int = Query(default=0,   ge=0),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """List players with analytics profiles. Paginated."""
    players, total = await PlayerService.list_all(db, search=search, limit=limit, offset=offset)
    return {
        "total":    total,
        "limit":    limit,
        "offset":   offset,
        "has_more": offset + len(players) < total,
        "players": [
            {
                "player_id":       p.get("_id"),
                "name":            p.get("name", ""),
                "sport_id":        p.get("sport_id"),
                "total_matches":   (p.get("stats") or {}).get("total_matches", 0),
                "total_wins":      (p.get("stats") or {}).get("total_wins", 0),
                "total_losses":    (p.get("stats") or {}).get("total_losses", 0),
                "win_rate":        (p.get("stats") or {}).get("win_rate_overall", 0),
                "pressure_rating": p.get("pressure_rating"),
                "signature_shot":  (p.get("stats") or {}).get("signature_shot"),
            }
            for p in players
        ],
    }


@player_router.post("/rebuild-profiles")
async def rebuild_analytics_player_profiles(
    request: Request,
    delete_existing: bool = Query(
        default=True,
        description="If true, deletes all documents in analytics_player_profiles before rebuilding.",
    ),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Wipe `analytics_player_profiles` (optional) and repopulate from `players`, `users`,
    and `matchstats`. Rolling stats use matchstats (same logic as per-match ingest updates).
    """
    role = str(request.headers.get("X-User-Role", "anonymous")).lower()
    if role != "superadmin":
        raise HTTPException(403, "Superadmin only")
    return await PlayerService.rebuild_all_profiles(db, delete_existing=delete_existing)


@player_router.get("/compare")
async def compare_players(
    player_a: str,
    player_b: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Head-to-head profile comparison."""
    return await PlayerService.compare(db, player_a, player_b)


@player_router.get("/{player_id}/profile")
async def get_player_profile(player_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """
    Rolling stats across all matches. Always a cache read.
    Updated automatically after every match ingest.
    """
    profile = await PlayerService.get_profile(db, player_id)
    if not profile:
        raise HTTPException(404, "Player profile not found")
    return profile.get("full_stats", profile)


@player_router.get("/{player_id}/matches")
async def get_player_matches(
    player_id: str,
    limit:  int = Query(default=20, ge=1, le=100, description="Max results per page"),
    offset: int = Query(default=0,  ge=0,          description="Number of records to skip"),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Paginated match history from matchstats (one row per match, both players)."""
    raw_rows, total = await MatchService.list_all(db, player_id=player_id, limit=limit, offset=offset)
    mids = [r.get("matchId") for r in raw_rows if r.get("matchId")]
    by_mid = await fetch_participants_by_match_ids(db, mids)

    # matchstats often has one document per participant per match — dedupe by matchId.
    out = []
    seen_mid = set()
    for row in raw_rows:
        mid = row.get("matchId")
        if mid is None:
            continue
        smid = str(mid)
        if smid in seen_mid:
            continue
        seen_mid.add(smid)
        recs = by_mid.get(smid) or [row]
        out.append(build_display_row(player_id, row, recs))

    return serialize_doc({
        "total":    total,
        "limit":    limit,
        "offset":   offset,
        "has_more": offset + len(raw_rows) < total,
        "matches": out,
    })


@player_router.get("/{player_id}/trends")
async def get_player_trends(player_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Shot win-rate and serve win-rate across all matches (chronological)."""
    return await PlayerService.get_trends(db, player_id)


@player_router.get("/{player_id}/last10-trends")
async def get_player_last10_trends(
    player_id: str,
    limit: int = Query(default=10, ge=1, le=20),
    date_from: Optional[datetime] = Query(
        default=None,
        description="Only include matches on/after this instant (ISO 8601).",
    ),
    date_to: Optional[datetime] = Query(
        default=None,
        description="Only include matches on/before this instant (ISO 8601).",
    ),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Last-N match trend rollups (point differential, momentum stability, rally length)."""
    return await PlayerService.get_last10_trends(
        db, player_id, limit=limit, date_from=date_from, date_to=date_to
    )


@player_router.get("/{player_id}/h2h")
async def get_player_h2h(player_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Head-to-head record vs every opponent, sorted by most-played."""
    return await PlayerService.get_h2h(db, player_id)


@player_router.get("/{player_id}/pressure-rating")
async def get_pressure_rating(
    player_id: str,
    date_from: Optional[datetime] = Query(
        default=None,
        description="If set with date_to (or alone), only matches in this window are used.",
    ),
    date_to: Optional[datetime] = Query(
        default=None,
        description="Upper bound for match date (ISO 8601).",
    ),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Clutch factor: pressure win rate minus overall win rate.
    Recomputed from match history + pressure_analysis cache (not a stale profile field).
    Optional date_from/date_to scope to the same window as the analytics date filter.
    """
    if not await PlayerService.get_profile(db, player_id):
        raise HTTPException(404, "Player not found")
    return await PlayerService.get_pressure_rating_payload(
        db, player_id, date_from=date_from, date_to=date_to
    )


# ─── PERMISSIONS ROUTER ───────────────────────────────────────────────────────

permissions_router = APIRouter(prefix="/permissions", tags=["permissions"])


@permissions_router.get("")
async def list_permissions(db: AsyncIOMotorDatabase = Depends(get_db)):
    """List all role tier assignments."""
    return await PermissionsService.list_all(db)


@permissions_router.put("/{role}")
async def update_permission(
    role: str,
    body: dict,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Update tier for a role. Superadmin only (enforced by gateway)."""
    tier = body.get("tier")
    if not tier:
        raise HTTPException(400, "Missing 'tier' in request body")
    try:
        result = await PermissionsService.update(db, role, tier)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return result


@permissions_router.put("/{role}/modules/{module_id}")
async def set_role_module_enabled(
    role: str,
    module_id: str,
    body: dict,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Enable/disable a specific module for a specific role.
    Superadmin only (enforced by gateway).
    Body: { "enabled": true|false }
    """
    if "enabled" not in body:
        raise HTTPException(400, "Missing 'enabled' in request body")
    enabled = bool(body.get("enabled"))
    try:
        return await PermissionsService.set_role_module_enabled(db, role, module_id, enabled)
    except ValueError as e:
        raise HTTPException(400, str(e))


# ─── MODULES ROUTER ───────────────────────────────────────────────────────────

modules_router = APIRouter(prefix="/modules", tags=["modules"])


@modules_router.get("")
async def list_modules(db: AsyncIOMotorDatabase = Depends(get_db)):
    """List all analytics modules with their enabled/disabled status."""
    return await ModulesService.list_all(db)


@modules_router.put("/{module_id}")
async def set_module_enabled(
    module_id: str,
    body: dict,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Enable or disable an analytics module globally.
    Superadmin only (enforced by gateway).
    Body: { "enabled": true | false }
    """
    if "enabled" not in body:
        raise HTTPException(400, "Missing 'enabled' in request body")
    enabled = bool(body["enabled"])
    try:
        result = await ModulesService.set_enabled(db, module_id, enabled)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return result
