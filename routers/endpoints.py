"""
Routers — all endpoints.
Routers only validate inputs and call services.
Zero business logic here. Zero DB queries here.
"""
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from db.mongo import get_db
from motor.motor_asyncio import AsyncIOMotorDatabase
from services.match_service import MatchService, AnalyticsService, PlayerService
from services.permissions_service import PermissionsService, ModulesService

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
    """
    matches, total = await MatchService.list_matches(db, search=search, limit=limit, offset=offset)
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": offset + len(matches) < total,
        "matches": [
            {
                "match_id": m.get("_id"),
                "date": m.get("date"),
                "sport_id": m.get("sport_id"),
                "player_a_id": m.get("player_a_id"),
                "player_b_id": m.get("player_b_id"),
                "player_a_name": m.get("player_a_name"),
                "player_b_name": m.get("player_b_name"),
                "winner_id": m.get("winner_id"),
                "total_sets": m.get("total_sets"),
                "sets_won_a": m.get("sets_won_a"),
                "sets_won_b": m.get("sets_won_b"),
                "centrepitch_event_id": m.get("centrepitch_event_id"),
                "centrepitch_match_id": m.get("centrepitch_match_id"),
            }
            for m in matches
        ],
    }


@match_router.post("", status_code=201)
async def create_match(match_data: dict, db: AsyncIOMotorDatabase = Depends(get_db)):
    """
    Ingest a full match JSON.
    Triggers: DB insert → analytics compute → cache store → profile update.
    Returns match_id immediately. Analytics are pre-computed on write.
    """
    match_id = await MatchService.create(db, match_data)
    return {"match_id": match_id, "status": "created"}


@match_router.get("/{match_id}/summary")
async def get_match_summary(match_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Core match facts. No computation — reads DB."""
    match = await db.analytics_matches.find_one({"_id": match_id})
    if not match:
        raise HTTPException(404, "Match not found")

    sets_cursor = db.analytics_sets.find({"match_id": match_id}).sort("set_number", 1)
    sets = await sets_cursor.to_list(None)

    return {
        "match_id":    match["_id"],
        "date":        match.get("date"),
        "player_a_id": match.get("player_a_id"),
        "player_b_id": match.get("player_b_id"),
        "player_a_name": match.get("player_a_name"),
        "player_b_name": match.get("player_b_name"),
        "winner_id":   match.get("winner_id"),
        "total_sets":  match.get("total_sets"),
        "sets_won": {
            "a": match.get("sets_won_a"),
            "b": match.get("sets_won_b"),
        },
        "sets": [
            {
                "set_number": s.get("set_number"),
                "score_a":    s.get("score_a"),
                "score_b":    s.get("score_b"),
                "winner_id":  s.get("winner_id"),
                "is_deuce":   s.get("is_deuce", False),
            }
            for s in sets
        ],
    }


@match_router.get("/{match_id}/score-progression")
async def get_score_progression(match_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Running score after each point. Pre-computed. Always fast."""
    data = await AnalyticsService.get_cached(db, match_id, "score_progression")
    if data is None:
        raise HTTPException(404, "Analytics not found. Re-ingest match.")
    return data


@match_router.get("/{match_id}/win-probability")
async def get_win_probability(match_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Win probability per point. Pre-computed on ingest."""
    data = await AnalyticsService.get_cached(db, match_id, "win_probability")
    if data is None:
        raise HTTPException(404, "Analytics not found")
    return data


@match_router.get("/{match_id}/momentum")
async def get_momentum(match_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Rolling 5-point momentum per player."""
    data = await AnalyticsService.get_cached(db, match_id, "momentum")
    if data is None:
        raise HTTPException(404, "Analytics not found")
    return data


@match_router.get("/{match_id}/turning-points")
async def get_turning_points(match_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Points where win probability shifted >8%. Top 5 returned."""
    data = await AnalyticsService.get_cached(db, match_id, "turning_points")
    if data is None:
        raise HTTPException(404, "Analytics not found")
    return data


@match_router.get("/{match_id}/rally-analysis")
async def get_rally_analysis(match_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Rally length distribution and win rates by bucket."""
    data = await AnalyticsService.get_cached(db, match_id, "rally_analysis")
    if data is None:
        raise HTTPException(404, "Analytics not found")
    return data


@match_router.get("/{match_id}/ending-types")
async def get_ending_types(match_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """How points ended — smash winner, forced error, net kill etc."""
    data = await AnalyticsService.get_cached(db, match_id, "ending_analysis")
    if data is None:
        raise HTTPException(404, "Analytics not found")
    return data


@match_router.get("/{match_id}/pressure-analysis")
async def get_pressure_analysis(match_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Win rate at close scores and late game."""
    data = await AnalyticsService.get_cached(db, match_id, "pressure_analysis")
    if data is None:
        raise HTTPException(404, "Analytics not found")
    return data


@match_router.get("/{match_id}/shot-stats")
async def get_shot_stats(
    match_id:  str,
    player_id: str = None,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Shot frequency and win rate per type.
    Returns win rate per shot, not just count.
    """
    data = await AnalyticsService.get_cached(db, match_id, "shot_effectiveness")
    if data is None:
        raise HTTPException(404, "Shot analytics not found. Requires shot-level data.")
    if player_id:
        match = await db.analytics_matches.find_one({"_id": match_id})
        if not match:
            raise HTTPException(404, "Match not found")
        key = "player_a" if match.get("player_a_id") == player_id else "player_b"
        return {player_id: data.get(key, [])}
    return data


@match_router.get("/{match_id}/shot-sequences")
async def get_shot_sequences(match_id: str, request: Request, db: AsyncIOMotorDatabase = Depends(get_db)):
    """
    Most frequent 2-shot combos and win rates.
    Returns 404 with explanation if shot data not available.
    """
    role            = request.headers.get("X-User-Role", "anonymous")
    tier            = await PermissionsService.get_tier(db, role)
    enabled_modules = await ModulesService.get_enabled_set(db)
    role_disabled   = await PermissionsService.get_role_disabled_set(db, role)
    if not PermissionsService.can_view_module("shot_sequences", tier, enabled_modules, role_disabled):
        raise HTTPException(403, "Not permitted to access this analytics module")

    data = await AnalyticsService.get_cached(db, match_id, "shot_sequences")
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
    Single endpoint returning all cached analytics filtered by the caller's role tier
    and globally enabled/disabled modules.
    Used by the dashboard to load everything in one call.
    """
    data = await AnalyticsService.get_full(db, match_id)
    if not data:
        raise HTTPException(404, "No analytics found for this match")

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

    analytics = await AnalyticsService.get_full(db, match_id)
    if not analytics:
        raise HTTPException(404, "No analytics found")

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
                "pressure_rating": p.get("pressure_rating", 0),
                "signature_shot":  (p.get("stats") or {}).get("signature_shot"),
            }
            for p in players
        ],
    }


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
    """Paginated match history. Use limit/offset for navigation."""
    matches, total = await MatchService.list_all(db, player_id=player_id, limit=limit, offset=offset)
    return {
        "total":    total,
        "limit":    limit,
        "offset":   offset,
        "has_more": offset + len(matches) < total,
        "matches": [
            {
                "match_id":   m.get("_id"),
                "date":       m.get("date"),
                "won":        m.get("winner_id") == player_id,
                "player_a_id": m.get("player_a_id"),
                "player_b_id": m.get("player_b_id"),
                "player_a_name": m.get("player_a_name"),
                "player_b_name": m.get("player_b_name"),
                # Useful for client-side H2H + scoped filtering
                "opponent_id": (
                    m.get("player_b_id") if m.get("player_a_id") == player_id else
                    m.get("player_a_id") if m.get("player_b_id") == player_id else
                    None
                ),
                "opponent_name": (
                    m.get("player_b_name") if m.get("player_a_id") == player_id else
                    m.get("player_a_name") if m.get("player_b_id") == player_id else
                    None
                ),
                "sets_won_a": m.get("sets_won_a"),
                "sets_won_b": m.get("sets_won_b"),
                "total_sets": m.get("total_sets"),
                "sport_id":   m.get("sport_id"),
                "centrepitch_event_id": m.get("centrepitch_event_id"),
                "centrepitch_match_id": m.get("centrepitch_match_id"),
            }
            for m in matches
        ],
    }


@player_router.get("/{player_id}/trends")
async def get_player_trends(player_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Shot win-rate and serve win-rate across all matches (chronological)."""
    return await PlayerService.get_trends(db, player_id)


@player_router.get("/{player_id}/last10-trends")
async def get_player_last10_trends(
    player_id: str,
    limit: int = Query(default=10, ge=1, le=20),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Last-N match trend rollups (point differential, momentum stability, rally length)."""
    return await PlayerService.get_last10_trends(db, player_id, limit=limit)


@player_router.get("/{player_id}/h2h")
async def get_player_h2h(player_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Head-to-head record vs every opponent, sorted by most-played."""
    return await PlayerService.get_h2h(db, player_id)


@player_router.get("/{player_id}/pressure-rating")
async def get_pressure_rating(player_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """
    Clutch factor: pressure win rate minus overall win rate.
    Positive = performs better under pressure.
    Negative = pressure-sensitive.
    """
    profile = await PlayerService.get_profile(db, player_id)
    if not profile:
        raise HTTPException(404, "Player not found")
    rating = profile.get("pressure_rating", 0.0)
    return {
        "player_id":       player_id,
        "pressure_rating": rating,
        "interpretation": (
            "Clutch — performs better under pressure"
            if rating > 0.05
            else "Pressure-sensitive — performance drops in close games"
            if rating < -0.05
            else "Consistent — pressure has minimal effect"
        ),
    }


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
