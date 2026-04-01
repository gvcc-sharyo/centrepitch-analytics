"""
Service layer — all business logic lives here.
Routers call services. Services query backend API and MongoDB via Motor.
No business logic in routers. No data ingestion in CA-1.

Architecture:
- Match data (source of truth): CentrePitch backend API
- Computed results: Cached locally in MongoDB
- Player identity: CentrePitch User ObjectId (string)
- No local data storage — only computations and caching

CA-1 is a pure analytics compute service:
1. Fetch match data from backend API
2. Compute analytics using the engine
3. Cache results in MongoDB
4. Return computed analytics to frontend
"""
import statistics
from datetime import datetime, timedelta
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase
from engine.analytics import run_all
from services.backend_client import BackendClient, matchstats_participant_ref_match
from services.data_fetcher import DataFetcher
from services.matchstats_aggregate import (
    build_display_row,
    compute_last10_metrics_for_match,
    fetch_participants_by_match_ids,
    load_match_summaries_for_player_profile,
)


# ─── MATCH SERVICE ────────────────────────────────────────────────────────────

class MatchService:
    """
    Match analytics service.

    Data flow:
    1. Frontend/Backend requests analytics for match_id
    2. Fetch match metadata from backend API (matchstats collection)
    3. Check if analytics are cached in MongoDB
    4. If not cached: compute via engine, cache results
    5. Return cached results

    NOTE: CA-1 does NOT ingest match data. All data comes from backend API.
    """

    @staticmethod
    async def get(db: AsyncIOMotorDatabase, match_id: str) -> dict | None:
        """Fetch match data from matchstats collection (single source of truth)."""
        return await BackendClient.get_match_stat(db, match_id)

    @staticmethod
    async def get_with_sets(db: AsyncIOMotorDatabase, match_id: str) -> dict | None:
        """Fetch match + sets from matchstats collection."""
        match = await BackendClient.get_match_stat(db, match_id)
        if not match:
            return None
        # Sets are fetched from analytics_sets collection
        # (computed when match data first arrives at CA-1)
        sets = await db.analytics_sets.find(
            {"match_id": match_id}
        ).sort("set_number", 1).to_list(None)
        match["sets"] = sets
        return match

    @staticmethod
    async def _ingest_match_data(db: AsyncIOMotorDatabase, match_data: dict) -> str:
        """
        INTERNAL: Ingest a full match payload (if needed for advanced features).

        NOTE: This is only called internally when match raw data is needed.
        Normal flow: Backend provides matchstats → CA-1 computes → caches results

        Steps:
        1. Store sets (from match_data)
        2. Store points (from match_data)
        3. Store shots (from match_data)
        4. Run analytics engine
        5. Cache each module result (upsert)
        6. Update player profiles (upsert into players.analyticsProfile)
        """
        info     = match_data["match_info"]
        match_id = info["match_id"]

        # 2. Upsert sets + delete/reinsert points per set
        for s in match_data.get("sets", []):
            set_id = s["set_id"]

            await db.analytics_sets.replace_one(
                {"_id": set_id},
                {
                    "_id":          set_id,
                    "match_id":     match_id,
                    "set_number":   s["set_number"],
                    "score_a":      s["score_a"],
                    "score_b":      s["score_b"],
                    "winner_id":    s.get("winner_id"),
                    "is_deuce":     s.get("is_deuce", False),
                    "total_points": s.get("total_points", 0),
                },
                upsert=True,
            )

            # 3. Idempotent points — delete then bulk insert
            await db.analytics_points.delete_many({"set_id": set_id})
            pts = s.get("points", [])
            if pts:
                point_docs = []
                for pt in pts:
                    score_parts = str(pt.get("score_before", "0-0")).split("-")
                    sa = int(score_parts[0]) if score_parts[0].isdigit() else 0
                    sb = int(score_parts[1]) if len(score_parts) > 1 and score_parts[1].isdigit() else 0
                    point_docs.append({
                        "_id":             f"{set_id}_P{pt['point_number']}",
                        "match_id":        match_id,
                        "set_id":          set_id,
                        "set_number":      s["set_number"],
                        "point_number":    pt["point_number"],
                        "score_a_before":  sa,
                        "score_b_before":  sb,
                        "score_before":    pt.get("score_before", "0-0"),
                        "server_id":       pt.get("server"),
                        "winner_id":       pt.get("winner_id") or pt.get("point_winner"),
                        "rally_shots":     pt.get("rally_shots"),
                        "rally_duration":  pt.get("rally_duration_sec"),
                        "ending_type":     pt.get("ending_type") or pt.get("ending_shot"),
                    })
                await db.analytics_points.insert_many(point_docs, ordered=False)

        # 4. Shots (video data — skip gracefully if absent)
        raw_shots = match_data.get("shots", [])
        if raw_shots:
            await db.analytics_shots.delete_many({"match_id": match_id})
            shot_docs = [{
                "match_id":        sh["match_id"],
                "set_id":          sh.get("set_id"),
                "player_id":       sh["player_id"],
                "shot_number":     sh["shot_number"],
                "shot_type":       sh["shot_type"],
                "is_winning_shot": sh.get("is_winning_shot", False),
                "prev_shot_type":  sh.get("prev_shot_type"),
                "next_shot_type":  sh.get("next_shot_type"),
                "landing_zone":    sh.get("landing_zone"),
                "speed_kmh":       sh.get("speed_kmh"),
            } for sh in raw_shots]
            await db.analytics_shots.insert_many(shot_docs, ordered=False)

        # 5–6. Run engine → cache
        engine_data = await _build_engine_data(db, match_id, match_data)
        disabled    = await _get_disabled_modules(db)
        results     = run_all(engine_data, disabled_modules=disabled)
        await AnalyticsService.compute_and_cache(db, match_id, results)

        # 7. Update player profiles
        for key in ("player_a", "player_b"):
            pid = info[key]["player_id"]
            name = info[key].get("name", pid)
            await PlayerService.update_profile(db, pid, name)

        return match_id

    @staticmethod
    async def get_set_analytics_data(
        db: AsyncIOMotorDatabase, match_id: str, set_number: int
    ) -> dict | None:
        """
        Engine payload for a single set. Primary path: matchstats via DataFetcher (same as full match).
        Legacy fallback: analytics_sets + analytics_points when present.
        """
        sn = int(set_number)
        match_data = await DataFetcher.get_match_with_participants(db, match_id)
        if match_data:
            full = await DataFetcher.build_engine_data(db, match_data)
            if full and full.get("points"):
                pts = [
                    p
                    for p in full["points"]
                    if int(p.get("set_number") or 1) == sn
                ]
                if pts:
                    filtered = []
                    for i, p in enumerate(pts, start=1):
                        row = dict(p)
                        row["set_number"] = sn
                        row["point_number"] = i
                        filtered.append(row)
                    return {
                        "match_info": full["match_info"],
                        "points": filtered,
                        "shots": list(full.get("shots") or []),
                    }

        match = await BackendClient.get_match_stat(db, match_id)
        if not match:
            return None
        set_obj = await db.analytics_sets.find_one(
            {"match_id": match_id, "set_number": sn}
        )
        if not set_obj:
            return None
        set_id = set_obj["_id"]
        points_raw = await db.analytics_points.find(
            {"set_id": set_id}
        ).sort("point_number", 1).to_list(None)
        shots_raw = await db.analytics_shots.find({"set_id": set_id}).to_list(None)

        points_data = [
            {
                "point_number":      p["point_number"],
                "set_number":        sn,
                "winner_id":         p["winner_id"],
                "point_winner":      p["winner_id"],
                "server":            p.get("server_id"),
                "rally_shots":       p.get("rally_shots"),
                "rally_duration_sec": p.get("rally_duration"),
                "ending_type":       p.get("ending_type"),
                "score_before":      p.get("score_before", "0-0"),
            }
            for p in points_raw
        ]
        shots_data = [
            {
                "player_id":       s["player_id"],
                "shot_type":       s["shot_type"],
                "shot_number":     s["shot_number"],
                "is_winning_shot": s.get("is_winning_shot", False),
                "prev_shot_type":  s.get("prev_shot_type"),
                "match_id":        match_id,
            }
            for s in shots_raw
        ]
        return {
            "match_info": {
                "match_id":    match_id,
                "player_a_id": match["player_a_id"],
                "player_b_id": match["player_b_id"],
                "player_a":    {"player_id": match["player_a_id"]},
                "player_b":    {"player_id": match["player_b_id"]},
                "winner_id":   match.get("winner_id"),
            },
            "points": points_data,
            "shots":  shots_data,
        }

    @staticmethod
    async def list_all(
        db: AsyncIOMotorDatabase,
        player_id: str = None,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list, int]:
        """List matches from matchstats, optionally filtered by player_id."""
        if player_id:
            result = await BackendClient.get_player_matches(db, player_id, limit=limit, offset=offset)
        else:
            result = await BackendClient.list_match_stats(db, limit=limit, offset=offset)
        total   = result.get("total", 0)
        matches = result.get("matches", [])
        return matches, total

    @staticmethod
    async def list_matches(
        db: AsyncIOMotorDatabase,
        search: str = "",
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list, int]:
        """
        List all matches (newest first) from matchstats collection.
        Optional search filters by:
        - match_id (_id) partial
        - player_a_id / player_b_id partial
        - player_a_name / player_b_name partial
        """
        result = await BackendClient.list_match_stats(
            db,
            search=search,
            limit=limit,
            offset=offset,
        )
        total = result.get("total", 0)
        matches = result.get("matches", [])
        return matches, total


# ─── ANALYTICS SERVICE ────────────────────────────────────────────────────────

class AnalyticsService:

    @staticmethod
    async def compute_and_cache(db: AsyncIOMotorDatabase, match_id: str, results: dict):
        """Store each module result in analytics_cache (upsert)."""
        skip_keys = {"match_id", "modules_run", "modules_skipped"}
        for module_key, result in results.items():
            if module_key in skip_keys:
                continue
            await db.analytics_cache.update_one(
                {"match_id": match_id, "module_name": module_key},
                {"$set": {
                    "match_id":    match_id,
                    "module_name": module_key,
                    "result":      result,
                    "computed_at": datetime.utcnow(),
                }},
                upsert=True,
            )

    @staticmethod
    async def get_cached(
        db: AsyncIOMotorDatabase, match_id: str, module_name: str
    ) -> dict | None:
        doc = await db.analytics_cache.find_one(
            {"match_id": match_id, "module_name": module_name}
        )
        return doc["result"] if doc else None

    @staticmethod
    async def get_full(db: AsyncIOMotorDatabase, match_id: str) -> dict:
        """Return all cached modules for a match. Caller applies tier + module filters."""
        docs = await db.analytics_cache.find({"match_id": match_id}).to_list(None)
        if not docs:
            return {}
        result = {"match_id": match_id}
        result.update({d["module_name"]: d["result"] for d in docs})
        return result

    @staticmethod
    async def invalidate(db: AsyncIOMotorDatabase, match_id: str):
        res = await db.analytics_cache.delete_many({"match_id": match_id})
        return int(getattr(res, "deleted_count", 0))


# ─── PLAYER SERVICE ───────────────────────────────────────────────────────────

class PlayerService:

    @staticmethod
    async def list_all(
        db: AsyncIOMotorDatabase,
        search: str = "",
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list, int]:
        """Returns paginated players with stats. Also returns total count."""
        query = {}
        if search:
            query = {"name": {"$regex": search, "$options": "i"}}
        total   = await db.analytics_player_profiles.count_documents(query)
        players = await db.analytics_player_profiles.find(
            query, {"_id": 1, "name": 1, "sport_id": 1, "stats": 1, "pressure_rating": 1}
        ).sort("name", 1).skip(offset).limit(limit).to_list(None)
        return players, total

    @staticmethod
    async def get_profile(db: AsyncIOMotorDatabase, player_id: str) -> dict | None:
        return await db.analytics_player_profiles.find_one({"_id": player_id})

    @staticmethod
    async def _resolve_player_name_from_players(db: AsyncIOMotorDatabase, player_id: str) -> str:
        try:
            oid = ObjectId(player_id)
        except Exception:
            return player_id
        pl = await db.players.find_one({"user": oid})
        if pl and pl.get("name"):
            return str(pl["name"])
        usr = await db.users.find_one({"_id": oid})
        if usr:
            if usr.get("name"):
                return str(usr["name"])
            em = usr.get("email")
            if em:
                return str(em).split("@")[0]
        return player_id

    @staticmethod
    async def _upsert_minimal_profile(
        db: AsyncIOMotorDatabase, player_id: str, name: str, now: datetime | None = None,
    ):
        """Profile with zero matches (still listed in analytics)."""
        now = now or datetime.utcnow()
        empty = {
            "total_matches": 0,
            "total_wins": 0,
            "total_losses": 0,
            "win_rate_overall": 0.0,
            "win_rate_30d": 0.0,
            "win_rate_90d": 0.0,
            "form_last5": None,
            "form_prev5": None,
            "form_delta": None,
            "form_direction": "stable",
            "consistency_score": None,
            "signature_shot": None,
            "best_winning_shot": None,
            "pressure_rating": 0.0,
        }
        await db.analytics_player_profiles.update_one(
            {"_id": player_id},
            {"$set": {
                "_id": player_id,
                "name": name or player_id,
                "sport_id": "badminton",
                "stats": empty,
                "pressure_rating": 0.0,
                "updated_at": now,
            }},
            upsert=True,
        )
        try:
            await db.players.update_one(
                {"user": ObjectId(player_id)},
                {"$set": {"analyticsProfile": empty, "analyticsUpdatedAt": now}},
            )
        except Exception:
            pass

    @staticmethod
    async def update_profile(db: AsyncIOMotorDatabase, player_id: str, name: str = ""):
        """Recompute rolling stats from matchstats (primary) or legacy analytics_matches."""
        if not name:
            name = await PlayerService._resolve_player_name_from_players(db, player_id)

        matches = await load_match_summaries_for_player_profile(db, player_id)
        if not matches:
            query = {"$or": [{"player_a_id": player_id}, {"player_b_id": player_id}]}
            matches = await db.analytics_matches.find(query).sort("date", -1).to_list(None)

        if not matches:
            await PlayerService._upsert_minimal_profile(db, player_id, name)
            return

        total = len(matches)
        wins  = sum(1 for m in matches if m.get("winner_id") and str(m.get("winner_id")) == str(player_id))
        win_rate = wins / total

        now = datetime.utcnow()

        def _won(m):
            w = m.get("winner_id")
            return w is not None and str(w) == str(player_id)

        def windowed(days):
            cut  = now - timedelta(days=days)
            pool = [m for m in matches if m.get("date", now) >= cut]
            if not pool:
                return win_rate
            return sum(1 for m in pool if _won(m)) / len(pool)

        win_rate_30d = windowed(30)
        win_rate_90d = windowed(90)

        form = prev_form = form_delta = None
        form_direction = "stable"
        if total >= 5:
            form = sum(1 for m in matches[:5] if _won(m)) / 5
        if total >= 10:
            prev_form = sum(1 for m in matches[5:10] if _won(m)) / 5
        if form is not None and prev_form is not None:
            form_delta     = round(form - prev_form, 3)
            form_direction = "up" if form_delta > 0.15 else "down" if form_delta < -0.15 else "stable"

        consistency_score = None
        ratios = []
        for m in matches[:10]:
            total_sets = m.get("total_sets", 0)
            if total_sets:
                sets_won = (
                    m["sets_won_a"]
                    if str(m.get("player_a_id")) == str(player_id)
                    else m["sets_won_b"]
                )
                ratios.append(sets_won / total_sets)
        if len(ratios) >= 2:
            std = statistics.pstdev(ratios)
            consistency_score = round(max(0.0, 1.0 - (std / 0.5)), 3)

        pressure_rating = await PlayerService._get_pressure_rating(db, player_id, matches)
        sig_shot        = await PlayerService._get_signature_shot(db, player_id)
        best_win_shot   = await PlayerService._get_best_winning_shot(db, player_id)
        sport_id        = matches[0].get("sport_id", "badminton") if matches else "badminton"

        pr_store = round(pressure_rating, 3) if pressure_rating is not None else None

        full_stats = {
            "total_matches":     total,
            "total_wins":        wins,
            "total_losses":      total - wins,
            "win_rate_overall":  round(win_rate, 3),
            "win_rate_30d":      round(win_rate_30d, 3),
            "win_rate_90d":      round(win_rate_90d, 3),
            "form_last5":        round(form, 3) if form is not None else None,
            "form_prev5":        round(prev_form, 3) if prev_form is not None else None,
            "form_delta":        form_delta,
            "form_direction":    form_direction,
            "consistency_score": consistency_score,
            "signature_shot":    sig_shot,
            "best_winning_shot": best_win_shot,
            "pressure_rating":   pr_store,
        }

        # Upsert analytics_player_profiles (CA-1's own player stats)
        await db.analytics_player_profiles.update_one(
            {"_id": player_id},
            {"$set": {
                "_id":          player_id,
                "name":         name or player_id,
                "sport_id":     sport_id,
                "stats":        full_stats,
                "pressure_rating": pr_store,
                "updated_at":   now,
            }},
            upsert=True,
        )

        # Mirror analyticsProfile onto CentrePitch's Player doc (same DB)
        # Player.user = User ObjectId = player_id used in analytics
        try:
            await db.players.update_one(
                {"user": ObjectId(player_id)},
                {"$set": {"analyticsProfile": full_stats, "analyticsUpdatedAt": now}},
            )
        except Exception:
            pass  # player_id may not be a valid ObjectId (team format) — skip silently

    @staticmethod
    async def rebuild_all_profiles(
        db: AsyncIOMotorDatabase,
        delete_existing: bool = True,
    ) -> dict:
        """
        Optionally wipe `analytics_player_profiles`, then upsert every profile from
        `players.user`, `users._id`, and distinct `matchstats.participant.refId`.
        Stats are computed from matchstats (see `update_profile`).
        """
        if delete_existing:
            await db.analytics_player_profiles.delete_many({})

        player_ids: set[str] = set()
        async for p in db.players.find({}, {"user": 1}):
            u = p.get("user")
            if u is not None:
                player_ids.add(str(u))

        try:
            async for u in db.users.find({}, {"_id": 1}):
                if u.get("_id") is not None:
                    player_ids.add(str(u["_id"]))
        except Exception:
            pass

        for ref in await db.matchstats.distinct("participant.refId"):
            if ref is not None:
                player_ids.add(str(ref))

        rebuilt = 0
        errors: list[dict] = []
        for pid in sorted(player_ids):
            try:
                name = await PlayerService._resolve_player_name_from_players(db, pid)
                await PlayerService.update_profile(db, pid, name)
                rebuilt += 1
            except Exception as e:
                errors.append({"player_id": pid, "error": str(e)})

        return {
            "deleted_existing": delete_existing,
            "total_player_ids": len(player_ids),
            "profiles_upserted": rebuilt,
            "errors": errors[:50],
        }

    @staticmethod
    async def get_trends(db: AsyncIOMotorDatabase, player_id: str) -> dict:
        """Trends from matchstats match order + analytics_cache (key = str(centre match id))."""
        pm = await matchstats_participant_ref_match(db, player_id)

        pipeline = [
            {"$match": pm},
            {
                "$group": {
                    "_id": "$matchId",
                    "dt": {"$max": {"$ifNull": ["$date", "$createdAt"]}},
                }
            },
            {"$sort": {"dt": 1}},
        ]
        grouped = await db.matchstats.aggregate(pipeline).to_list(None)
        if not grouped:
            return {
                "total_matches": 0,
                "has_trend_data": False,
                "shot_win_rate_trend": {},
                "serve_win_rate_trend": [],
            }

        mids = [str(r["_id"]) for r in grouped if r.get("_id") is not None]
        by_mid = await fetch_participants_by_match_ids(db, mids)
        cache_docs = await db.analytics_cache.find(
            {
                "match_id": {"$in": mids},
                "module_name": {"$in": ["shot_effectiveness", "serve_analysis"]},
            }
        ).to_list(None)
        cache_map = {(str(c["match_id"]), c["module_name"]): c["result"] for c in cache_docs}

        # If serve/shot trends aren't cached yet, compute them on-demand for recent matches.
        # This keeps the player chart usable without requiring opening each match first.
        want = {"shot_effectiveness", "serve_analysis"}
        need_compute = [
            mid for mid in mids
            if any(cache_map.get((mid, mod)) is None for mod in want)
        ]
        # Cap to avoid heavy requests if a player has lots of matches.
        need_compute = need_compute[:50]
        if need_compute:
            keep = want
            disable = {
                "score_progression",
                "win_probability",
                "momentum",
                "turning_points",
                "rally_analysis",
                "pressure_analysis",
                "ending_analysis",
                "shot_sequences",
            }
            # Ensure we don't accidentally disable the modules we want.
            disable = set(disable) - set(keep)
            for mid in need_compute:
                recs = by_mid.get(mid, [])
                if not recs:
                    continue
                engine_data = await DataFetcher.build_engine_data(
                    db,
                    {"match_id": str(mid), "participants": recs},
                )
                if not engine_data or not engine_data.get("points"):
                    continue
                try:
                    results = run_all(engine_data, disabled_modules=disable)
                    # Cache only what we need (compute_and_cache will skip missing keys anyway)
                    await AnalyticsService.compute_and_cache(db, str(mid), results)
                    for mod in want:
                        if results.get(mod) is not None:
                            cache_map[(mid, mod)] = results.get(mod)
                except Exception:
                    # If compute fails, just leave this match out of the trend.
                    continue

        shot_trend: dict = {}
        serve_trend: list = []

        for mid in mids:
            recs = by_mid.get(mid, [])
            if not recs:
                continue
            parts = sorted(
                recs,
                key=lambda r: str((r.get("participant") or {}).get("refId") or ""),
            )
            pa_id = str((parts[0].get("participant") or {}).get("refId") or "")
            pb_id = (
                str((parts[1].get("participant") or {}).get("refId") or "")
                if len(parts) > 1
                else ""
            )
            is_a = str(player_id) == pa_id
            shot_key = "player_a" if is_a else "player_b"
            serve_key = "player_a_serving" if is_a else "player_b_serving"
            row0 = parts[0]
            dv = row0.get("date") or row0.get("createdAt")
            date_str = dv.isoformat() if isinstance(dv, datetime) else str(dv or "")

            shot_data = cache_map.get((mid, "shot_effectiveness"))
            if shot_data:
                for s in shot_data.get(shot_key, []):
                    if s["count"] >= 3:
                        st = s["shot_type"]
                        shot_trend.setdefault(st, []).append(
                            {
                                "date": date_str,
                                "match_id": mid,
                                "win_rate": s["win_rate_pct"],
                                "count": s["count"],
                            }
                        )

            serve_data = cache_map.get((mid, "serve_analysis"))
            if serve_data:
                sv = serve_data.get(serve_key, {})
                if sv.get("total", 0) >= 5:
                    serve_trend.append(
                        {
                            "date": date_str,
                            "match_id": mid,
                            "serve_win_pct": sv["win_pct"],
                            "total_serves": sv["total"],
                        }
                    )

        n = len(mids)
        return {
            "total_matches": n,
            "has_trend_data": n >= 6,
            "shot_win_rate_trend": shot_trend,
            "serve_win_rate_trend": serve_trend,
        }

    @staticmethod
    async def get_last10_trends(
        db: AsyncIOMotorDatabase,
        player_id: str,
        limit: int = 10,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> dict:
        """
        Last-N per-match trends (newest first):
        - Rolling point differential: points_for - points_against
        - Momentum stability: longest point streak for/against + lead changes (from point-by-point scores)
        - Rally length: avg rally_shots + short/long rally % (if rally_shots captured)

        Uses matchstats (teamTotals.sets / teamTotals.points), not analytics_sets / analytics_points.
        """
        pm = await matchstats_participant_ref_match(db, player_id)

        cnt_pipeline = [
            {"$match": pm},
            {"$group": {"_id": "$matchId", "dt": {"$max": {"$ifNull": ["$date", "$createdAt"]}}}},
        ]
        if date_from is not None or date_to is not None:
            dt_rng = {}
            if date_from is not None:
                dt_rng["$gte"] = date_from
            if date_to is not None:
                dt_rng["$lte"] = date_to
            cnt_pipeline.append({"$match": {"dt": dt_rng}})
        cnt_pipeline.append({"$count": "c"})
        cnt_rows = await db.matchstats.aggregate(cnt_pipeline).to_list(1)
        total = int(cnt_rows[0]["c"]) if cnt_rows else 0

        pipeline = [
            {"$match": pm},
            {
                "$group": {
                    "_id": "$matchId",
                    "dt": {"$max": {"$ifNull": ["$date", "$createdAt"]}},
                }
            },
        ]
        if date_from is not None or date_to is not None:
            dt_rng = {}
            if date_from is not None:
                dt_rng["$gte"] = date_from
            if date_to is not None:
                dt_rng["$lte"] = date_to
            pipeline.append({"$match": {"dt": dt_rng}})
        pipeline.extend([{"$sort": {"dt": -1}}, {"$limit": limit}])
        grouped = await db.matchstats.aggregate(pipeline).to_list(None)
        if not grouped:
            return {
                "player_id": player_id,
                "limit": limit,
                "total_matches": total,
                "matches": [],
                "summary": {},
            }

        mids = [str(r["_id"]) for r in grouped if r.get("_id") is not None]
        by_mid = await fetch_participants_by_match_ids(db, mids)

        results: list[dict] = []
        for mid in mids:
            recs = by_mid.get(mid, [])
            row = compute_last10_metrics_for_match(player_id, recs)
            if row:
                results.append(row)

        diffs = [r["point_diff"] for r in results if isinstance(r.get("point_diff"), int)]
        avg_diff = round(sum(diffs) / len(diffs), 2) if diffs else None

        lsf = [r["momentum"]["longest_streak_for"] for r in results]
        lsa = [r["momentum"]["longest_streak_against"] for r in results]
        lch = [r["momentum"]["lead_changes"] for r in results]
        avg_lsf = round(sum(lsf) / len(lsf), 2) if lsf else None
        avg_lsa = round(sum(lsa) / len(lsa), 2) if lsa else None
        avg_lch = round(sum(lch) / len(lch), 2) if lch else None

        rally_avgs = [r["rally"]["avg_rally_shots"] for r in results if r["rally"]["avg_rally_shots"] is not None]
        avg_rally = round(sum(rally_avgs) / len(rally_avgs), 2) if rally_avgs else None
        tracked = sum(int(r["rally"]["rallies_tracked"] or 0) for r in results)
        short_total = sum(int(r["rally"]["short_rallies"] or 0) for r in results)
        long_total = sum(int(r["rally"]["long_rallies"] or 0) for r in results)

        return {
            "player_id": player_id,
            "limit": limit,
            "total_matches": total,
            "matches": results,
            "summary": {
                "avg_point_diff": avg_diff,
                "avg_longest_streak_for": avg_lsf,
                "avg_longest_streak_against": avg_lsa,
                "avg_lead_changes": avg_lch,
                "avg_rally_shots": avg_rally,
                "rallies_tracked": tracked,
                "short_rally_pct": round((short_total / tracked) * 100, 1) if tracked else None,
                "long_rally_pct": round((long_total / tracked) * 100, 1) if tracked else None,
            },
        }

    @staticmethod
    async def get_h2h(db: AsyncIOMotorDatabase, player_id: str) -> list:
        """Head-to-head from matchstats (distinct matches, newest first for last_date)."""
        pm = await matchstats_participant_ref_match(db, player_id)

        pipeline = [
            {"$match": pm},
            {
                "$group": {
                    "_id": "$matchId",
                    "dt": {"$max": {"$ifNull": ["$date", "$createdAt"]}},
                }
            },
            {"$sort": {"dt": -1}},
        ]
        grouped = await db.matchstats.aggregate(pipeline).to_list(None)
        mids = [str(r["_id"]) for r in grouped if r.get("_id") is not None]
        if not mids:
            return []

        by_mid = await fetch_participants_by_match_ids(db, mids)

        h2h: dict[str, dict] = {}
        for mid in mids:
            recs = by_mid.get(mid, [])
            if not recs:
                continue
            disp = build_display_row(player_id, recs[0], recs)
            opp = disp.get("opponent_id")
            if not opp:
                continue
            won = bool(disp.get("won"))
            opp_name = disp.get("opponent_name")
            if opp not in h2h:
                h2h[opp] = {
                    "wins": 0,
                    "losses": 0,
                    "last_date": None,
                    "last_result": None,
                    "opponent_name": opp_name,
                }
            if won:
                h2h[opp]["wins"] += 1
            else:
                h2h[opp]["losses"] += 1
            if h2h[opp]["last_date"] is None:
                date_val = disp.get("date")
                h2h[opp]["last_date"] = (
                    date_val.isoformat()
                    if isinstance(date_val, datetime)
                    else str(date_val or "")
                )
                h2h[opp]["last_result"] = "win" if won else "loss"

        records = []
        for opp_id, d in h2h.items():
            total_m = d["wins"] + d["losses"]
            records.append(
                {
                    "opponent_id": opp_id,
                    "opponent_name": d.get("opponent_name"),
                    "wins": d["wins"],
                    "losses": d["losses"],
                    "matches": total_m,
                    "win_pct": round(d["wins"] / total_m * 100, 1),
                    "last_date": d["last_date"],
                    "last_result": d["last_result"],
                }
            )
        return sorted(records, key=lambda x: -x["matches"])

    @staticmethod
    async def compare(db: AsyncIOMotorDatabase, player_a_id: str, player_b_id: str) -> dict:
        a = await PlayerService.get_profile(db, player_a_id)
        b = await PlayerService.get_profile(db, player_b_id)
        return {
            "player_a": a.get("stats", {}) if a else {},
            "player_b": b.get("stats", {}) if b else {},
        }

    @staticmethod
    async def _get_signature_shot(db: AsyncIOMotorDatabase, player_id: str) -> str | None:
        pipeline = [
            {"$match": {"player_id": player_id}},
            {"$group": {"_id": "$shot_type", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1},
        ]
        result = await db.analytics_shots.aggregate(pipeline).to_list(1)
        return result[0]["_id"] if result else None

    @staticmethod
    async def _get_best_winning_shot(db: AsyncIOMotorDatabase, player_id: str) -> str | None:
        pipeline = [
            {"$match": {"player_id": player_id, "is_winning_shot": True}},
            {"$group": {"_id": "$shot_type", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1},
        ]
        result = await db.analytics_shots.aggregate(pipeline).to_list(1)
        return result[0]["_id"] if result else None

    @staticmethod
    def _match_summary_datetime(m: dict) -> datetime | None:
        dv = m.get("date")
        if dv is None:
            return None
        if isinstance(dv, datetime):
            return dv
        if isinstance(dv, str):
            try:
                return datetime.fromisoformat(dv.replace("Z", "+00:00"))
            except ValueError:
                return None
        return None

    @staticmethod
    def _filter_match_summaries_by_date(
        matches: list,
        date_from: datetime | None,
        date_to: datetime | None,
    ) -> list:
        if not date_from and not date_to:
            return matches
        out = []
        for m in matches:
            d = PlayerService._match_summary_datetime(m)
            if d is None:
                continue
            if date_from and d < date_from:
                continue
            if date_to and d > date_to:
                continue
            out.append(m)
        return out

    @staticmethod
    def _interpret_pressure_rating(rating: float | None) -> str | None:
        if rating is None:
            return None
        if rating > 0.05:
            return "Clutch — performs better under pressure"
        if rating < -0.05:
            return "Pressure-sensitive — performance drops in close games"
        return "Consistent — pressure has minimal effect"

    @staticmethod
    async def get_pressure_rating_payload(
        db: AsyncIOMotorDatabase,
        player_id: str,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> dict:
        """
        Pressure win rate minus overall win rate for matches in scope.
        When date_from/date_to are set, only those matches are used (selected analytics window).
        """
        matches = await load_match_summaries_for_player_profile(db, player_id)
        if not matches:
            query = {"$or": [{"player_a_id": player_id}, {"player_b_id": player_id}]}
            matches = await db.analytics_matches.find(query).sort("date", -1).to_list(None)
        if date_from or date_to:
            matches = PlayerService._filter_match_summaries_by_date(matches, date_from, date_to)
        if not matches:
            return {
                "player_id": player_id,
                "pressure_rating": None,
                "interpretation": None,
            }
        rating = await PlayerService._get_pressure_rating(db, player_id, matches)
        return {
            "player_id": player_id,
            "pressure_rating": rating,
            "interpretation": PlayerService._interpret_pressure_rating(rating),
        }

    @staticmethod
    async def _get_pressure_rating(
        db: AsyncIOMotorDatabase, player_id: str, matches: list
    ) -> float | None:
        if not matches:
            return None
        total_wins = sum(
            1
            for m in matches
            if m.get("winner_id") and str(m.get("winner_id")) == str(player_id)
        )
        overall_rate = total_wins / len(matches)

        match_ids = [str(m["_id"]) for m in matches]
        cache_docs = await db.analytics_cache.find({
            "match_id": {"$in": match_ids},
            "module_name": "pressure_analysis",
        }).to_list(None)
        cache_map = {str(c["match_id"]): c["result"] for c in cache_docs}

        pressure_wins = pressure_total = 0
        for m in matches:
            mid = str(m["_id"])
            cached = cache_map.get(mid)
            if not cached:
                continue
            pressure_total += int(cached.get("total_pressure_points", 0) or 0)
            key = (
                "a_wins"
                if str(m.get("player_a_id")) == str(player_id)
                else "b_wins"
            )
            pressure_wins += int(cached.get(key, 0) or 0)

        if pressure_total <= 0:
            return None

        pressure_rate = pressure_wins / pressure_total
        return pressure_rate - overall_rate


# ─── HELPERS ─────────────────────────────────────────────────────────────────

async def _build_engine_data(
    db: AsyncIOMotorDatabase, match_id: str, match_data: dict
) -> dict:
    """Assemble the analytics-engine-compatible data dict from stored points + shots."""
    info     = match_data["match_info"]
    all_pts  = await db.analytics_points.find({"match_id": match_id}).sort([
        ("set_number", 1), ("point_number", 1)
    ]).to_list(None)
    all_shots = await db.analytics_shots.find({"match_id": match_id}).to_list(None)

    points_data = [
        {
            "point_number":       global_n,          # 1-indexed across all sets
            "set_number":         p.get("set_number", 1),
            "set_point_number":   p["point_number"], # original within-set number
            "winner_id":          p["winner_id"],
            "point_winner":       p["winner_id"],
            "server":             p.get("server_id"),
            "rally_shots":        p.get("rally_shots"),
            "rally_duration_sec": p.get("rally_duration"),
            "ending_type":        p.get("ending_type"),
            "score_before":       p.get("score_before", "0-0"),
        }
        for global_n, p in enumerate(all_pts, start=1)
    ]
    shots_data = [
        {
            "player_id":       s["player_id"],
            "shot_type":       s["shot_type"],
            "shot_number":     s.get("shot_number", 0),
            "is_winning_shot": s.get("is_winning_shot", False),
            "prev_shot_type":  s.get("prev_shot_type"),
            "match_id":        match_id,
        }
        for s in all_shots
    ]
    return {
        "match_info": {
            "match_id":    match_id,
            "player_a_id": info["player_a"]["player_id"],
            "player_b_id": info["player_b"]["player_id"],
            "player_a":    info["player_a"],
            "player_b":    info["player_b"],
            "winner_id":   info.get("winner_id"),
        },
        "points": points_data,
        "shots":  shots_data,
    }


async def _get_disabled_modules(db: AsyncIOMotorDatabase) -> set[str]:
    """Load the set of admin-disabled module keys from analytics_modules collection."""
    docs = await db.analytics_modules.find({"enabled": False}, {"_id": 1}).to_list(None)
    return {d["_id"] for d in docs}
