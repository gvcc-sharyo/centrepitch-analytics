"""
Service layer — all business logic lives here.
Routers call services. Services call MongoDB via Motor.
No business logic in routers. No file I/O in services.

Player identity: analytics player_id = CentrePitch User ObjectId (string).
Player names are stored inside analytics_player_profiles when a match is ingested.
analytics_players collection is NOT used — profiles are the single source for player data.
"""
import statistics
from datetime import datetime, timedelta
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase
from engine.analytics import run_all


# ─── MATCH SERVICE ────────────────────────────────────────────────────────────

class MatchService:

    @staticmethod
    async def create(db: AsyncIOMotorDatabase, match_data: dict) -> str:
        """
        Ingest a full match. Steps:
        1. Replace match doc            (idempotent)
        2. Replace sets                 (idempotent)
        3. Delete+insert points         (idempotent)
        4. Delete+insert shots          (idempotent, if present)
        5. Run analytics engine
        6. Cache each module result     (upsert)
        7. Update player profiles       (upsert into players.analyticsProfile)
        """
        info     = match_data["match_info"]
        match_id = info["match_id"]
        sets_won = info.get("sets_won", {})

        existing = await db.analytics_matches.find_one({"_id": match_id})
        # Optional FKs to CentrePitch; preserve on re-ingest if payload omits them.
        ce = info.get("centrepitch_event_id") or (existing or {}).get("centrepitch_event_id")
        cm = info.get("centrepitch_match_id") or (existing or {}).get("centrepitch_match_id")

        # 1. Upsert match doc
        doc = {
            "_id":         match_id,
            "player_a_id": info["player_a"]["player_id"],
            "player_b_id": info["player_b"]["player_id"],
            "player_a_name": info["player_a"].get("name", info["player_a"]["player_id"]),
            "player_b_name": info["player_b"].get("name", info["player_b"]["player_id"]),
            "winner_id":   info.get("winner_id"),
            "date":        datetime.fromisoformat(info["date"]),
            "venue":       info.get("venue"),
            "sport_id":    info.get("sport_id", "badminton"),
            "total_sets":  info.get("total_sets", 0),
            "sets_won_a":  sets_won.get("player_a", 0),
            "sets_won_b":  sets_won.get("player_b", 0),
            "ingested_at": datetime.utcnow(),
        }
        if ce:
            doc["centrepitch_event_id"] = ce
        if cm:
            doc["centrepitch_match_id"] = cm

        await db.analytics_matches.replace_one(
            {"_id": match_id},
            doc,
            upsert=True,
        )

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
    async def get(db: AsyncIOMotorDatabase, match_id: str) -> dict | None:
        return await db.analytics_matches.find_one({"_id": match_id})

    @staticmethod
    async def get_with_sets(db: AsyncIOMotorDatabase, match_id: str) -> dict | None:
        match = await db.analytics_matches.find_one({"_id": match_id})
        if not match:
            return None
        sets = await db.analytics_sets.find(
            {"match_id": match_id}
        ).sort("set_number", 1).to_list(None)
        match["sets"] = sets
        return match

    @staticmethod
    async def get_set_analytics_data(
        db: AsyncIOMotorDatabase, match_id: str, set_number: int
    ) -> dict | None:
        match = await db.analytics_matches.find_one({"_id": match_id})
        if not match:
            return None
        set_obj = await db.analytics_sets.find_one(
            {"match_id": match_id, "set_number": set_number}
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
                "set_number":        set_number,
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
        query = {}
        if player_id:
            query = {"$or": [{"player_a_id": player_id}, {"player_b_id": player_id}]}
        total   = await db.analytics_matches.count_documents(query)
        matches = await db.analytics_matches.find(query).sort("date", -1).skip(offset).limit(limit).to_list(None)
        return matches, total

    @staticmethod
    async def list_matches(
        db: AsyncIOMotorDatabase,
        search: str = "",
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list, int]:
        """
        List all matches (newest first). Optional search matches by:
        - match_id (_id) partial
        - player_a_id / player_b_id partial
        - player_a_name / player_b_name partial
        """
        query = {}
        if search:
            query = {
                "$or": [
                    {"_id": {"$regex": search, "$options": "i"}},
                    {"player_a_id": {"$regex": search, "$options": "i"}},
                    {"player_b_id": {"$regex": search, "$options": "i"}},
                    {"player_a_name": {"$regex": search, "$options": "i"}},
                    {"player_b_name": {"$regex": search, "$options": "i"}},
                ]
            }
        total = await db.analytics_matches.count_documents(query)
        matches = (
            await db.analytics_matches.find(
                query,
                {
                    "_id": 1,
                    "date": 1,
                    "player_a_id": 1,
                    "player_b_id": 1,
                    "player_a_name": 1,
                    "player_b_name": 1,
                    "winner_id": 1,
                    "sport_id": 1,
                    "total_sets": 1,
                    "sets_won_a": 1,
                    "sets_won_b": 1,
                    "centrepitch_event_id": 1,
                    "centrepitch_match_id": 1,
                },
            )
            .sort("date", -1)
            .skip(offset)
            .limit(limit)
            .to_list(None)
        )
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
        await db.analytics_cache.delete_many({"match_id": match_id})


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
    async def update_profile(db: AsyncIOMotorDatabase, player_id: str, name: str = ""):
        """Recompute rolling stats across all matches for this player."""
        query = {"$or": [{"player_a_id": player_id}, {"player_b_id": player_id}]}
        matches = await db.analytics_matches.find(query).sort("date", -1).to_list(None)

        if not matches:
            return

        total = len(matches)
        wins  = sum(1 for m in matches if m.get("winner_id") == player_id)
        win_rate = wins / total

        now = datetime.utcnow()

        def windowed(days):
            cut  = now - timedelta(days=days)
            pool = [m for m in matches if m.get("date", now) >= cut]
            if not pool:
                return win_rate
            return sum(1 for m in pool if m.get("winner_id") == player_id) / len(pool)

        win_rate_30d = windowed(30)
        win_rate_90d = windowed(90)

        form = prev_form = form_delta = None
        form_direction = "stable"
        if total >= 5:
            form = sum(1 for m in matches[:5] if m.get("winner_id") == player_id) / 5
        if total >= 10:
            prev_form = sum(1 for m in matches[5:10] if m.get("winner_id") == player_id) / 5
        if form is not None and prev_form is not None:
            form_delta     = round(form - prev_form, 3)
            form_direction = "up" if form_delta > 0.15 else "down" if form_delta < -0.15 else "stable"

        consistency_score = None
        ratios = []
        for m in matches[:10]:
            total_sets = m.get("total_sets", 0)
            if total_sets:
                sets_won = m["sets_won_a"] if m["player_a_id"] == player_id else m["sets_won_b"]
                ratios.append(sets_won / total_sets)
        if len(ratios) >= 2:
            std = statistics.pstdev(ratios)
            consistency_score = round(max(0.0, 1.0 - (std / 0.5)), 3)

        pressure_rating = await PlayerService._get_pressure_rating(db, player_id, matches)
        sig_shot        = await PlayerService._get_signature_shot(db, player_id)
        best_win_shot   = await PlayerService._get_best_winning_shot(db, player_id)
        sport_id        = matches[0].get("sport_id", "badminton") if matches else "badminton"

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
            "pressure_rating":   round(pressure_rating, 3),
        }

        # Upsert analytics_player_profiles (CA-1's own player stats)
        await db.analytics_player_profiles.update_one(
            {"_id": player_id},
            {"$set": {
                "_id":          player_id,
                "name":         name or player_id,
                "sport_id":     sport_id,
                "stats":        full_stats,
                "pressure_rating": round(pressure_rating, 3),
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
    async def get_trends(db: AsyncIOMotorDatabase, player_id: str) -> dict:
        query = {"$or": [{"player_a_id": player_id}, {"player_b_id": player_id}]}
        matches = await db.analytics_matches.find(query).sort("date", 1).to_list(None)
        if not matches:
            return {"total_matches": 0, "has_trend_data": False,
                    "shot_win_rate_trend": {}, "serve_win_rate_trend": []}

        match_ids  = [m["_id"] for m in matches]
        cache_docs = await db.analytics_cache.find({
            "match_id":   {"$in": match_ids},
            "module_name": {"$in": ["shot_effectiveness", "serve_analysis"]},
        }).to_list(None)
        cache_map = {(c["match_id"], c["module_name"]): c["result"] for c in cache_docs}

        shot_trend  = {}
        serve_trend = []

        for m in matches:
            is_a      = m["player_a_id"] == player_id
            shot_key  = "player_a" if is_a else "player_b"
            serve_key = "player_a_serving" if is_a else "player_b_serving"
            date_str  = m["date"].isoformat() if isinstance(m.get("date"), datetime) else str(m.get("date", ""))

            shot_data = cache_map.get((m["_id"], "shot_effectiveness"))
            if shot_data:
                for s in shot_data.get(shot_key, []):
                    if s["count"] >= 3:
                        st = s["shot_type"]
                        shot_trend.setdefault(st, []).append({
                            "date": date_str, "match_id": m["_id"],
                            "win_rate": s["win_rate_pct"], "count": s["count"],
                        })

            serve_data = cache_map.get((m["_id"], "serve_analysis"))
            if serve_data:
                sv = serve_data.get(serve_key, {})
                if sv.get("total", 0) >= 5:
                    serve_trend.append({
                        "date":          date_str,
                        "match_id":      m["_id"],
                        "serve_win_pct": sv["win_pct"],
                        "total_serves":  sv["total"],
                    })

        return {
            "total_matches":        len(matches),
            "has_trend_data":       len(matches) >= 6,
            "shot_win_rate_trend":  shot_trend,
            "serve_win_rate_trend": serve_trend,
        }

    @staticmethod
    async def get_last10_trends(db: AsyncIOMotorDatabase, player_id: str, limit: int = 10) -> dict:
        """
        Last-N per-match trends (newest first):
        - Rolling point differential: points_for - points_against
        - Momentum stability: longest point streak for/against + lead changes (from point-by-point scores)
        - Rally length: avg rally_shots + short/long rally % (if rally_shots captured)
        """
        matches, total = await MatchService.list_all(db, player_id=player_id, limit=limit, offset=0)
        if not matches:
            return {
                "player_id": player_id,
                "limit": limit,
                "total_matches": total,
                "matches": [],
                "summary": {},
            }

        results: list[dict] = []
        for m in matches:
            match_id = m.get("_id")
            if not match_id:
                continue

            is_a = m.get("player_a_id") == player_id
            opp_id = m.get("player_b_id") if is_a else m.get("player_a_id")

            # Point differential from final set scores (stable even if points data missing)
            sets = await db.analytics_sets.find({"match_id": match_id}).sort("set_number", 1).to_list(None)
            points_for = 0
            points_against = 0
            for s in sets:
                sa = int(s.get("score_a") or 0)
                sb = int(s.get("score_b") or 0)
                if is_a:
                    points_for += sa
                    points_against += sb
                else:
                    points_for += sb
                    points_against += sa

            pts = await db.analytics_points.find(
                {"match_id": match_id},
                {
                    "set_number": 1,
                    "point_number": 1,
                    "winner_id": 1,
                    "score_a_before": 1,
                    "score_b_before": 1,
                    "rally_shots": 1,
                },
            ).sort([("set_number", 1), ("point_number", 1)]).to_list(None)

            # Longest point streaks
            longest_for = 0
            longest_against = 0
            cur_for = 0
            cur_against = 0

            # Lead changes (ignoring ties)
            lead_changes = 0
            prev_leader = None

            # Rally length
            rally_shots_vals: list[int] = []
            short_rallies = 0  # 1–4 shots
            long_rallies = 0   # 15+ shots

            for p in pts:
                winner = p.get("winner_id")

                if winner == player_id:
                    cur_for += 1
                    cur_against = 0
                else:
                    cur_against += 1
                    cur_for = 0
                longest_for = max(longest_for, cur_for)
                longest_against = max(longest_against, cur_against)

                # Determine leader after point using score_before + winner side.
                sa = int(p.get("score_a_before") or 0)
                sb = int(p.get("score_b_before") or 0)
                if winner == m.get("player_a_id"):
                    sa2, sb2 = sa + 1, sb
                elif winner == m.get("player_b_id"):
                    sa2, sb2 = sa, sb + 1
                else:
                    sa2, sb2 = sa, sb

                if sa2 != sb2:
                    leader = "a" if sa2 > sb2 else "b"
                    if prev_leader is not None and leader != prev_leader:
                        lead_changes += 1
                    prev_leader = leader

                rs = p.get("rally_shots")
                if isinstance(rs, (int, float)) and rs > 0:
                    rs_i = int(rs)
                    rally_shots_vals.append(rs_i)
                    if 1 <= rs_i <= 4:
                        short_rallies += 1
                    if rs_i >= 15:
                        long_rallies += 1

            avg_rally_shots = round(sum(rally_shots_vals) / len(rally_shots_vals), 2) if rally_shots_vals else None

            results.append({
                "match_id": match_id,
                "date": m.get("date").isoformat() if isinstance(m.get("date"), datetime) else str(m.get("date", "")),
                "won": m.get("winner_id") == player_id,
                "opponent_id": opp_id,
                "points_for": points_for,
                "points_against": points_against,
                "point_diff": points_for - points_against,
                "momentum": {
                    "longest_streak_for": longest_for,
                    "longest_streak_against": longest_against,
                    "lead_changes": lead_changes,
                },
                "rally": {
                    "avg_rally_shots": avg_rally_shots,
                    "short_rallies": short_rallies,
                    "long_rallies": long_rallies,
                    "rallies_tracked": len(rally_shots_vals),
                },
            })

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
        query = {"$or": [{"player_a_id": player_id}, {"player_b_id": player_id}]}
        matches = await db.analytics_matches.find(query).sort("date", -1).to_list(None)

        h2h: dict[str, dict] = {}
        for m in matches:
            opp = m["player_b_id"] if m["player_a_id"] == player_id else m["player_a_id"]
            if opp not in h2h:
                h2h[opp] = {"wins": 0, "losses": 0, "last_date": None, "last_result": None,
                             "opponent_name": m.get("player_b_name") if m["player_a_id"] == player_id else m.get("player_a_name")}
            if m.get("winner_id") == player_id:
                h2h[opp]["wins"] += 1
            else:
                h2h[opp]["losses"] += 1
            if h2h[opp]["last_date"] is None:
                date_val = m.get("date")
                h2h[opp]["last_date"]   = date_val.isoformat() if isinstance(date_val, datetime) else str(date_val)
                h2h[opp]["last_result"] = "win" if m.get("winner_id") == player_id else "loss"

        records = []
        for opp_id, d in h2h.items():
            total = d["wins"] + d["losses"]
            records.append({
                "opponent_id":   opp_id,
                "opponent_name": d.get("opponent_name"),
                "wins":          d["wins"],
                "losses":        d["losses"],
                "matches":       total,
                "win_pct":       round(d["wins"] / total * 100, 1),
                "last_date":     d["last_date"],
                "last_result":   d["last_result"],
            })
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
    async def _get_pressure_rating(
        db: AsyncIOMotorDatabase, player_id: str, matches: list
    ) -> float:
        total_wins   = sum(1 for m in matches if m.get("winner_id") == player_id)
        overall_rate = total_wins / len(matches) if matches else 0

        match_ids  = [m["_id"] for m in matches]
        cache_docs = await db.analytics_cache.find({
            "match_id":    {"$in": match_ids},
            "module_name": "pressure_analysis",
        }).to_list(None)
        cache_map = {c["match_id"]: c["result"] for c in cache_docs}

        pressure_wins = pressure_total = 0
        for m in matches:
            cached = cache_map.get(m["_id"])
            if cached:
                pressure_total += cached.get("total_pressure_points", 0)
                key = "a_wins" if m["player_a_id"] == player_id else "b_wins"
                pressure_wins += cached.get(key, 0)

        pressure_rate = pressure_wins / pressure_total if pressure_total else overall_rate
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
