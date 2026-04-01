"""
Helpers to aggregate matchstats rows (one document per participant per match)
into a single match-shaped dict for API responses.
"""
from __future__ import annotations

from collections import defaultdict
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

from services.backend_client import matchstats_participant_ref_match


def _oid(val):
    if val is None:
        return None
    if isinstance(val, ObjectId):
        return val
    s = str(val)
    try:
        if len(s) == 24:
            return ObjectId(s)
    except Exception:
        pass
    return val


def explorer_matchstats_filter(search: str | None) -> dict:
    """
    Match explorer lists one row per real match (matchId present).
    Optional search filters participant.name (same behaviour as before).
    """
    s = (search or "").strip()
    if not s:
        return {"matchId": {"$ne": None}}
    return {
        "$and": [
            {"matchId": {"$ne": None}},
            {"participant.name": {"$regex": s, "$options": "i"}},
        ]
    }


async def count_distinct_match_ids_for_explorer(
    db: AsyncIOMotorDatabase, search: str = "",
) -> int:
    """Total number of unique matches (distinct matchId) matching the explorer filter."""
    flt = explorer_matchstats_filter(search)
    pipeline = [
        {"$match": flt},
        {"$group": {"_id": "$matchId"}},
        {"$match": {"_id": {"$ne": None}}},
        {"$count": "c"},
    ]
    rows = await db.matchstats.aggregate(pipeline).to_list(1)
    return int(rows[0]["c"]) if rows else 0


async def paginate_distinct_match_ids_for_explorer(
    db: AsyncIOMotorDatabase,
    search: str = "",
    limit: int = 20,
    offset: int = 0,
) -> list[str]:
    """
    Ordered list of matchId strings (newest match activity first) for one explorer page.
    """
    flt = explorer_matchstats_filter(search)
    pipeline = [
        {"$match": flt},
        {
            "$group": {
                "_id": "$matchId",
                "dt": {"$max": {"$ifNull": ["$date", "$createdAt"]}},
            }
        },
        {"$sort": {"dt": -1}},
        {"$skip": offset},
        {"$limit": limit},
    ]
    rows = await db.matchstats.aggregate(pipeline).to_list(None)
    out: list[str] = []
    for r in rows:
        mid = r.get("_id")
        if mid is not None:
            out.append(str(mid))
    return out


async def fetch_participants_by_match_ids(
    db: AsyncIOMotorDatabase, match_ids: list,
) -> dict[str, list[dict]]:
    """Return map matchId(str) -> list of matchstat documents."""
    if not match_ids:
        return {}
    oids = []
    for mid in match_ids:
        oids.append(_oid(mid))
    cursor = db.matchstats.find({"matchId": {"$in": oids}})
    docs = await cursor.to_list(None)
    by_mid: dict[str, list[dict]] = defaultdict(list)
    for d in docs:
        mid = d.get("matchId")
        if mid is not None:
            by_mid[str(mid)].append(d)
    return dict(by_mid)


def _sets_from_team_totals(stat: dict | None) -> list[dict]:
    if not stat:
        return []
    return list(stat.get("teamTotals", {}).get("sets") or [])


def build_display_row(
    player_id: str,
    any_row: dict,
    participants: list[dict],
) -> dict:
    """
    Build one match row for GET /players/:id/matches using all participant rows for that match.
    """
    pid = str(player_id)
    # Order deterministically by refId
    parts = sorted(
        participants,
        key=lambda r: str((r.get("participant") or {}).get("refId") or ""),
    )
    pa = (parts[0].get("participant") or {}) if parts else {}
    pb = (parts[1].get("participant") or {}) if len(parts) > 1 else {}
    pa_id = str(pa.get("refId") or "")
    pb_id = str(pb.get("refId") or "")
    pa_name = pa.get("name") or pa_id
    pb_name = pb.get("name") or pb_id

    sets_a = _sets_from_team_totals(parts[0] if parts else None)
    if not sets_a and len(parts) > 1:
        sets_a = _sets_from_team_totals(parts[1])

    sets_out = []
    sets_won_a = 0
    sets_won_b = 0
    for s in sets_a:
        sn = s.get("set_number")
        sca = int(s.get("score_a") or 0)
        scb = int(s.get("score_b") or 0)
        w = s.get("winner")
        wid = str(w) if w is not None else None
        if wid == pa_id:
            sets_won_a += 1
        elif wid == pb_id:
            sets_won_b += 1
        sets_out.append({
            "set_number": sn,
            "score_a": sca,
            "score_b": scb,
            "winner_id": wid,
            "is_deuce": bool(s.get("is_deuce", False)),
        })

    winner_match = None
    if sets_won_a > sets_won_b:
        winner_match = pa_id
    elif sets_won_b > sets_won_a:
        winner_match = pb_id

    mid = any_row.get("matchId")
    return {
        # Use this participant's matchstat _id so GET /matches/:id resolves via DataFetcher
        "match_id": str(any_row.get("_id")),
        "date": any_row.get("date") or any_row.get("createdAt"),
        "won": (str(winner_match) == str(pid)) if winner_match else False,
        "player_a_id": pa_id,
        "player_b_id": pb_id,
        "player_a_name": pa_name,
        "player_b_name": pb_name,
        "opponent_id": pb_id if pid == pa_id else (pa_id if pid == pb_id else None),
        "opponent_name": pb_name if pid == pa_id else (pa_name if pid == pb_id else None),
        "sets_won_a": sets_won_a,
        "sets_won_b": sets_won_b,
        "total_sets": len(sets_out) or None,
        "sport_id": "badminton",
        "winner_id": winner_match,
        "centrepitch_event_id": any_row.get("event"),
        "centrepitch_match_id": str(mid) if mid is not None else None,
    }


async def load_match_summaries_for_player_profile(
    db: AsyncIOMotorDatabase, player_id: str
) -> list[dict]:
    """
    Build legacy-shaped match docs (newest first) from matchstats for rolling profile stats.
    _id is str(centre match id) so analytics_cache pressure_analysis keys align.
    """
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
    out: list[dict] = []
    for mid in mids:
        recs = by_mid.get(mid, [])
        if not recs:
            continue
        row = build_display_row(player_id, recs[0], recs)
        out.append(
            {
                "_id": str(mid),
                "player_a_id": row["player_a_id"],
                "player_b_id": row["player_b_id"],
                "winner_id": row["winner_id"],
                "date": row["date"],
                "total_sets": int(row.get("total_sets") or 0),
                "sets_won_a": row.get("sets_won_a", 0),
                "sets_won_b": row.get("sets_won_b", 0),
                "sport_id": row.get("sport_id", "badminton"),
            }
        )
    return out


def _parse_score_before(raw: str | None) -> tuple[int, int]:
    if not raw:
        return 0, 0
    s = str(raw).replace("–", "-").strip()
    parts = s.split("-", 1)
    if len(parts) < 2:
        return 0, 0
    try:
        return int(parts[0].strip()), int(parts[1].strip())
    except ValueError:
        return 0, 0


def compute_last10_metrics_for_match(player_id: str, participant_rows: list[dict]) -> dict | None:
    """
    Point diff + momentum + rally stats for one match from raw matchstats participant docs.
    Does not use analytics_sets / analytics_points.
    """
    if not participant_rows:
        return None

    pid = str(player_id)
    parts = sorted(
        participant_rows,
        key=lambda r: str((r.get("participant") or {}).get("refId") or ""),
    )
    pa = (parts[0].get("participant") or {}) if parts else {}
    pb = (parts[1].get("participant") or {}) if len(parts) > 1 else {}
    pa_id = str(pa.get("refId") or "")
    pb_id = str(pb.get("refId") or "")
    is_a = pid == pa_id

    sets_raw = _sets_from_team_totals(parts[0] if parts else None)
    if not sets_raw and len(parts) > 1:
        sets_raw = _sets_from_team_totals(parts[1])

    points_for = 0
    points_against = 0
    for s in sets_raw:
        sca = int(s.get("score_a") or 0)
        scb = int(s.get("score_b") or 0)
        if is_a:
            points_for += sca
            points_against += scb
        else:
            points_for += scb
            points_against += sca

    sets_won_a = sets_won_b = 0
    for s in sets_raw:
        w = s.get("winner")
        wids = str(w) if w is not None else None
        if wids == pa_id:
            sets_won_a += 1
        elif wids == pb_id:
            sets_won_b += 1

    winner_match = pa_id if sets_won_a > sets_won_b else (pb_id if sets_won_b > sets_won_a else None)
    won = (winner_match == pid) if winner_match else False

    pts_src = None
    for r in parts:
        pts = (r.get("teamTotals") or {}).get("points") or []
        if len(pts) > 0:
            pts_src = pts
            break

    longest_for = longest_against = 0
    cur_for = cur_against = 0
    lead_changes = 0
    prev_leader = None
    rally_shots_vals: list[int] = []
    short_rallies = long_rallies = 0

    if pts_src:
        pts_sorted = sorted(
            pts_src,
            key=lambda p: (
                int(p.get("set_number") or 1),
                int(p.get("point_id") or p.get("point_number") or 0),
            ),
        )
        for p in pts_sorted:
            w = str(p.get("point_winner") or p.get("winner_id") or p.get("winner") or "")
            if w == pid:
                cur_for += 1
                cur_against = 0
            elif w:
                cur_against += 1
                cur_for = 0
            longest_for = max(longest_for, cur_for)
            longest_against = max(longest_against, cur_against)

            sa, sb = _parse_score_before(p.get("score_before"))
            if w == pa_id:
                sa2, sb2 = sa + 1, sb
            elif w == pb_id:
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

    avg_rally_shots = (
        round(sum(rally_shots_vals) / len(rally_shots_vals), 2) if rally_shots_vals else None
    )

    row0 = parts[0]
    dt = row0.get("date") or row0.get("createdAt")
    date_str = dt.isoformat() if hasattr(dt, "isoformat") else str(dt or "")

    return {
        "match_id": str(row0.get("_id")),
        "date": date_str,
        "won": won,
        "opponent_id": pb_id if pid == pa_id else pa_id,
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
    }
