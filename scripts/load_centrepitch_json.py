"""
Load centrepitch_15players.json (or compatible) into CA-1 MongoDB via MatchService.create.

Usage (from CA-1 directory):
    python scripts/load_centrepitch_json.py
    python scripts/load_centrepitch_json.py --file centrepitch_15players.json --limit 5
    python scripts/load_centrepitch_json.py --file centrepitch_15players.json --all

Requires: MongoDB running, same MONGODB_URI as CA-1 (default mongodb://localhost:27017/centrepitch_local).

Match IDs in the file (e.g. MTH-BDM-20250106-001) are used as analytics_matches._id.
Open admin Match Explorer with that exact ID.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.mongo import connect, disconnect, get_db
from services.match_service import MatchService
from bson import ObjectId
from datetime import datetime


def _is_object_id(value: str) -> bool:
    try:
        ObjectId(str(value))
        return True
    except Exception:
        return False


def _point_doc(p: dict) -> dict:
    pn = p.get("point_number")
    if pn is None:
        pn = p.get("point_id")
    pn = int(pn) if pn is not None else 1
    pw = p.get("point_winner") or p.get("winner_id")
    return {
        "point_number": pn,
        "point_id": p.get("point_id", pn),
        "score_before": p.get("score_before", "0-0"),
        "server": p.get("server"),
        "rally_shots": p.get("rally_shots"),
        "rally_duration_sec": p.get("rally_duration_sec"),
        "point_winner": pw,
        "winner_id": pw,
        "ending_type": p.get("ending_type") or p.get("ending_shot"),
    }


def _set_doc(s: dict) -> dict:
    score = s.get("score") or {}
    sa = score.get("player_a", score.get("score_a", 0))
    sb = score.get("player_b", score.get("score_b", 0))
    pts = [_point_doc(p) for p in s.get("points") or []]
    return {
        "set_id": s["set_id"],
        "set_number": int(s["set_number"]),
        "score_a": int(sa),
        "score_b": int(sb),
        "winner_id": s.get("winner"),
        "is_deuce": bool(s.get("is_deuce", False)),
        "total_points": int(s.get("total_points", len(pts))),
        "points": pts,
    }


def _rewrite_point_ids(p: dict, mapping: dict[str, str]) -> dict:
    out = dict(p)
    for key in ("server", "point_winner", "winner_id"):
        if out.get(key) in mapping:
            out[key] = mapping[out[key]]
    return out


def _rewrite_set_ids(s: dict, mapping: dict[str, str]) -> dict:
    out = dict(s)
    if out.get("winner_id") in mapping:
        out["winner_id"] = mapping[out["winner_id"]]
    pts = out.get("points") or []
    out["points"] = [_rewrite_point_ids(p, mapping) for p in pts]
    return out


def match_to_payload(m: dict, *, player_id_map: dict[str, str] | None = None) -> dict:
    pa = m["player_a"]["id"] if isinstance(m.get("player_a"), dict) else m["player_a"]
    pb = m["player_b"]["id"] if isinstance(m.get("player_b"), dict) else m["player_b"]
    sw = m.get("sets_won") or {}
    sets_out = [_set_doc(s) for s in m.get("sets") or []]
    date = m.get("date", "2026-01-01")
    if "T" not in str(date):
        date = f"{date}T12:00:00"
    sid = m.get("sport_id")
    if isinstance(sid, str) and sid.upper().startswith("SPT"):
        sport_id = "badminton"
    else:
        sport_id = (sid or "badminton") if isinstance(sid, str) else "badminton"

    mapping = player_id_map or {}
    pa_out = mapping.get(pa, pa)
    pb_out = mapping.get(pb, pb)
    winner_raw = m.get("match_winner") or m.get("winner_id")
    winner_out = mapping.get(winner_raw, winner_raw)
    sets_out = [_rewrite_set_ids(s, mapping) for s in sets_out]

    return {
        "match_info": {
            "match_id": m["match_id"],
            "sport_id": sport_id,
            "date": date,
            "venue": m.get("venue"),
            "player_a": {"player_id": pa_out, "name": m.get("player_a_name", pa)},
            "player_b": {"player_id": pb_out, "name": m.get("player_b_name", pb)},
            "winner_id": winner_out,
            "sets_won": {
                "player_a": int(sw.get("player_a", 0)),
                "player_b": int(sw.get("player_b", 0)),
            },
            "total_sets": int(m.get("total_sets", len(sets_out))),
        },
        "sets": sets_out,
        "shots": m.get("shots") or [],
    }


async def seed_player_profiles(db, player_ids: list[str], sport_id: str = "badminton") -> None:
    """
    Ensure the admin Players list can show the full roster even if some players
    have no matches ingested yet. CA-1 lists from analytics_player_profiles.
    """
    for pid in player_ids:
        await db.analytics_player_profiles.update_one(
            {"_id": pid},
            {"$setOnInsert": {
                "_id": pid,
                "name": pid,
                "sport_id": sport_id,
                "stats": {
                    "total_matches": 0,
                    "total_wins": 0,
                    "total_losses": 0,
                    "win_rate_overall": 0.0,
                    "win_rate_30d": 0.0,
                    "win_rate_90d": 0.0,
                },
                "pressure_rating": 0.0,
            }},
            upsert=True,
        )


async def seed_centrepitch_users_and_players(
    db,
    legacy_player_ids: list[str],
    *,
    sport_id: str = "badminton",
) -> dict[str, str]:
    """
    Create CentrePitch `users` (role=player) + `players` docs so the platform's
    Admin -> All Players list (`GET /api/admin/players`) shows these demo players.

    Returns mapping: legacy_player_id -> centrepitch_user_objectid_string
    """
    mapping: dict[str, str] = {}

    # Reuse existing mapping if present in analytics_player_profiles
    existing = await db.analytics_player_profiles.find(
        {"_id": {"$in": legacy_player_ids}},
        {"_id": 1, "centrepitch_user_id": 1},
    ).to_list(None)
    for doc in existing:
        cid = doc.get("centrepitch_user_id")
        if cid:
            mapping[str(doc["_id"])] = str(cid)

    for legacy in legacy_player_ids:
        if legacy in mapping:
            continue

        user_id = ObjectId()
        mapping[legacy] = str(user_id)

        # Create a User doc. We set googleId so mongoose password-required validator is bypassed.
        email = f"{legacy.lower()}@demo.local"
        name_num = legacy.split("-")[-1] if "-" in legacy else legacy
        first = "Player"
        last = str(name_num)

        await db.users.update_one(
            {"_id": user_id},
            {"$setOnInsert": {
                "_id": user_id,
                "firstName": first,
                "lastName": last,
                "email": email,
                "googleId": f"demo:{legacy}",
                "role": "player",
                "baseRole": "player",
                "activeRole": "player",
                "isActive": True,
                "isVerified": False,
                "isEmailVerified": False,
                "isPhoneVerified": False,
                "createdAt": datetime.utcnow(),
            }},
            upsert=True,
        )

        # Create a Player doc so CA-1 can mirror analyticsProfile onto it later (optional, but useful).
        await db.players.update_one(
            {"user": user_id},
            {"$setOnInsert": {
                "user": user_id,
                "name": f"{first} {last}",
                "sports": [],
                "status": "active",
                "isActive": True,
            }},
            upsert=True,
        )

        # Store mapping on the analytics profile for traceability.
        await db.analytics_player_profiles.update_one(
            {"_id": legacy},
            {"$set": {"centrepitch_user_id": str(user_id), "sport_id": sport_id}},
            upsert=True,
        )

    return mapping


async def run_load(path: str, limit: int | None) -> None:
    print(f"Reading {path}...")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    matches = data.get("matches") or []
    players = data.get("players") or []
    player_ids = []
    for p in players:
        if isinstance(p, dict) and p.get("id"):
            player_ids.append(str(p["id"]))
        elif isinstance(p, str):
            player_ids.append(p)
    if limit is not None:
        matches = matches[:limit]
    print(f"Ingesting {len(matches)} match(es)...")

    await connect()
    try:
        async for db in get_db():
            if player_ids:
                await seed_player_profiles(db, player_ids)
            # If these are legacy IDs (not ObjectIds), also seed CentrePitch users so /admin/players shows them.
            player_id_map = {}
            if player_ids and any(not _is_object_id(pid) for pid in player_ids):
                player_id_map = await seed_centrepitch_users_and_players(db, player_ids)
            for i, m in enumerate(matches, start=1):
                mid = m.get("match_id", f"#{i}")
                try:
                    payload = match_to_payload(m, player_id_map=player_id_map)
                    await MatchService.create(db, payload)
                    print(f"  [{i}/{len(matches)}] OK {mid}")
                except Exception as e:
                    print(f"  [{i}/{len(matches)}] FAIL {mid}: {e}")
            break
    finally:
        await disconnect()

    print("\nDone. Example Match Explorer IDs:")
    for m in matches[:5]:
        print(f"  {m.get('match_id')}")


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    default_file = os.path.join(root, "centrepitch_15players.json")
    ap = argparse.ArgumentParser(description="Ingest CentrePitch JSON into CA-1 MongoDB")
    ap.add_argument("--file", "-f", default=default_file, help="Path to JSON file")
    ap.add_argument("--limit", "-n", type=int, default=None, help="Max matches to load (default: 10 for safety)")
    ap.add_argument("--all", action="store_true", help="Load all matches in file")
    args = ap.parse_args()
    path = os.path.abspath(args.file)
    if not os.path.isfile(path):
        print(f"File not found: {path}", file=sys.stderr)
        sys.exit(1)
    limit = None if args.all else (args.limit if args.limit is not None else 10)
    asyncio.run(run_load(path, limit))


if __name__ == "__main__":
    main()
