"""
migrate.py — imports match data into the DB via the service layer.

Supports:
  - centrepitch_final.json: multi-match, multi-set format (current)
  - centrepitch_data.json:  multi-match, flat points (legacy, auto-detected)
  - data.json:              single match, flat points (old legacy)

Run:
  python migrate.py --input centrepitch_final.json
"""
import asyncio
import json
import argparse
import uuid
from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from db.models import Base
from services.match_service import MatchService


# ─── FORMAT CONVERTERS ────────────────────────────────────────────────────────

def convert_final_format(raw: dict) -> list[dict]:
    """
    Convert centrepitch_final.json:
    - matches[] → sets[] → points[]
    - Players identified by ID only (no name field in final format)
    """
    player_id_set = {p["id"] for p in raw.get("players", [])}

    results = []
    for match in raw["matches"]:
        player_a_id = match["player_a"]["id"]
        player_b_id = match["player_b"]["id"]
        winner_id   = match.get("match_winner")

        match_data = {
            "match_info": {
                "match_id":    match["match_id"],
                "sport_id":    match.get("sport_id", "badminton"),
                "date":        match.get("date", datetime.now().isoformat()),
                "venue":       match.get("venue"),
                "player_a":    {"player_id": player_a_id, "name": player_a_id},
                "player_b":    {"player_id": player_b_id, "name": player_b_id},
                "player_a_id": player_a_id,
                "player_b_id": player_b_id,
                "winner_id":   winner_id,
                "sets_won": {
                    "player_a": match["sets_won"]["player_a"],
                    "player_b": match["sets_won"]["player_b"],
                },
                "total_sets":  match.get("total_sets", len(match.get("sets", []))),
            },
            "sets":   [],
            "points": [],   # flat list across all sets (for analytics engine)
            "shots":  [],
        }

        global_point_counter = 1
        shot_counter = 1

        for s in match.get("sets", []):
            set_data = {
                "set_id":      s["set_id"],
                "set_number":  s["set_number"],
                "score_a":     s["score"]["player_a"],
                "score_b":     s["score"]["player_b"],
                "winner_id":   s.get("winner"),
                "is_deuce":    s.get("is_deuce", False),
                "total_points": s.get("total_points", len(s.get("points", []))),
                "points": [],
            }

            for pt in s.get("points", []):
                winner = pt["point_winner"]   # already a player ID in final format
                score_parts = pt["score_before"].split("-")

                point_entry = {
                    "point_id":          pt["point_id"],
                    "point_number":      pt["point_id"],          # 1-indexed within set
                    "global_point_number": global_point_counter,  # across the full match
                    "set_id":            s["set_id"],
                    "set_number":        s["set_number"],
                    "score_before":      pt["score_before"],
                    "server":            pt.get("server"),
                    "rally_shots":       pt.get("rally_shots"),
                    "rally_duration_sec": pt.get("rally_duration_sec"),
                    "point_winner":      winner,
                    "winner_id":         winner,
                    "ending_type":       pt.get("ending_type"),
                }
                set_data["points"].append(point_entry)
                match_data["points"].append(point_entry)
                global_point_counter += 1

                a_shots = pt.get("player_a_shot_types") or []
                b_shots = pt.get("player_b_shot_types") or []
                shot_counter = _append_shots(
                    match_data, pt["point_id"], match["match_id"], s["set_id"],
                    player_a_id, player_b_id, winner, a_shots, b_shots, shot_counter
                )

            match_data["sets"].append(set_data)

        results.append(match_data)

    return results


def convert_new_format(raw: dict) -> list[dict]:
    """Convert centrepitch_data.json: multi-match, flat points (no sets)."""
    player_id_map = {p["name"]: p["id"] for p in raw.get("players", []) if "name" in p}

    results = []
    for match in raw["matches"]:
        player_a_name = match["player_a"].get("name", match["player_a"].get("id", ""))
        player_b_name = match["player_b"].get("name", match["player_b"].get("id", ""))
        player_a_id = match["player_a"].get("id") or player_id_map.get(player_a_name, "PLR_" + player_a_name.replace(" ", "_").upper())
        player_b_id = match["player_b"].get("id") or player_id_map.get(player_b_name, "PLR_" + player_b_name.replace(" ", "_").upper())

        winner_name = match.get("winner")
        winner_id   = player_a_id if winner_name == player_a_name else player_b_id

        # Wrap flat points into a synthetic single set
        synthetic_set_id = f"{match['match_id']}-S1"
        points_raw = match.get("points", [])

        match_data = {
            "match_info": {
                "match_id":    match["match_id"],
                "sport_id":    match.get("sport_id", "badminton"),
                "date":        match.get("date", datetime.now().isoformat()),
                "venue":       match.get("venue"),
                "player_a":    {"player_id": player_a_id, "name": player_a_name},
                "player_b":    {"player_id": player_b_id, "name": player_b_name},
                "player_a_id": player_a_id,
                "player_b_id": player_b_id,
                "winner_id":   winner_id,
                "sets_won":    {"player_a": 1, "player_b": 0},
                "total_sets":  1,
            },
            "sets": [{
                "set_id":      synthetic_set_id,
                "set_number":  1,
                "score_a":     match["final_score"]["player_a"],
                "score_b":     match["final_score"]["player_b"],
                "winner_id":   winner_id,
                "is_deuce":    False,
                "total_points": len(points_raw),
                "points": [],
            }],
            "points": [],
            "shots":  [],
        }

        shot_counter = 1
        for pt in points_raw:
            winner = player_a_id if pt["point_winner"] == player_a_name else player_b_id
            raw_server = pt.get("server")
            server_id = (
                player_a_id if raw_server == player_a_name else
                player_b_id if raw_server == player_b_name else
                raw_server   # already an ID, or None
            )
            point_entry = {
                "point_id":     pt["point_id"],
                "point_number": pt["point_id"],
                "global_point_number": pt["point_id"],
                "set_id":       synthetic_set_id,
                "set_number":   1,
                "score_before": pt["score_before"],
                "server":       server_id,
                "rally_shots":  pt.get("rally_shots"),
                "rally_duration_sec": pt.get("rally_duration_sec"),
                "point_winner": pt["point_winner"],
                "winner_id":    winner,
                "ending_type":  pt.get("ending_type"),
            }
            match_data["sets"][0]["points"].append(point_entry)
            match_data["points"].append(point_entry)

            a_shots = pt.get("player_a_shot_types") or []
            b_shots = pt.get("player_b_shot_types") or []
            shot_counter = _append_shots(
                match_data, pt["point_id"], match["match_id"], synthetic_set_id,
                player_a_id, player_b_id, winner, a_shots, b_shots, shot_counter
            )

        results.append(match_data)

    return results


def convert_old_format(raw: dict) -> list[dict]:
    """Convert data.json: single match, flat points."""
    info = raw["match_info"]
    player_a_id = "PLR_" + info["player_A"].replace(" ", "_").upper()
    player_b_id = "PLR_" + info["player_B"].replace(" ", "_").upper()
    winner_id   = player_a_id if info["final_score"]["player_A"] > info["final_score"]["player_B"] else player_b_id
    match_id    = info.get("match_id", f"MATCH_{uuid.uuid4().hex[:8].upper()}")
    synthetic_set_id = f"{match_id}-S1"

    match_data = {
        "match_info": {
            "match_id":    match_id,
            "sport_id":    "badminton",
            "date":        datetime.now().isoformat(),
            "venue":       None,
            "player_a":    {"player_id": player_a_id, "name": info["player_A"]},
            "player_b":    {"player_id": player_b_id, "name": info["player_B"]},
            "player_a_id": player_a_id,
            "player_b_id": player_b_id,
            "winner_id":   winner_id,
            "sets_won":    {"player_a": 1, "player_b": 0},
            "total_sets":  1,
        },
        "sets": [{
            "set_id":      synthetic_set_id,
            "set_number":  1,
            "score_a":     info["final_score"]["player_A"],
            "score_b":     info["final_score"]["player_B"],
            "winner_id":   winner_id,
            "is_deuce":    False,
            "total_points": len(raw.get("points", [])),
            "points": [],
        }],
        "points": [],
        "shots":  [],
    }

    shot_counter = 1
    for pt in raw.get("points", []):
        winner = player_a_id if pt["point_winner"] == info["player_A"] else player_b_id
        point_entry = {
            "point_id":     pt["point_id"],
            "point_number": pt["point_id"],
            "global_point_number": pt["point_id"],
            "set_id":       synthetic_set_id,
            "set_number":   1,
            "score_before": pt["score_before"],
            "server":       pt.get("server"),
            "rally_shots":  pt.get("rally_shots"),
            "rally_duration_sec": pt.get("rally_duration_sec"),
            "point_winner": pt["point_winner"],
            "winner_id":    winner,
            "ending_type":  pt.get("ending_type"),
        }
        match_data["sets"][0]["points"].append(point_entry)
        match_data["points"].append(point_entry)

        a_shots = (pt.get("player_A_stats") or {}).get("shot_types", [])
        b_shots = (pt.get("player_B_stats") or {}).get("shot_types", [])
        shot_counter = _append_shots(
            match_data, pt["point_id"], match_id, synthetic_set_id,
            player_a_id, player_b_id, winner, a_shots, b_shots, shot_counter
        )

    return [match_data]


# ─── SHARED HELPERS ───────────────────────────────────────────────────────────

def _append_shots(match_data, point_id, match_id, set_id,
                  player_a_id, player_b_id, winner_id,
                  a_shots, b_shots, shot_counter) -> int:
    """Interleave player shots for a point and append to match_data['shots']."""
    rally = []
    max_len = max(len(a_shots), len(b_shots), 0)
    for i in range(max_len):
        if i < len(a_shots):
            rally.append({"player_id": player_a_id, "shot_type": a_shots[i], "idx": i * 2})
        if i < len(b_shots):
            rally.append({"player_id": player_b_id, "shot_type": b_shots[i], "idx": i * 2 + 1})
    rally.sort(key=lambda x: x["idx"])

    for j, shot in enumerate(rally):
        is_last = (j == len(rally) - 1)
        match_data["shots"].append({
            "shot_id":       shot_counter,
            "point_id":      point_id,
            "match_id":      match_id,
            "set_id":        set_id,
            "player_id":     shot["player_id"],
            "shot_number":   j + 1,
            "shot_type":     shot["shot_type"],
            "is_winning_shot": is_last and (shot["player_id"] == winner_id),
            "prev_shot_type": rally[j - 1]["shot_type"] if j > 0 else None,
            "next_shot_type": rally[j + 1]["shot_type"] if j < len(rally) - 1 else None,
            "landing_zone":  None,
            "speed_kmh":     None,
            "frame_number":  None,
        })
        shot_counter += 1

    return shot_counter


def detect_and_convert(raw: dict) -> list[dict]:
    """Auto-detect format and return a list of normalised match_data dicts."""
    if "matches" in raw:
        first = raw["matches"][0] if raw["matches"] else {}
        if "sets" in first:
            print("Detected format: centrepitch_final (multi-set)")
            return convert_final_format(raw)
        else:
            print("Detected format: centrepitch_data (flat points)")
            return convert_new_format(raw)
    elif "match_info" in raw:
        print("Detected format: legacy single-match")
        return convert_old_format(raw)
    else:
        raise ValueError("Unrecognised JSON format.")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def migrate(input_path: str):
    engine = create_async_engine("sqlite+aiosqlite:///./centre_pitch.db")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    with open(input_path) as f:
        raw = json.load(f)

    all_matches = detect_and_convert(raw)
    print(f"Found {len(all_matches)} match(es) to import.\n")

    async with AsyncSessionLocal() as db:
        for match_data in all_matches:
            mid = match_data["match_info"]["match_id"]
            n_sets   = len(match_data.get("sets", []))
            n_points = len(match_data["points"])
            n_shots  = len(match_data["shots"])
            print(f"Migrating: {mid}  |  {n_sets} set(s)  |  {n_points} points  |  {n_shots} shots")
            match_id = await MatchService.create(db, match_data)
            print(f"  Done → GET /matches/{match_id}/full-analytics")

    print("\nAll matches imported successfully.")
    await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to data JSON file")
    args = parser.parse_args()
    asyncio.run(migrate(args.input))
