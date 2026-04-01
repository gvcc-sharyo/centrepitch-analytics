"""
backfill_centrepitch.py
Ingest all completed CentrePitch matches into CA-1 using the real player ObjectIds.

Reads directly from MongoDB:
  - events collection  → match list, player IDs, scores, dates
  - matchstats collection → point-by-point data (if scorer entered it)

Usage (run from CA-1 directory):
    python scripts/backfill_centrepitch.py [--clean]

  --clean   Delete existing analytics_* data before ingesting (fresh start)
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.mongo import connect, disconnect, get_db
from services.match_service import MatchService


def _parse_score_str(score_str: str):
    """
    Parse 'score_a-score_b' into (int, int).
    Handles '21-15', '21-15, 21-16' (takes first), '0-0', etc.
    """
    first = str(score_str or "0-0").split(",")[0].strip()
    parts = first.split("-")
    try:
        return int(parts[0]), int(parts[1])
    except Exception:
        return 0, 0


async def _backfill(db, clean: bool):
    if clean:
        print("  Cleaning existing analytics data...")
        for col in ["analytics_matches", "analytics_sets", "analytics_points",
                    "analytics_shots", "analytics_cache", "analytics_player_profiles"]:
            await db[col].delete_many({})
        print("  Done.")

    # ── Load all events with embedded matches ─────────────────────────────────
    events = await db.events.find({}).to_list(None)
    print(f"\n  Found {len(events)} event(s).")

    # ── Build a lookup: matchId → list of MatchStat docs ─────────────────────
    all_stats = await db.matchstats.find({}).to_list(None)
    stats_by_match: dict[str, list] = {}
    for s in all_stats:
        key = str(s["matchId"])
        stats_by_match.setdefault(key, []).append(s)

    ingested = 0
    skipped  = 0

    for event in events:
        sport_id  = (event.get("eventType") or "badminton").lower()
        is_ind    = event.get("gameFormat") in ("individual", "doubles")
        venue     = event.get("location", {}).get("venue")
        event_id  = str(event["_id"])

        for rnd in event.get("schedule", []):
            for match in rnd.get("matches", []):
                result = match.get("result", {})
                if result.get("status") != "completed":
                    skipped += 1
                    continue

                match_id_obj = str(match["_id"])
                ca1_match_id = f"MATCH_{match_id_obj}"

                if is_ind:
                    player_a_id = str(match.get("player1", ""))
                    player_b_id = str(match.get("player2", ""))
                else:
                    player_a_id = str(match.get("team1", ""))
                    player_b_id = str(match.get("team2", ""))

                winner_id = str(result.get("winner", "") or "")
                date_iso  = (match.get("dateTime") or event.get("startDate")).isoformat()

                # Look up MatchStat docs for this match
                match_stats = stats_by_match.get(match_id_obj, [])
                stat_a = next((s for s in match_stats if str(s["participant"]["refId"]) == player_a_id), None)
                stat_b = next((s for s in match_stats if str(s["participant"]["refId"]) == player_b_id), None)

                player_a_name = (stat_a or {}).get("participant", {}).get("name", player_a_id)
                player_b_name = (stat_b or {}).get("participant", {}).get("name", player_b_id)

                # Build sets and points from MatchStat teamTotals
                sets, points, sets_won_a, sets_won_b = _build_match_data(
                    ca1_match_id, player_a_id, player_b_id, winner_id,
                    stat_a, stat_b, match
                )

                payload = {
                    "match_info": {
                        "match_id":    ca1_match_id,
                        "sport_id":    sport_id,
                        "date":        date_iso,
                        "venue":       match.get("venue") or venue,
                        "player_a":    {"player_id": player_a_id, "name": player_a_name},
                        "player_b":    {"player_id": player_b_id, "name": player_b_name},
                        "player_a_id": player_a_id,
                        "player_b_id": player_b_id,
                        "winner_id":   winner_id,
                        "sets_won":    {"player_a": sets_won_a, "player_b": sets_won_b},
                        "total_sets":  len(sets),
                    },
                    "sets":   sets,
                    "points": points,
                    "shots":  [],
                }

                try:
                    await MatchService.create(db, payload)
                    ingested += 1
                    print(f"  ✓ {ca1_match_id}  {player_a_name} vs {player_b_name}  "
                          f"({sets_won_a}-{sets_won_b})")
                except Exception as e:
                    print(f"  ✗ {ca1_match_id}: {e}")

    print(f"\n  Ingested: {ingested}  |  Skipped (not completed): {skipped}")


def _build_match_data(ca1_match_id, player_a_id, player_b_id, winner_id,
                      stat_a, stat_b, match_doc):
    """
    Build sets + points lists from MatchStat teamTotals.
    stat_a has priority for point data (scorer typically fills from one side).
    Falls back to sets-only from the score string if no teamTotals.sets.
    """
    # Try structured sets from teamTotals (either stat can have it)
    raw_sets = None
    for stat in [stat_a, stat_b]:
        if stat and stat.get("teamTotals", {}).get("sets"):
            raw_sets = stat["teamTotals"]["sets"]
            break

    # Try structured points
    raw_points = None
    for stat in [stat_a, stat_b]:
        if stat and stat.get("teamTotals", {}).get("points"):
            raw_points = stat["teamTotals"]["points"]
            break

    if raw_sets:
        return _from_structured(ca1_match_id, player_a_id, player_b_id,
                                raw_sets, raw_points)
    else:
        # Minimal fallback — only score string available
        return _from_score_string(ca1_match_id, player_a_id, player_b_id,
                                  winner_id, match_doc)


def _from_structured(ca1_match_id, player_a_id, player_b_id, raw_sets, raw_points):
    # Group points by set_number
    pts_by_set: dict[int, list] = {}
    for pt in (raw_points or []):
        sn = pt.get("set_number", 1)
        pts_by_set.setdefault(sn, []).append(pt)

    sets_out   = []
    points_out = []
    sets_won_a = 0
    sets_won_b = 0
    server     = player_a_id  # player_a serves first per set

    for s in raw_sets:
        sn     = s.get("set_number", 1)
        set_id = f"{ca1_match_id}-S{sn}"
        sa     = s.get("score_a", 0)
        sb     = s.get("score_b", 0)

        # winner stored as string ID in scorer data
        raw_winner = str(s.get("winner") or "")
        set_winner = raw_winner if raw_winner in (player_a_id, player_b_id) else (
            player_a_id if sa > sb else player_b_id
        )

        if set_winner == player_a_id:
            sets_won_a += 1
        else:
            sets_won_b += 1

        set_pts_raw = pts_by_set.get(sn, [])
        set_pts = []
        score_a, score_b = 0, 0
        current_server   = player_a_id  # reset each set

        for i, pt in enumerate(set_pts_raw, start=1):
            pt_winner = str(pt.get("point_winner") or pt.get("winner_id") or "")
            set_pts.append({
                "point_id":     i,
                "point_number": i,
                "set_number":   sn,
                "score_before": f"{score_a}-{score_b}",
                "point_winner": pt_winner,
                "winner_id":    pt_winner,
                "server":       str(pt.get("server") or current_server),
                "rally_shots":  pt.get("rally_shots"),
                "rally_duration_sec": pt.get("rally_duration_sec"),
                "ending_type":  pt.get("ending_type") or pt.get("ending_shot"),
            })
            if pt_winner == player_a_id:
                score_a += 1
            elif pt_winner == player_b_id:
                score_b += 1
            current_server = pt_winner or current_server  # winner serves next

        sets_out.append({
            "set_id":       set_id,
            "set_number":   sn,
            "score_a":      sa,
            "score_b":      sb,
            "winner_id":    set_winner,
            "is_deuce":     s.get("is_deuce", False),
            "total_points": len(set_pts),
            "points":       set_pts,
        })
        points_out.extend(set_pts)

    return sets_out, points_out, sets_won_a, sets_won_b


def _from_score_string(ca1_match_id, player_a_id, player_b_id, winner_id, match_doc):
    """Minimal fallback using only the score string from match result."""
    score_str  = str(match_doc.get("result", {}).get("score") or "0-0")
    set_scores = [p.strip() for p in score_str.split(",")]

    sets_out   = []
    sets_won_a = 0
    sets_won_b = 0

    for sn, sc in enumerate(set_scores, start=1):
        sa, sb   = _parse_score_str(sc)
        set_id   = f"{ca1_match_id}-S{sn}"
        winner   = player_a_id if sa > sb else player_b_id
        if winner == player_a_id:
            sets_won_a += 1
        else:
            sets_won_b += 1

        sets_out.append({
            "set_id":       set_id,
            "set_number":   sn,
            "score_a":      sa,
            "score_b":      sb,
            "winner_id":    winner,
            "is_deuce":     max(sa, sb) > 21,
            "total_points": 0,
            "points":       [],
        })

    return sets_out, [], sets_won_a, sets_won_b


async def main():
    clean = "--clean" in sys.argv
    print("Connecting to MongoDB...")
    await connect()

    async for db in get_db():
        await _backfill(db, clean)
        break

    await disconnect()

    print("\nDone! Players with real IDs now have analytics profiles.")
    print("Check: http://localhost:8001/players")


if __name__ == "__main__":
    asyncio.run(main())
