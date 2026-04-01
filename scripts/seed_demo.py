"""
seed_demo.py — Insert demo badminton match data directly into CA-1.

Usage (run from CA-1 directory):
    python scripts/seed_demo.py

What it creates:
  - 2 players: Arjun Mehta (P001) and Priya Nair (P002)
  - 3 matches across ~3 weeks, with full point-by-point data
  - Varied ending types, serve patterns, rally lengths

After running, open:
  http://localhost:8001/players          — list players
  http://localhost:8001/players/P001/profile
  http://localhost:8001/players/P002/profile
  http://localhost:8001/matches/DEMO_M1/full-analytics
  http://localhost:8001/matches/DEMO_M2/full-analytics
  http://localhost:8001/matches/DEMO_M3/full-analytics
"""

import asyncio
import sys
import os

# Allow running from the CA-1 root directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.mongo import connect, disconnect, get_db
from services.match_service import MatchService

# ─── PLAYER IDs ──────────────────────────────────────────────────────────────

ARJUN = "P001"
PRIYA = "P002"


# ─── POINT GENERATOR HELPERS ─────────────────────────────────────────────────

def _score(a, b):
    return f"{a}-{b}"


def _make_point(pid, set_number, pt_num, winner, server, score_before,
                ending_type, rally_shots):
    return {
        "point_id":     pid,
        "point_number": pt_num,
        "set_number":   set_number,
        "score_before": score_before,
        "point_winner": winner,
        "winner_id":    winner,
        "server":       server,
        "rally_shots":  rally_shots,
        "ending_type":  ending_type,
    }


def _build_set(match_id, set_number, rallies):
    """
    rallies: list of (winner, ending_type, rally_shots)
    Server is computed automatically: winner of a rally becomes server next rally.
    First server is Arjun (player_a) by default.
    Returns (set_doc, sa, sb)
    """
    set_id  = f"{match_id}-S{set_number}"
    points  = []
    sa, sb  = 0, 0
    server  = ARJUN   # Arjun serves first
    for i, (winner, ending_type, shots) in enumerate(rallies, start=1):
        pid          = f"{set_id}-P{i}"
        score_before = _score(sa, sb)
        points.append(_make_point(pid, set_number, i, winner, server,
                                  score_before, ending_type, shots))
        if winner == ARJUN:
            sa += 1
        else:
            sb += 1
        server = winner   # winner serves next in badminton

    set_winner = ARJUN if sa > sb else PRIYA
    is_deuce   = sa >= 20 and sb >= 20

    set_doc = {
        "set_id":       set_id,
        "set_number":   set_number,
        "score_a":      sa,
        "score_b":      sb,
        "winner_id":    set_winner,
        "is_deuce":     is_deuce,
        "total_points": len(points),
        "points":       points,
    }
    return set_doc, sa, sb


def _match_payload(match_id, date_iso, winner, sets_data):
    sets_won_a = sum(1 for s in sets_data if s["winner_id"] == ARJUN)
    sets_won_b = sum(1 for s in sets_data if s["winner_id"] == PRIYA)
    return {
        "match_info": {
            "match_id":    match_id,
            "sport_id":    "badminton",
            "date":        date_iso,
            "venue":       "GVCC Court 1",
            "player_a":    {"player_id": ARJUN, "name": "Arjun Mehta"},
            "player_b":    {"player_id": PRIYA, "name": "Priya Nair"},
            "player_a_id": ARJUN,
            "player_b_id": PRIYA,
            "winner_id":   winner,
            "sets_won":    {"player_a": sets_won_a, "player_b": sets_won_b},
            "total_sets":  len(sets_data),
        },
        "sets": sets_data,
        "points": [pt for s in sets_data for pt in s["points"]],
        "shots": [],
    }


# ─── MATCH 1: Arjun wins 2–0  (dominant) ─────────────────────────────────────
# Set 1: 21-14  Set 2: 21-17

def _match1():
    A, P = ARJUN, PRIYA
    sm, nk, fe, ue, dw = "smash_winner", "net_kill", "forced_error", "unforced_error", "drop_winner"
    # tuples: (winner, ending_type, rally_shots)  — server computed by _build_set

    # Set 1 — 35 rallies, Arjun wins 21-14
    s1 = [
        (A, sm, 3), (A, sm, 4), (P, fe, 6), (A, nk, 2), (A, sm, 5), (P, nk, 2),
        (P, ue, 8), (A, sm, 3), (A, dw, 7), (A, sm, 4), (P, fe, 5), (A, nk, 2),
        (A, sm, 6), (P, ue, 9), (A, sm, 3), (A, nk, 2), (P, fe, 4), (A, sm, 5),
        (A, dw, 8), (P, ue,11), (A, sm, 3), (A, sm, 4), (P, nk, 2), (A, sm, 6),
        (A, fe, 3), (P, ue, 7), (A, sm, 4), (A, nk, 2), (P, fe, 5), (A, dw, 9),
        (A, sm, 3), (P, ue, 8), (A, sm, 4), (A, nk, 2), (A, sm, 5),
    ]
    # Set 2 — 38 rallies, Arjun wins 21-17
    s2 = [
        (P, sm, 4), (A, sm, 3), (A, nk, 2), (P, dw, 6), (A, sm, 5), (A, fe, 4),
        (P, nk, 2), (A, sm, 6), (P, ue, 9), (A, sm, 3), (A, dw, 7), (P, fe, 4),
        (A, sm, 5), (P, ue, 8), (A, nk, 2), (A, sm, 4), (P, fe, 5), (A, sm, 3),
        (P, ue,10), (A, nk, 2), (A, sm, 6), (P, dw, 8), (A, sm, 4), (A, fe, 3),
        (P, ue, 7), (A, sm, 5), (P, nk, 2), (A, sm, 4), (P, fe, 6), (A, dw, 9),
        (A, sm, 3), (P, ue, 8), (A, sm, 4), (P, fe, 5), (A, nk, 2), (A, sm, 6),
        (P, ue, 7), (A, sm, 3),
    ]

    set1, _, _ = _build_set("DEMO_M1", 1, s1)
    set2, _, _ = _build_set("DEMO_M1", 2, s2)
    return _match_payload("DEMO_M1", "2026-03-01T10:00:00", ARJUN, [set1, set2])


# ─── MATCH 2: Priya wins 2–1  (close match) ──────────────────────────────────
# Set 1: Priya 21-19  Set 2: Arjun 21-18  Set 3: Priya 21-19

def _match2():
    A, P = ARJUN, PRIYA
    sm, nk, fe, ue, dw = "smash_winner", "net_kill", "forced_error", "unforced_error", "drop_winner"

    # Set 1 — Priya wins 21-19  (40 rallies)
    s1 = [
        (P, sm, 4), (A, sm, 3), (P, nk, 2), (A, dw, 6), (P, fe, 4), (A, sm, 5),
        (P, ue, 8), (P, sm, 3), (A, nk, 2), (P, dw, 7), (A, fe, 5), (P, sm, 4),
        (A, ue, 9), (P, nk, 2), (A, sm, 6), (P, fe, 4), (A, dw, 8), (P, sm, 3),
        (P, nk, 2), (A, ue,10), (P, sm, 5), (A, fe, 4), (P, dw, 7), (A, sm, 6),
        (P, ue, 8), (P, nk, 2), (A, sm, 4), (P, fe, 5), (A, dw, 9), (P, sm, 3),
        (A, ue, 7), (P, nk, 2), (A, sm, 5), (P, fe, 4), (A, dw, 8), (P, sm, 6),
        (A, ue, 9), (P, nk, 2), (A, sm, 4), (P, sm, 5),
    ]
    # Set 2 — Arjun wins 21-18  (39 rallies)
    s2 = [
        (A, sm, 4), (P, nk, 2), (A, dw, 6), (P, fe, 4), (A, sm, 5), (A, ue, 8),
        (P, sm, 3), (A, nk, 2), (P, dw, 7), (A, fe, 5), (P, sm, 4), (A, ue, 9),
        (A, nk, 2), (P, sm, 6), (A, fe, 4), (P, dw, 8), (A, sm, 3), (A, nk, 2),
        (P, ue,10), (A, sm, 5), (P, fe, 4), (A, dw, 7), (P, sm, 6), (A, ue, 8),
        (A, nk, 2), (P, sm, 4), (A, fe, 5), (P, dw, 9), (A, sm, 3), (P, ue, 7),
        (A, nk, 2), (P, sm, 5), (A, fe, 4), (P, dw, 8), (A, sm, 6), (P, ue, 9),
        (A, nk, 2), (A, sm, 4), (A, sm, 3),
    ]
    # Set 3 — Priya wins 21-19  (40 rallies)
    s3 = [
        (P, nk, 2), (A, sm, 4), (P, dw, 6), (A, fe, 4), (P, sm, 5), (A, ue, 8),
        (P, nk, 2), (A, dw, 7), (P, fe, 5), (A, sm, 4), (P, ue, 9), (P, nk, 2),
        (A, sm, 6), (P, fe, 4), (A, dw, 8), (P, sm, 3), (A, nk, 2), (P, ue,10),
        (A, sm, 5), (P, fe, 4), (A, dw, 7), (P, sm, 6), (A, ue, 8), (P, nk, 2),
        (A, sm, 4), (P, fe, 5), (A, dw, 9), (P, sm, 3), (A, ue, 7), (P, nk, 2),
        (A, sm, 5), (P, fe, 4), (A, dw, 8), (P, sm, 6), (A, ue, 9), (P, nk, 2),
        (A, sm, 4), (P, fe, 5), (A, dw, 8), (P, sm, 3),
    ]

    set1, _, _ = _build_set("DEMO_M2", 1, s1)
    set2, _, _ = _build_set("DEMO_M2", 2, s2)
    set3, _, _ = _build_set("DEMO_M2", 3, s3)
    return _match_payload("DEMO_M2", "2026-03-10T14:00:00", PRIYA, [set1, set2, set3])


# ─── MATCH 3: Arjun wins 2–1 (comeback) ────────────────────────────────────
# Set 1: Priya 21-15  Set 2: Arjun 21-16  Set 3: Arjun 21-18

def _match3():
    A, P = ARJUN, PRIYA
    sm, nk, fe, ue, dw = "smash_winner", "net_kill", "forced_error", "unforced_error", "drop_winner"

    # Set 1 — Priya wins 21-15  (36 rallies)
    s1 = [
        (P, sm, 3), (P, sm, 4), (A, fe, 5), (P, nk, 2), (A, sm, 6), (P, ue, 8),
        (P, sm, 3), (A, dw, 7), (P, fe, 4), (A, sm, 5), (P, ue, 9), (P, nk, 2),
        (A, sm, 4), (P, fe, 6), (A, dw, 8), (P, sm, 3), (P, nk, 2), (A, ue,10),
        (P, sm, 5), (A, fe, 4), (P, dw, 7), (A, sm, 6), (P, ue, 8), (P, nk, 2),
        (A, sm, 4), (P, fe, 5), (A, dw, 9), (P, sm, 3), (A, ue, 7), (P, nk, 2),
        (P, sm, 5), (A, fe, 4), (P, dw, 8), (A, sm, 6), (P, ue, 9), (P, sm, 3),
    ]
    # Set 2 — Arjun wins 21-16  (37 rallies)
    s2 = [
        (A, sm, 4), (A, nk, 2), (P, fe, 5), (A, sm, 6), (A, ue, 8), (P, nk, 2),
        (A, dw, 7), (P, fe, 4), (A, sm, 5), (A, ue, 9), (P, sm, 3), (A, nk, 2),
        (P, dw, 7), (A, fe, 5), (A, sm, 4), (P, ue, 9), (A, nk, 2), (A, sm, 6),
        (P, fe, 4), (A, dw, 8), (A, sm, 3), (P, ue, 7), (A, nk, 2), (P, sm, 5),
        (A, fe, 4), (A, dw, 9), (P, sm, 3), (A, ue, 7), (A, nk, 2), (P, sm, 5),
        (A, fe, 4), (A, sm, 6), (P, ue, 8), (A, nk, 2), (A, sm, 4), (A, dw, 7),
        (A, sm, 3),
    ]
    # Set 3 — Arjun wins 21-18  (39 rallies)
    s3 = [
        (P, sm, 4), (A, nk, 2), (P, dw, 6), (A, fe, 4), (A, sm, 5), (P, ue, 8),
        (A, nk, 2), (P, dw, 7), (A, fe, 5), (A, sm, 4), (P, ue, 9), (A, nk, 2),
        (P, sm, 6), (A, fe, 4), (P, dw, 8), (A, sm, 3), (P, nk, 2), (A, ue,10),
        (A, sm, 5), (P, fe, 4), (A, dw, 7), (P, sm, 6), (A, ue, 8), (A, nk, 2),
        (P, sm, 4), (A, fe, 5), (P, dw, 9), (A, sm, 3), (P, ue, 7), (A, nk, 2),
        (A, sm, 5), (P, fe, 4), (A, dw, 8), (A, sm, 6), (P, ue, 9), (A, nk, 2),
        (A, sm, 4), (P, fe, 5), (A, sm, 3),
    ]

    set1, _, _ = _build_set("DEMO_M3", 1, s1)
    set2, _, _ = _build_set("DEMO_M3", 2, s2)
    set3, _, _ = _build_set("DEMO_M3", 3, s3)
    return _match_payload("DEMO_M3", "2026-03-20T16:00:00", ARJUN, [set1, set2, set3])


# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def main():
    print("Connecting to MongoDB...")
    await connect()

    async for db in get_db():
        matches = [
            ("DEMO_M1", "Arjun wins 2–0 (dominant)",  _match1()),
            ("DEMO_M2", "Priya wins 2–1 (close)",     _match2()),
            ("DEMO_M3", "Arjun wins 2–1 (comeback)",  _match3()),
        ]

        for match_id, desc, payload in matches:
            print(f"\n  Ingesting {match_id}: {desc}...")
            try:
                result_id = await MatchService.create(db, payload)
                print(f"  ✓ {result_id} ingested")
            except Exception as e:
                print(f"  ✗ Failed: {e}")
                import traceback; traceback.print_exc()

        break  # generator yields once

    await disconnect()
    print("\nDone! Try these URLs:")
    print("  http://localhost:8001/players")
    print("  http://localhost:8001/players/P001/profile")
    print("  http://localhost:8001/players/P002/profile")
    print("  http://localhost:8001/matches/DEMO_M1/full-analytics")
    print("  http://localhost:8001/matches/DEMO_M2/full-analytics")
    print("  http://localhost:8001/matches/DEMO_M3/full-analytics")


if __name__ == "__main__":
    asyncio.run(main())
