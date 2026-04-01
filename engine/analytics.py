"""
Analytics Engine — module-based, self-declaring dependencies.

Each module:
  - declares required_fields (will be skipped if missing from data)
  - declares optional_fields (enriches output if present)
  - implements can_run(data) → bool
  - implements run(data) → dict
  - implements confidence(data) → float 0.0–1.0

Engine iterates all registered modules. Output always includes
which modules ran, which were skipped, and why.
"""
import math
from abc import ABC, abstractmethod
from collections import Counter, defaultdict
from typing import Any


# ─── BASE MODULE ─────────────────────────────────────────────────────────────

class AnalyticsModule(ABC):
    required_fields: list[str] = []
    optional_fields: list[str] = []
    output_key: str = ""

    def can_run(self, data: dict) -> bool:
        points = data.get("points", [])
        if not points:
            return False
        sample = points[0]
        return all(f in sample or f in data for f in self.required_fields)

    def confidence(self, data: dict) -> float:
        return 1.0

    @abstractmethod
    def run(self, data: dict) -> Any:
        pass


# ─── MODULE 1: SCORE PROGRESSION ─────────────────────────────────────────────

class ScoreProgressionModule(AnalyticsModule):
    required_fields = ["point_winner", "point_number"]
    optional_fields = ["ending_type", "rally_shots"]
    output_key = "score_progression"

    def run(self, data: dict) -> list:
        points = data["points"]
        player_a = data["match_info"]["player_a_id"]
        a, b = 0, 0           # cumulative match totals
        sa, sb = 0, 0         # within-set totals
        last_set = None
        result = []
        for p in sorted(points, key=lambda x: (x.get("set_number", 1), x["point_number"])):
            current_set = p.get("set_number", 1)
            if current_set != last_set:   # new set — reset per-set counters
                sa, sb = 0, 0
                last_set = current_set
            if p["winner_id"] == player_a:
                a += 1; sa += 1
            else:
                b += 1; sb += 1
            result.append({
                "point_number":  p["point_number"],
                "set_number":    current_set,
                "set_score_a":   sa,   # score within this set
                "set_score_b":   sb,
                "score_a":       a,    # cumulative across match (used by win prob charts)
                "score_b":       b,
                "score_diff":    a - b,
                "winner_id":     p["winner_id"],
                "ending_type":   p.get("ending_type"),
                "rally_shots":   p.get("rally_shots"),
            })
        return result


# ─── MODULE 2: WIN PROBABILITY ────────────────────────────────────────────────

class WinProbabilityModule(AnalyticsModule):
    required_fields = ["point_number", "winner_id"]
    output_key = "win_probability"

    def _sigmoid(self, x: float) -> float:
        return 1 / (1 + math.exp(-x))

    def run(self, data: dict) -> list:
        """
        Score-state logistic model.
        Replace self._predict() with LightGBM model.pkl later —
        the interface stays identical.
        """
        points = sorted(
            data["points"],
            key=lambda x: (x.get("set_number", 1), x["point_number"]),
        )
        player_a = data["match_info"]["player_a_id"]
        TARGET = 21
        a, b = 0, 0
        result = []

        for p in points:
            if p["winner_id"] == player_a:
                a += 1
            else:
                b += 1
            prob_a = self._predict(a, b, TARGET)
            result.append({
                "point_number": p["point_number"],
                "score_a": a,
                "score_b": b,
                "win_prob_a": round(prob_a, 3),
                "win_prob_b": round(1 - prob_a, 3),
            })
        return result

    def _predict(self, a: int, b: int, target: int) -> float:
        """
        Heuristic model. Swap for trained LightGBM with:
        features = [score_diff, set_completion_pct, is_server,
                    consecutive_a, consecutive_b]
        return float(model.predict([features])[0])
        """
        score_diff = a - b
        completion = (a + b) / (target * 2)
        return self._sigmoid(score_diff * 0.22 + completion * 0.4)


# ─── MODULE 3: MOMENTUM ──────────────────────────────────────────────────────

class MomentumModule(AnalyticsModule):
    required_fields = ["point_number", "winner_id"]
    output_key = "momentum"
    WINDOW = 5

    def run(self, data: dict) -> list:
        points = sorted(data["points"], key=lambda x: x["point_number"])
        player_a = data["match_info"]["player_a_id"]
        result = []
        for i, p in enumerate(points):
            window = points[max(0, i - self.WINDOW + 1): i + 1]
            a_in_window = sum(1 for w in window if w["winner_id"] == player_a)
            b_in_window = len(window) - a_in_window
            result.append({
                "point_number": p["point_number"],
                "a_momentum": a_in_window,
                "b_momentum": b_in_window,
                "dominant": "a" if a_in_window > b_in_window else
                            "b" if b_in_window > a_in_window else "neutral"
            })
        return result


# ─── MODULE 4: TURNING POINTS ────────────────────────────────────────────────

class TurningPointModule(AnalyticsModule):
    required_fields = ["point_number", "winner_id"]
    optional_fields = ["ending_type"]
    output_key = "turning_points"
    THRESHOLD = 0.05  # win prob shift that constitutes a turning point

    def run(self, data: dict) -> list:
        wp_module = WinProbabilityModule()
        win_probs = wp_module.run(data)
        points = {p["point_number"]: p for p in data["points"]}
        turning = []
        for i in range(1, len(win_probs)):
            shift = abs(win_probs[i]["win_prob_a"] - win_probs[i-1]["win_prob_a"])
            if shift >= self.THRESHOLD:
                pt = points.get(win_probs[i]["point_number"], {})
                turning.append({
                    "point_number": win_probs[i]["point_number"],
                    "score": f"{win_probs[i]['score_a']}-{win_probs[i]['score_b']}",
                    "win_prob_shift": round(shift, 3),
                    "direction": "a_gaining" if win_probs[i]["win_prob_a"] > win_probs[i-1]["win_prob_a"]
                                 else "b_gaining",
                    "ending_type": pt.get("ending_type"),
                })
        return sorted(turning, key=lambda x: -x["win_prob_shift"])[:5]


# ─── MODULE 5: RALLY ANALYSIS ────────────────────────────────────────────────

class RallyAnalysisModule(AnalyticsModule):
    # rally_duration_sec is video-only — only rally_shots is required from the scorer
    required_fields = ["rally_shots", "winner_id"]
    optional_fields = ["rally_duration_sec"]
    output_key = "rally_analysis"

    def _bucket(self, shots: int) -> str:
        if shots <= 4:   return "short (1-4)"
        if shots <= 9:   return "medium (5-9)"
        if shots <= 14:  return "long (10-14)"
        return "extended (15+)"

    def run(self, data: dict) -> dict:
        points = data["points"]
        player_a = data["match_info"]["player_a_id"]
        rallies = [p["rally_shots"] for p in points if p.get("rally_shots")]
        durations = [p["rally_duration_sec"] for p in points if p.get("rally_duration_sec")]
        bucket_stats = defaultdict(lambda: {"a": 0, "b": 0, "total": 0})
        for p in points:
            if not p.get("rally_shots"):
                continue
            b = self._bucket(p["rally_shots"])
            bucket_stats[b]["total"] += 1
            if p["winner_id"] == player_a:
                bucket_stats[b]["a"] += 1
            else:
                bucket_stats[b]["b"] += 1

        by_bucket = []
        for bucket, counts in sorted(bucket_stats.items()):
            t = counts["total"]
            by_bucket.append({
                "bucket": bucket,
                "total": t,
                "a_wins": counts["a"],
                "b_wins": counts["b"],
                "a_win_pct": round(counts["a"] / t * 100, 1) if t else 0,
                "b_win_pct": round(counts["b"] / t * 100, 1) if t else 0,
            })

        return {
            "avg_shots": round(sum(rallies) / len(rallies), 2) if rallies else 0,
            "avg_duration_sec": round(sum(durations) / len(durations), 2) if durations else 0,
            "max_rally": max(rallies) if rallies else 0,
            "min_rally": min(rallies) if rallies else 0,
            "total_shots_played": sum(rallies),
            "total_match_duration_sec": round(sum(durations), 1) if durations else 0,
            "by_bucket": by_bucket,
        }


# ─── MODULE 6: SHOT EFFECTIVENESS ────────────────────────────────────────────

class ShotEffectivenessModule(AnalyticsModule):
    """
    Requires shot-level data (Format B).
    Skipped gracefully if shots not present.
    """
    required_fields = ["shots"]      # top-level key in data
    output_key = "shot_effectiveness"

    def can_run(self, data: dict) -> bool:
        return bool(data.get("shots"))

    def run(self, data: dict) -> dict:
        shots = data["shots"]
        player_a = data["match_info"]["player_a_id"]
        stats_a = defaultdict(lambda: {"count": 0, "wins": 0})
        stats_b = defaultdict(lambda: {"count": 0, "wins": 0})

        for s in shots:
            target = stats_a if s["player_id"] == player_a else stats_b
            target[s["shot_type"]]["count"] += 1
            if s.get("is_winning_shot"):
                target[s["shot_type"]]["wins"] += 1

        def build(stats):
            result = []
            for shot, d in sorted(stats.items(), key=lambda x: -x[1]["count"]):
                t = d["count"]
                result.append({
                    "shot_type": shot,
                    "count": t,
                    "wins": d["wins"],
                    "win_rate_pct": round(d["wins"] / t * 100, 1) if t else 0,
                })
            return result

        return {"player_a": build(stats_a), "player_b": build(stats_b)}


# ─── MODULE 7: SERVE ANALYSIS ────────────────────────────────────────────────

class ServeAnalysisModule(AnalyticsModule):
    required_fields = ["server", "winner_id"]
    output_key = "serve_analysis"

    def run(self, data: dict) -> dict:
        points = data["points"]
        player_a = data["match_info"]["player_a_id"]
        player_b = data["match_info"]["player_b_id"]

        def stats(server_id, winner_id):
            serving = [p for p in points if p.get("server") == server_id]
            if not serving:
                return {"total": 0, "wins": 0, "win_pct": 0}
            wins = sum(1 for p in serving if p["winner_id"] == winner_id)
            return {
                "total": len(serving),
                "wins": wins,
                "win_pct": round(wins / len(serving) * 100, 1),
                "avg_rally": round(
                    sum(p.get("rally_shots", 0) for p in serving) / len(serving), 2
                ),
            }

        return {
            "player_a_serving": stats(player_a, player_a),
            "player_b_serving": stats(player_b, player_b),
        }


# ─── MODULE 8: PRESSURE ANALYSIS ─────────────────────────────────────────────

class PressureAnalysisModule(AnalyticsModule):
    required_fields = ["point_number", "winner_id"]
    output_key = "pressure_analysis"

    def run(self, data: dict) -> dict:
        points = sorted(data["points"], key=lambda x: x["point_number"])
        player_a = data["match_info"]["player_a_id"]
        a, b = 0, 0
        pressure_pts = []

        for p in points:
            if p["winner_id"] == player_a:
                a += 1
            else:
                b += 1
            diff = abs(a - b)
            is_pressure = diff <= 2 or (a >= 15 and b >= 15)
            if is_pressure:
                pressure_pts.append({
                    "point_number": p["point_number"],
                    "score": f"{a}-{b}",
                    "winner_id": p["winner_id"],
                    "ending_type": p.get("ending_type"),
                })

        a_wins = sum(1 for p in pressure_pts if p["winner_id"] == player_a)
        total = len(pressure_pts)
        return {
            "total_pressure_points": total,
            "a_wins": a_wins,
            "b_wins": total - a_wins,
            "a_win_pct": round(a_wins / total * 100, 1) if total else 0,
            "b_win_pct": round((total - a_wins) / total * 100, 1) if total else 0,
            "points": pressure_pts,
        }


# ─── MODULE 9: ENDING TYPE BREAKDOWN ─────────────────────────────────────────

class EndingTypeModule(AnalyticsModule):
    required_fields = ["ending_type", "winner_id"]
    output_key = "ending_analysis"

    def can_run(self, data: dict) -> bool:
        points = data.get("points", [])
        if not points:
            return False
        return any(p.get("ending_type") for p in points) and all(
            "winner_id" in p for p in points
        )

    def run(self, data: dict) -> list:
        points = data["points"]
        player_a = data["match_info"]["player_a_id"]
        by_ending = defaultdict(lambda: {"a": 0, "b": 0})
        for p in points:
            if not p.get("ending_type"):
                continue
            if p["winner_id"] == player_a:
                by_ending[p["ending_type"]]["a"] += 1
            else:
                by_ending[p["ending_type"]]["b"] += 1

        result = []
        for ending, counts in by_ending.items():
            total = counts["a"] + counts["b"]
            result.append({
                "ending_type": ending,
                "total": total,
                "a_wins": counts["a"],
                "b_wins": counts["b"],
                "pct_of_match": round(total / len(points) * 100, 1),
            })
        return sorted(result, key=lambda x: -x["total"])


# ─── MODULE 10: SHOT SEQUENCE PATTERNS (V2) ──────────────────────────────────

class ShotSequenceModule(AnalyticsModule):
    """V2 module — requires shot-level data with prev_shot_type populated."""
    required_fields = ["shots"]
    output_key = "shot_sequences"

    def can_run(self, data: dict) -> bool:
        shots = data.get("shots", [])
        return bool(shots) and any(s.get("prev_shot_type") for s in shots)

    def run(self, data: dict) -> dict:
        shots = data["shots"]
        player_a = data["match_info"]["player_a_id"]

        def sequences(player_id):
            player_shots = [s for s in shots if s["player_id"] == player_id]
            two_shot = defaultdict(lambda: {"count": 0, "wins": 0})
            for s in player_shots:
                if s.get("prev_shot_type"):
                    key = f"{s['prev_shot_type']} → {s['shot_type']}"
                    two_shot[key]["count"] += 1
                    if s.get("is_winning_shot"):
                        two_shot[key]["wins"] += 1
            result = []
            for seq, d in sorted(two_shot.items(), key=lambda x: -x[1]["count"])[:10]:
                t = d["count"]
                result.append({
                    "sequence": seq,
                    "count": t,
                    "win_rate_pct": round(d["wins"] / t * 100, 1) if t else 0,
                })
            return result

        return {
            "player_a": sequences(data["match_info"]["player_a_id"]),
            "player_b": sequences(data["match_info"]["player_b_id"]),
        }


# ─── ENGINE RUNNER ────────────────────────────────────────────────────────────

MODULES = [
    ScoreProgressionModule(),
    WinProbabilityModule(),
    MomentumModule(),
    TurningPointModule(),
    RallyAnalysisModule(),
    ShotEffectivenessModule(),
    ServeAnalysisModule(),
    PressureAnalysisModule(),
    EndingTypeModule(),
    ShotSequenceModule(),
]


def run_all(data: dict, disabled_modules: set[str] | None = None) -> dict:
    """
    Run every module. Skip those that cannot run or are admin-disabled.
    Always return which ran, which skipped, and why.

    disabled_modules: set of output_key strings to skip (admin-controlled).
    """
    disabled = disabled_modules or set()
    output = {
        "match_id": data["match_info"]["match_id"],
        "modules_run": [],
        "modules_skipped": [],
    }

    for module in MODULES:
        if module.output_key in disabled:
            output["modules_skipped"].append({
                "module": module.output_key,
                "reason": "disabled_by_admin",
            })
            continue
        if module.can_run(data):
            try:
                output[module.output_key] = module.run(data)
                output["modules_run"].append(module.output_key)
            except Exception as e:
                output["modules_skipped"].append({
                    "module": module.output_key,
                    "reason": f"runtime_error: {str(e)}"
                })
        else:
            output["modules_skipped"].append({
                "module": module.output_key,
                "reason": f"missing_required_fields: {module.required_fields}"
            })

    return output
