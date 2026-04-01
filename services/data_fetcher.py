"""
Data fetcher service - Fetches raw data from source collections (matchstats, players).
No data migration needed. Transforms source data on-the-fly for analytics computation.

Source of truth:
- matchstats → match data and point-level analytics
- players → player information
"""
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId


def _sid(v) -> str:
    """Normalize ObjectId or any id to string for engine + UI comparisons."""
    if v is None:
        return ""
    if isinstance(v, ObjectId):
        return str(v)
    return str(v)


class DataFetcher:
    """Fetch and transform data from source collections for analytics computation."""

    @staticmethod
    async def get_match_with_participants(db: AsyncIOMotorDatabase, match_id: str):
        """
        Fetch a match and its participants from matchstats.
        Returns the actual MongoDB match ObjectId and participant records.
        """
        try:
            # Try as ObjectId first
            try:
                match_id_obj = ObjectId(match_id)
            except:
                match_id_obj = match_id

            # Find any matchstat record for this match
            matchstat = await db.matchstats.find_one(
                {"$or": [{"_id": match_id_obj}, {"_id": match_id}]}
            )
            if not matchstat:
                return None

            actual_match_id = matchstat.get("matchId")
            if not actual_match_id:
                return None

            # Get all participants for this match
            participants = await db.matchstats.find(
                {"matchId": actual_match_id}
            ).to_list(None)

            return {
                "match_id": str(actual_match_id),
                "match_id_obj": actual_match_id,
                "participants": participants,
            }
        except Exception as e:
            print(f"Error fetching match: {e}")
            return None

    @staticmethod
    async def build_engine_data(db: AsyncIOMotorDatabase, match_data: dict) -> dict | None:
        """
        Build engine data from raw matchstats records.
        Transforms matchstats.teamTotals.points into the format the analytics engine expects.
        """
        try:
            if not match_data:
                return None

            match_id = match_data["match_id"]
            participants = match_data["participants"]

            if not participants:
                return None

            # Extract player IDs (stable order)
            player_ids = []
            for p in participants:
                rid = p.get("participant", {}).get("refId")
                if rid:
                    player_ids.append(_sid(rid))

            # Fetch player details from players collection
            players_info = {}
            if player_ids:
                for pid in player_ids:
                    try:
                        player = await db.players.find_one(
                            {"user": ObjectId(pid) if len(pid) == 24 else pid}
                        )
                        if player:
                            players_info[pid] = {
                                "name": player.get("name", "Unknown"),
                                "email": player.get("email", ""),
                            }
                        else:
                            players_info[pid] = {"name": "Unknown", "email": ""}
                    except Exception:
                        players_info[pid] = {"name": "Unknown", "email": ""}

            # CentrePitch stores the full point log on each participant's MatchStat.
            # Concatenating both sides duplicates every point and breaks score progression / timeline.
            points_source = None
            for stat in participants:
                pts = stat.get("teamTotals", {}).get("points") or []
                if len(pts) > 0:
                    points_source = stat
                    break
            if not points_source:
                return None

            all_points = [p.copy() for p in (points_source.get("teamTotals", {}).get("points") or [])]

            # Sort: set_number, then point_id / point_number
            all_points.sort(
                key=lambda p: (
                    int(p.get("set_number") or 1),
                    int(p.get("point_id") or p.get("point_number") or 0),
                )
            )

            # Transform to engine format
            # Map actual matchstat fields to engine schema
            points = []
            for i, p in enumerate(all_points):
                w = _sid(p.get("point_winner") or p.get("winner_id") or p.get("winner"))
                points.append({
                    "_id": f"{match_id}_p{i}",
                    "match_id": match_id,
                    "set_id": f"{match_id}_s{p.get('set_number', 1)}",
                    "set_number": int(p.get("set_number") or 1),
                    "point_number": int(p.get("point_id") or p.get("point_number") or (i + 1)),
                    "point_winner": w,
                    "winner_id": w,
                    "server_id": _sid(p.get("server")),
                    "server": _sid(p.get("server")),
                    "rally_shots": p.get("rally_shots") or 0,
                    "rally_duration": p.get("rally_duration_sec") or p.get("rally_duration") or 0,
                    "ending_type": p.get("ending_type") or p.get("ending_shot"),
                    "ending_shot": p.get("ending_shot") or p.get("ending_type"),
                    "score_before": p.get("score_before", "0-0"),
                })

            # Build match_info
            player_a_id = _sid(player_ids[0]) if len(player_ids) > 0 else ""
            player_b_id = _sid(player_ids[1]) if len(player_ids) > 1 else ""

            _created = participants[0].get("createdAt") if participants else None
            _date_iso = (
                _created.isoformat()
                if hasattr(_created, "isoformat")
                else str(_created or "")
            )

            match_info = {
                "match_id": match_id,
                "player_a_id": player_a_id,  # Engine needs this at top level
                "player_b_id": player_b_id,  # Engine needs this at top level
                "player_a": {
                    "player_id": player_a_id,
                    "name": players_info.get(player_a_id, {}).get("name", "Unknown"),
                },
                "player_b": {
                    "player_id": player_b_id,
                    "name": players_info.get(player_b_id, {}).get("name", "Unknown"),
                },
                "date": _date_iso,
                "winner_id": None,
            }

            return {
                "match_info": match_info,
                "points": points,
                "shots": [],  # No shot data in this schema
            }

        except Exception as e:
            print(f"Error building engine data: {e}")
            return None
