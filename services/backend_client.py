"""
Match data accessor — queries matchstats collection from shared MongoDB.
matchstats is the single source of truth for match data.

Since CA-1 and the backend share the same MongoDB database,
we query matchstats directly for maximum performance.

Data flow:
- Match data: Read from matchstats collection
- Computed results: Cached in analytics_cache
"""
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId


async def matchstats_participant_ref_match(db: AsyncIOMotorDatabase, player_id: str) -> dict:
    """
    Filter matchstats rows for a player. refId is sometimes stored as ObjectId,
    sometimes as the same 24-char hex string, and sometimes as the `players` document _id.
    A single-type query returns zero rows.
    """
    try:
        oid = ObjectId(player_id)
    except Exception:
        return {"participant.refId": player_id}

    # The "player id" in URLs can be either:
    # - users._id  (user id)
    # - players._id (player doc id)
    #
    # And matchstats.participant.refId may store either (as ObjectId or string).
    # Resolve both directions to make the filter robust.

    ors: list[dict] = [
        {"participant.refId": oid},
        {"participant.refId": str(player_id)},
    ]

    # If player_id is a user id, this finds their Player doc.
    player_doc_by_user = await db.players.find_one({"user": oid}, {"_id": 1})
    if isinstance(player_doc_by_user, dict) and player_doc_by_user.get("_id") is not None:
        pid = player_doc_by_user["_id"]
        ors.append({"participant.refId": pid})
        ors.append({"participant.refId": str(pid)})

    # If player_id is a Player doc id, this finds the user id.
    player_doc_by_id = await db.players.find_one({"_id": oid}, {"user": 1})
    if isinstance(player_doc_by_id, dict) and player_doc_by_id.get("user") is not None:
        uid = player_doc_by_id["user"]
        ors.append({"participant.refId": uid})
        ors.append({"participant.refId": str(uid)})

    # De-dupe (stable)
    seen = set()
    deduped = []
    for q in ors:
        k = str(q.get("participant.refId"))
        if k in seen:
            continue
        seen.add(k)
        deduped.append(q)

    return {"$or": deduped}


class BackendClient:
    """Access match data from matchstats collection (single source of truth)."""

    @staticmethod
    async def get_match_stat(db: AsyncIOMotorDatabase, match_id: str) -> dict | None:
        """
        Fetch a single match from matchstats collection by ID.
        matchstats stores detailed match statistics.
        """
        try:
            # Try string ID first, then ObjectId
            match = await db.matchstats.find_one({"_id": match_id})
            if not match:
                try:
                    match = await db.matchstats.find_one({"_id": ObjectId(match_id)})
                except (ValueError, Exception):
                    pass
            return match
        except Exception as e:
            print(f"Error fetching match {match_id}: {e}")
            return None

    @staticmethod
    async def list_match_stats(
        db: AsyncIOMotorDatabase,
        limit: int = 20,
        offset: int = 0,
        search: str = "",
        player_id: str = None,
    ) -> dict:
        """
        Fetch paginated list of matches from matchstats collection.

        Args:
            db: MongoDB database instance
            limit: Number of matches to return
            offset: Number of matches to skip
            search: Filter by match id / player name
            player_id: Filter by specific player

        Returns:
            {
                "total": int,
                "matches": [match_data, ...],
                "limit": int,
                "offset": int
            }
        """
        try:
            query = {}

            if search:
                # Search in participant name (text search)
                query = {
                    "$or": [
                        {"participant.name": {"$regex": search, "$options": "i"}},
                    ]
                }

            if player_id:
                player_query = await matchstats_participant_ref_match(db, player_id)
                if search:
                    query = {"$and": [query, player_query]}
                else:
                    query = player_query

            total = await db.matchstats.count_documents(query)
            matches = (
                await db.matchstats.find(query)
                .sort("createdAt", -1)
                .skip(offset)
                .limit(limit)
                .to_list(None)
            )

            return {
                "total": total,
                "matches": matches,
                "limit": limit,
                "offset": offset,
            }
        except Exception as e:
            print(f"Error listing matches: {e}")
            return {
                "total": 0,
                "matches": [],
                "limit": limit,
                "offset": offset,
            }

    @staticmethod
    async def get_player_matches(
        db: AsyncIOMotorDatabase,
        player_id: str,
        limit: int = 20,
        offset: int = 0,
    ) -> dict:
        """
        Fetch all matches for a specific player from matchstats.
        Returns all matchstat records where player participated.
        """
        try:
            query = await matchstats_participant_ref_match(db, player_id)
            total = await db.matchstats.count_documents(query)
            matches = (
                await db.matchstats.find(query)
                .sort("createdAt", -1)
                .skip(offset)
                .limit(limit)
                .to_list(None)
            )

            return {
                "total": total,
                "matches": matches,
                "limit": limit,
                "offset": offset,
            }
        except Exception as e:
            print(f"Error fetching player {player_id} matches: {e}")
            return {
                "total": 0,
                "matches": [],
                "limit": limit,
                "offset": offset,
            }


# Note: BackendClient queries matchstats directly from shared MongoDB
# - matchstats is the single source of truth for match data
# - CA-1 computes analytics and caches results in analytics_cache
# - No HTTP requests, no API overhead
