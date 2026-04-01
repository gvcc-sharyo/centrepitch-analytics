"""
Analytics permissions service.
Manages two orthogonal access-control systems:

1. TIER PERMISSIONS — which analytics modules each role can see
   (basic | advanced | full). Seeded per role, editable by superadmin.

2. MODULE REGISTRY — which modules are globally enabled or disabled.
   Disabled modules are skipped by the engine AND stripped from all
   responses, regardless of tier. Superadmin can toggle any module.
   Modules requiring video data are seeded as disabled (on hold).
"""
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorDatabase

# ─── TIER DEFINITIONS ────────────────────────────────────────────────────────

TIER_MODULES = {
    "basic": {
        "score_progression",
        "win_probability",
        "momentum",
        "turning_points",
        "ending_analysis",
        "score_timeline",
        "coaching_brief",
    },
    "advanced": {
        "score_progression",
        "win_probability",
        "momentum",
        "turning_points",
        "ending_analysis",
        "rally_analysis",
        "pressure_analysis",
        "serve_analysis",
        "shot_effectiveness",
        "score_timeline",
        "coaching_brief",
    },
    "full": {
        "score_progression",
        "win_probability",
        "momentum",
        "turning_points",
        "ending_analysis",
        "rally_analysis",
        "pressure_analysis",
        "serve_analysis",
        "shot_effectiveness",
        "shot_sequences",
        "score_timeline",
        "coaching_brief",
    },
}

DEFAULT_PERMISSIONS = {
    "superadmin":   {"tier": "full",     "scope": "global"},
    "academyadmin": {"tier": "advanced", "scope": "academy"},
    "organizer":    {"tier": "advanced", "scope": "event"},
    "coach":        {"tier": "advanced", "scope": "academy"},
    "player":       {"tier": "basic",    "scope": "self"},
    "anonymous":    {"tier": "basic",    "scope": "public"},
}

# ─── MODULE REGISTRY ──────────────────────────────────────────────────────────
# requires:
#   "manual_scoring" — winner + ending_shot per point (scorer enters)
#   "rally_shots"    — optional rally_shots field from scorer
#   "video"          — needs video processing pipeline (future)

MODULE_REGISTRY = [
    {
        "_id":             "score_progression",
        "label":           "Score Progression",
        "description":     "Running score after each point, per set.",
        "requires":        "manual_scoring",
        "default_enabled": True,
    },
    {
        "_id":             "win_probability",
        "label":           "Win Probability",
        "description":     "Estimated win % for each player after every point.",
        "requires":        "manual_scoring",
        "default_enabled": True,
    },
    {
        "_id":             "momentum",
        "label":           "Momentum",
        "description":     "Rolling 5-point win streak per player.",
        "requires":        "manual_scoring",
        "default_enabled": True,
    },
    {
        "_id":             "turning_points",
        "label":           "Turning Points",
        "description":     "Points where win probability shifted >8%.",
        "requires":        "manual_scoring",
        "default_enabled": True,
    },
    {
        "_id":             "ending_analysis",
        "label":           "Ending Types",
        "description":     "How points ended — smash, net kill, error, etc.",
        "requires":        "manual_scoring",
        "default_enabled": True,
    },
    {
        "_id":             "serve_analysis",
        "label":           "Serve Analysis",
        "description":     "Win rate when serving vs receiving.",
        "requires":        "manual_scoring",
        "default_enabled": True,
    },
    {
        "_id":             "pressure_analysis",
        "label":           "Pressure Analysis",
        "description":     "Win rate at close scores and late game.",
        "requires":        "manual_scoring",
        "default_enabled": True,
    },
    {
        "_id":             "rally_analysis",
        "label":           "Rally Analysis",
        "description":     "Short/medium/long rally distribution and win rates. Requires scorer to enter rally shot count.",
        "requires":        "rally_shots",
        "default_enabled": False,   # on hold — scorer must enter rally_shots
    },
    {
        "_id":             "shot_effectiveness",
        "label":           "Shot Effectiveness",
        "description":     "Win rate per shot type. Requires video-derived shot-level data.",
        "requires":        "video",
        "default_enabled": False,   # on hold — video pipeline
    },
    {
        "_id":             "shot_sequences",
        "label":           "Shot Sequences",
        "description":     "Most frequent 2-shot combos and win rates. Requires video.",
        "requires":        "video",
        "default_enabled": False,   # on hold — video pipeline
    },
    {
        "_id":             "score_timeline",
        "label":           "Score Timeline",
        "description":     "Interactive point-by-point timeline (circles) on match analytics.",
        "requires":        "manual_scoring",
        "default_enabled": True,
    },
    {
        "_id":             "coaching_brief",
        "label":           "AI Coaching Brief",
        "description":     "Generate AI coaching insights for a match (requires API key on server).",
        "requires":        "manual_scoring",
        "default_enabled": True,
    },
]

_MODULE_IDS = {m["_id"] for m in MODULE_REGISTRY}


class PermissionsService:

    # ── Role tier management ───────────────────────────────────────────────────

    @staticmethod
    async def seed_defaults(db: AsyncIOMotorDatabase):
        """Seed role tiers and module registry. Safe to call on every startup."""
        for role, config in DEFAULT_PERMISSIONS.items():
            await db.analytics_permissions.update_one(
                {"_id": role},
                {"$setOnInsert": {
                    "_id":        role,
                    "tier":       config["tier"],
                    "scope":      config["scope"],
                    # Per-role module overrides:
                    # Store module ids that are disabled for this role.
                    "disabled_modules": [],
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                }},
                upsert=True,
            )
        await ModulesService.seed_defaults(db)

    @staticmethod
    async def get_tier(db: AsyncIOMotorDatabase, role: str) -> str:
        doc = await db.analytics_permissions.find_one({"_id": role}, {"tier": 1})
        if doc:
            return doc["tier"]
        return DEFAULT_PERMISSIONS.get(role, DEFAULT_PERMISSIONS["anonymous"])["tier"]

    @staticmethod
    async def list_all(db: AsyncIOMotorDatabase) -> list:
        return await db.analytics_permissions.find({}).sort("_id", 1).to_list(None)

    @staticmethod
    async def update(db: AsyncIOMotorDatabase, role: str, tier: str) -> dict:
        if tier not in TIER_MODULES:
            raise ValueError(f"Invalid tier '{tier}'. Must be one of: {list(TIER_MODULES)}")
        await db.analytics_permissions.update_one(
            {"_id": role},
            {"$set": {"tier": tier, "updated_at": datetime.utcnow()}},
            upsert=True,
        )
        return await db.analytics_permissions.find_one({"_id": role})

    @staticmethod
    async def get_role_disabled_set(db: AsyncIOMotorDatabase, role: str) -> set[str]:
        doc = await db.analytics_permissions.find_one({"_id": role}, {"disabled_modules": 1})
        disabled = (doc or {}).get("disabled_modules") or []
        return {m for m in disabled if m in _MODULE_IDS}

    @staticmethod
    async def set_role_module_enabled(
        db: AsyncIOMotorDatabase,
        role: str,
        module_id: str,
        enabled: bool,
    ) -> dict:
        if module_id not in _MODULE_IDS:
            raise ValueError(f"Unknown module '{module_id}'")

        if enabled:
            await db.analytics_permissions.update_one(
                {"_id": role},
                {
                    "$pull": {"disabled_modules": module_id},
                    "$set":  {"updated_at": datetime.utcnow()},
                    "$setOnInsert": {"created_at": datetime.utcnow()},
                },
                upsert=True,
            )
        else:
            await db.analytics_permissions.update_one(
                {"_id": role},
                {
                    "$addToSet": {"disabled_modules": module_id},
                    "$set":      {"updated_at": datetime.utcnow()},
                    "$setOnInsert": {"created_at": datetime.utcnow()},
                },
                upsert=True,
            )

        return await db.analytics_permissions.find_one({"_id": role})

    @staticmethod
    def can_view_module(
        module_id: str,
        tier: str,
        enabled_modules: set[str],
        role_disabled: set[str] | None = None,
    ) -> bool:
        allowed = TIER_MODULES.get(tier, TIER_MODULES["basic"])
        if module_id not in allowed:
            return False
        if module_id not in enabled_modules:
            return False
        if role_disabled and module_id in role_disabled:
            return False
        return True

    @staticmethod
    def filter_analytics(
        analytics: dict,
        tier: str,
        enabled_modules: set[str],
        role_disabled: set[str] | None = None,
    ) -> dict:
        """
        Strip modules the tier cannot see AND modules disabled by admin AND modules disabled for this role.
        Non-module keys are always included.
        """
        allowed    = TIER_MODULES.get(tier, TIER_MODULES["basic"])
        visible    = allowed & enabled_modules          # tier allowed AND admin-enabled
        if role_disabled:
            visible = visible - role_disabled
        non_module   = {"match_id", "modules_run", "modules_skipped"}
        return {k: v for k, v in analytics.items() if k in non_module or k in visible}


# ─── MODULE MANAGEMENT SERVICE ────────────────────────────────────────────────

class ModulesService:

    @staticmethod
    async def seed_defaults(db: AsyncIOMotorDatabase):
        """Insert module registry docs. $setOnInsert — never overwrites admin changes."""
        for m in MODULE_REGISTRY:
            await db.analytics_modules.update_one(
                {"_id": m["_id"]},
                {"$setOnInsert": {
                    "_id":             m["_id"],
                    "label":           m["label"],
                    "description":     m["description"],
                    "requires":        m["requires"],
                    "enabled":         m["default_enabled"],
                    "created_at":      datetime.utcnow(),
                    "updated_at":      datetime.utcnow(),
                }},
                upsert=True,
            )

    @staticmethod
    async def list_all(db: AsyncIOMotorDatabase) -> list:
        return await db.analytics_modules.find({}).sort("_id", 1).to_list(None)

    @staticmethod
    async def set_enabled(db: AsyncIOMotorDatabase, module_id: str, enabled: bool) -> dict:
        if module_id not in _MODULE_IDS:
            raise ValueError(f"Unknown module '{module_id}'")
        await db.analytics_modules.update_one(
            {"_id": module_id},
            {"$set": {"enabled": enabled, "updated_at": datetime.utcnow()}},
            upsert=True,
        )
        return await db.analytics_modules.find_one({"_id": module_id})

    @staticmethod
    async def get_enabled_set(db: AsyncIOMotorDatabase) -> set[str]:
        """Returns the set of currently enabled module keys."""
        docs = await db.analytics_modules.find({}, {"_id": 1, "enabled": 1}).to_list(None)
        by_id = {d["_id"]: d.get("enabled", True) for d in docs}
        enabled = {mid for mid, en in by_id.items() if en}
        for m in MODULE_REGISTRY:
            mid = m["_id"]
            if mid not in by_id and m.get("default_enabled", True):
                enabled.add(mid)
        return enabled

    @staticmethod
    async def get_disabled_set(db: AsyncIOMotorDatabase) -> set[str]:
        docs = await db.analytics_modules.find({"enabled": False}, {"_id": 1}).to_list(None)
        return {d["_id"] for d in docs}
