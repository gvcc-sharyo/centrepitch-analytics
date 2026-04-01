"""
Database models — SQLAlchemy ORM
Replaces Excel. Every table is queryable, indexable, joinable.
"""
from datetime import datetime
from sqlalchemy import (
    Column, String, Integer, Float, Boolean,
    DateTime, ForeignKey, JSON, Text, Index
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Player(Base):
    __tablename__ = "players"

    player_id   = Column(String, primary_key=True)  # e.g. "PLR_001"
    name        = Column(String, nullable=False)
    sport_id    = Column(String, default="badminton")
    created_at  = Column(DateTime, default=datetime.utcnow)
    metadata_   = Column("metadata", JSON, default=dict)

    matches_as_a  = relationship("Match", foreign_keys="Match.player_a_id", back_populates="player_a")
    matches_as_b  = relationship("Match", foreign_keys="Match.player_b_id", back_populates="player_b")
    profile       = relationship("PlayerProfile", back_populates="player", uselist=False)


class Match(Base):
    __tablename__ = "matches"

    match_id      = Column(String, primary_key=True)
    player_a_id   = Column(String, ForeignKey("players.player_id"), nullable=False)
    player_b_id   = Column(String, ForeignKey("players.player_id"), nullable=False)
    winner_id     = Column(String, ForeignKey("players.player_id"), nullable=True)
    date          = Column(DateTime, nullable=False)
    venue         = Column(String, nullable=True)
    sport_id      = Column(String, default="badminton")
    total_sets    = Column(Integer, default=0)          # 2 or 3
    sets_won_a    = Column(Integer, default=0)          # sets won by player A
    sets_won_b    = Column(Integer, default=0)          # sets won by player B
    created_at    = Column(DateTime, default=datetime.utcnow)

    player_a      = relationship("Player", foreign_keys=[player_a_id], back_populates="matches_as_a")
    player_b      = relationship("Player", foreign_keys=[player_b_id], back_populates="matches_as_b")
    sets          = relationship("Set", back_populates="match", order_by="Set.set_number")
    analytics     = relationship("AnalyticsCache", back_populates="match")

    __table_args__ = (
        Index("ix_matches_date",          "date"),
        Index("ix_matches_winner",        "winner_id"),
        Index("ix_matches_player_a",      "player_a_id"),
        Index("ix_matches_player_b",      "player_b_id"),
        # Composite: covers player-filtered + date-ordered queries in one scan
        Index("ix_matches_player_a_date", "player_a_id", "date"),
        Index("ix_matches_player_b_date", "player_b_id", "date"),
    )


class Set(Base):
    """
    One row per set within a match.
    Badminton: best of 3, each set to 21 (deuce extends to max 30).
    """
    __tablename__ = "sets"

    set_id        = Column(String, primary_key=True)   # e.g. "MTH-001-S1"
    match_id      = Column(String, ForeignKey("matches.match_id"), nullable=False)
    set_number    = Column(Integer, nullable=False)    # 1, 2, 3
    score_a       = Column(Integer, nullable=False)    # final score for player A in this set
    score_b       = Column(Integer, nullable=False)    # final score for player B in this set
    winner_id     = Column(String, ForeignKey("players.player_id"), nullable=True)
    is_deuce      = Column(Boolean, default=False)     # True when set went to deuce (20-20+)
    total_points  = Column(Integer, default=0)

    match  = relationship("Match", back_populates="sets")
    points = relationship("Point", back_populates="set", order_by="Point.point_number")

    __table_args__ = (
        Index("ix_sets_match_id", "match_id"),
    )


class Point(Base):
    __tablename__ = "points"

    point_id        = Column(Integer, primary_key=True, autoincrement=True)
    match_id        = Column(String, ForeignKey("matches.match_id"), nullable=False)
    set_id          = Column(String, ForeignKey("sets.set_id"), nullable=True)
    set_number      = Column(Integer, nullable=True)    # denormalised for easy filtering
    point_number    = Column(Integer, nullable=False)   # 1-indexed within set
    score_a_before  = Column(Integer, nullable=False)
    score_b_before  = Column(Integer, nullable=False)
    server_id       = Column(String, ForeignKey("players.player_id"), nullable=True)
    winner_id       = Column(String, ForeignKey("players.player_id"), nullable=False)
    rally_shots     = Column(Integer, nullable=True)
    rally_duration  = Column(Float, nullable=True)      # seconds
    ending_type     = Column(String, nullable=True)     # smash_winner, forced_error, etc.
    win_prob_after  = Column(Float, nullable=True)      # computed on ingest
    momentum_after  = Column(Float, nullable=True)      # computed on ingest

    match  = relationship("Match")
    set    = relationship("Set", back_populates="points")
    shots  = relationship("Shot", back_populates="point", order_by="Shot.shot_number")

    __table_args__ = (
        Index("ix_points_match_id", "match_id"),
        Index("ix_points_set_id", "set_id"),
        Index("ix_points_winner", "winner_id"),
    )


class Shot(Base):
    """
    One row per individual shot. This is the key table that the
    current Excel approach cannot support. Enables:
    - GROUP BY shot_type → frequency
    - JOIN with points → win rate per shot
    - Self-join on prev_shot_type → sequence analysis
    - Filter by landing_zone → heatmap
    """
    __tablename__ = "shots"

    shot_id             = Column(Integer, primary_key=True, autoincrement=True)
    point_id            = Column(Integer, ForeignKey("points.point_id"), nullable=False)
    match_id            = Column(String, ForeignKey("matches.match_id"), nullable=False)
    set_id              = Column(String, ForeignKey("sets.set_id"), nullable=True)
    player_id           = Column(String, ForeignKey("players.player_id"), nullable=False)
    shot_number         = Column(Integer, nullable=False)   # 1-indexed within rally
    shot_type           = Column(String, nullable=False)    # smash, clear, drop, etc.
    is_winning_shot     = Column(Boolean, default=False)
    prev_shot_type      = Column(String, nullable=True)     # shot before this one
    next_shot_type      = Column(String, nullable=True)     # shot after this one
    # Optional — populated by CV pipeline later
    landing_zone        = Column(String, nullable=True)     # rear_left, net_right, etc.
    speed_kmh           = Column(Float, nullable=True)
    trajectory_type     = Column(String, nullable=True)     # flat, arc, steep
    frame_number        = Column(Integer, nullable=True)

    point  = relationship("Point", back_populates="shots")

    __table_args__ = (
        Index("ix_shots_match_id",       "match_id"),
        Index("ix_shots_player_id",      "player_id"),
        Index("ix_shots_shot_type",      "shot_type"),
        Index("ix_shots_player_shot",    "player_id", "shot_type"),
        # Covers best_winning_shot query: WHERE player_id=? AND is_winning_shot=1
        Index("ix_shots_player_winning", "player_id", "is_winning_shot"),
    )


class AnalyticsCache(Base):
    """
    Computed analytics stored on ingest. GET endpoints read this.
    Never recomputed on request. Invalidated when match data changes.
    """
    __tablename__ = "analytics_cache"

    cache_id        = Column(Integer, primary_key=True, autoincrement=True)
    match_id        = Column(String, ForeignKey("matches.match_id"), nullable=False)
    module_name     = Column(String, nullable=False)    # e.g. "win_probability"
    result          = Column(JSON, nullable=False)
    confidence      = Column(Float, default=1.0)
    modules_skipped = Column(JSON, default=list)
    computed_at     = Column(DateTime, default=datetime.utcnow)

    match = relationship("Match", back_populates="analytics")

    __table_args__ = (
        Index("ix_cache_match_module", "match_id", "module_name", unique=True),
    )


class PlayerProfile(Base):
    """
    Rolling stats across all matches. Updated after every match
    involving this player. GET /players/{id}/profile reads this.
    """
    __tablename__ = "player_profiles"

    player_id        = Column(String, ForeignKey("players.player_id"), primary_key=True)
    computed_at      = Column(DateTime, default=datetime.utcnow)
    total_matches    = Column(Integer, default=0)
    win_rate_overall = Column(Float, default=0.0)
    win_rate_30d     = Column(Float, default=0.0)
    consistency      = Column(Float, default=0.0)   # lower = more consistent
    pressure_rating  = Column(Float, default=0.0)   # pressure win rate minus overall
    signature_shot   = Column(String, nullable=True)
    full_stats       = Column(JSON, default=dict)   # all computed metrics

    player = relationship("Player", back_populates="profile")
