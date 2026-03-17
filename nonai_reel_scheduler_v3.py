"""
nonai_scheduled_reels_posts.py
Run via cron every 15 minutes.

Purpose:
- Create & upload 15-second EssentiaScan (NonAI.life) reels via Gemini Veo.
- Posts to all platforms according to client weekly reel schedule.
- Generates unique captions with tracking links using imported caption generator.
- Uses brand hashtags + Ayrshare auto-hashtags (5 total for Instagram).
- Instagram: NO URL in caption (links don't work there).
- Other platforms: Include tracking URL in caption.
- Avoids duplicate posting per platform per slot using DATABASE.
- Logs all activity.

NEW (same as image pipeline):
- TRACKS concept performance per platform via Ayrshare analytics.
- AUTO-PAUSES / STOPS underperforming reel concepts.
- Smart concept selection: evaluation → exploit best → explore others.
- Stores ayrshare_post_id + social_post_id for analytics linking.
- Unified analytics via /api/unified-report on click-tracking server.
"""

import os
import sys
import time
import json
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from google import genai
from google.genai import types
from dotenv import load_dotenv
import google.generativeai as genai_caption
from pinecone import Pinecone
import psycopg2
from psycopg2.extras import RealDictCursor
import re
import random

# ======================================================
# ENV
# ======================================================
load_dotenv()

GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY")
AYRSHARE_API_KEY = os.getenv("AYESHARE_API_KEY")
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
DATABASE_URL     = os.getenv("DATABASE_URL")
TRACKING_API_URL = os.getenv("TRACKING_API_URL", "http://44.193.35.107:8000")

if not GEMINI_API_KEY or not AYRSHARE_API_KEY or not DATABASE_URL:
    print("❌ Missing required API keys (GEMINI_API_KEY, AYESHARE_API_KEY, DATABASE_URL)")
    sys.exit(1)

# ======================================================
# CONFIG
# ======================================================
TIMEZONE    = ZoneInfo("America/New_York")   # client posts in ET
PROMPT_FILE = "last_reel_prompt_index.txt"   # separate from image pipeline

# ── Performance thresholds (same logic as image pipeline) ─────────────────
UNDERPERFORM_RATIO    = 0.4   # concept scoring < 40% of best → pause
MIN_POSTS_TO_EVALUATE = 3     # need this many posts before any judgement
PAUSE_DAYS            = 7     # days a concept is blocked before re-evaluation
RE_EVAL_POSTS         = 2     # posts given during re-evaluation window
MAX_PAUSE_COUNT       = 2     # pauses before permanent stop

# ======================================================
# REEL POSTING SCHEDULE (ET)
# ======================================================
POSTING_SCHEDULE = {
    "instagram": [
        {"days": [0], "hour": 12, "minute": 15},   # Monday    12:15 PM
        {"days": [0], "hour": 19, "minute": 30},   # Monday     7:30 PM
        {"days": [1], "hour": 12, "minute": 15},   # Tuesday   12:15 PM
        {"days": [1], "hour": 19, "minute": 30},   # Tuesday    7:30 PM
        {"days": [2], "hour": 19, "minute": 30},   # Wednesday  7:30 PM
        {"days": [3], "hour": 14, "minute": 59},   # Thursday   2:59 PM
        {"days": [4], "hour": 12, "minute": 15},   # Friday    12:15 PM
        {"days": [4], "hour": 19, "minute": 30},   # Friday     7:30 PM
        {"days": [5], "hour": 19, "minute": 30},   # Saturday   7:30 PM
        {"days": [6], "hour": 19, "minute": 30},   # Sunday     7:30 PM
    ],
}
# Apply same schedule to all platforms
for _platform in [ "x", "linkedin",  "facebook"]:
    POSTING_SCHEDULE[_platform] = POSTING_SCHEDULE["instagram"]

# ======================================================
# REEL CONCEPTS  (5 concepts, each with full metadata)
# ======================================================
REEL_CONCEPTS = [
    {
        "concept":     "deepfake_protection",
        "title":       "Deepfake Panic Call",
        "prompt":      "Older adult receives frantic call, phone UI overlay showing AI voice clone warning, taps verify button, Non-AI score appears — 15-second vertical reel, dramatic music.",
        "description": "Emotional scenario showing deepfake threat and instant verification.",
    },
    {
        "concept":     "social_media_trust",
        "title":       "Catfish Reveal",
        "prompt":      "Person scrolling dating app, sees too-perfect profile, taps verify, AI-generated images flagged in red, relief on face — 15-second vertical reel, suspenseful then relieved.",
        "description": "Trust and authenticity in online relationships.",
    },
    {
        "concept":     "human_verification_intro",
        "title":       "Non-AI Score Challenge",
        "prompt":      "Friends in coffee shop checking their Non-AI Scores on phones, reacting with laughs and surprise, playful leaderboard overlay — 15-second vertical reel, upbeat music.",
        "description": "Fun social proof showing verification as a social activity.",
    },
    {
        "concept":     "ai_impersonation_fight",
        "title":       "Gaming & Streaming Proof",
        "prompt":      "Gamers in a bot-filled lobby, one player verifies with Non-AI Score, human-verified badge appears, bots kicked — 15-second vertical reel, gaming aesthetic, victory sound.",
        "description": "Verification protecting authentic gaming communities.",
    },
    {
        "concept":     "creator_authenticity",
        "title":       "Social Proof: Verified Human",
        "prompt":      "Montage of diverse creators posting content with Non-AI Verified badges glowing on screen, inspiring text overlay 'Real People. Real Content.' — 15-second vertical reel, inspirational.",
        "description": "Creator economy and authentic content movement.",
    },
]

# ======================================================
# BRAND HASHTAGS
# ======================================================
BRAND_HASHTAGS_MARKETING = {
    "core":      ["#NonAI", "#EssentiaScan", "#VerifiedHuman"],
    "secondary": ["#HumanVerification", "#BiologicalFirewall", "#ProveYoureHuman"],
    "business":  ["#DeepfakeDefense", "#IdentitySecurity", "#ZeroTrust"],
    "social":    ["#RealHuman", "#HumanOnly", "#AIFree"],
}

# ======================================================
# GEMINI CLIENTS
# ======================================================
client = genai.Client(api_key=GEMINI_API_KEY)
genai_caption.configure(api_key=GEMINI_API_KEY)
model_caption = genai_caption.GenerativeModel("gemini-2.0-flash-exp")

if PINECONE_API_KEY:
    pc = Pinecone(api_key=PINECONE_API_KEY)

# ======================================================
# LOGGING
# ======================================================
def log(msg):
    now = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}")

# ======================================================
# DATABASE CONNECTION
# ======================================================
def get_db_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        log(f"❌ DB connection error: {e}")
        return None

# ======================================================
# TABLE INIT / MIGRATIONS
# ======================================================
def init_caption_table():
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS caption_history_marketing (
                    id           SERIAL PRIMARY KEY,
                    caption      TEXT NOT NULL,
                    video_concept VARCHAR(100),
                    platform     VARCHAR(50) NOT NULL,
                    timestamp    TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            conn.commit()
        return True
    finally:
        conn.close()


def init_posted_slots_table():
    """
    posted_slots_reels — one row per platform per slot per day.
    Stores ayrshare_post_id (top-level) + social_post_id (platform-native)
    so analytics can be fetched and cross-referenced.
    """
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS posted_slots_reels (
                    id               SERIAL PRIMARY KEY,
                    platform         VARCHAR(50)  NOT NULL,
                    post_date        DATE         NOT NULL,
                    target_hour      INTEGER      NOT NULL,
                    target_minute    INTEGER      NOT NULL,
                    posted_at        TIMESTAMP    NOT NULL DEFAULT NOW(),
                    post_url         TEXT,
                    tracking_id      VARCHAR(100),
                    ayrshare_post_id TEXT,
                    social_post_id   TEXT,
                    concept_key      VARCHAR(100),
                    UNIQUE(platform, post_date, target_hour, target_minute)
                )
            """)
            # Safe migrations for older databases
            for col, dtype in [
                ("ayrshare_post_id", "TEXT"),
                ("social_post_id",   "TEXT"),
                ("concept_key",      "VARCHAR(100)"),
            ]:
                cur.execute(f"""
                    ALTER TABLE posted_slots_reels
                    ADD COLUMN IF NOT EXISTS {col} {dtype}
                """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_posted_slots_reels_lookup
                ON posted_slots_reels(platform, post_date, target_hour, target_minute)
            """)
            conn.commit()
        log("✅ posted_slots_reels table ready")
        return True
    except Exception as e:
        log(f"❌ Error initialising posted_slots_reels: {e}")
        return False
    finally:
        conn.close()


def init_concept_analytics_table():
    """
    concept_analytics_reels — one row per post per analytics refresh.
    Mirrors concept_analytics from the image pipeline but for reels.
    """
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS concept_analytics_reels (
                    id                   SERIAL PRIMARY KEY,
                    platform             VARCHAR(50)  NOT NULL,
                    concept_key          VARCHAR(100) NOT NULL,
                    concept_title        VARCHAR(200),
                    ayrshare_post_id     TEXT         NOT NULL,
                    post_url             TEXT,
                    posted_at            TIMESTAMP,
                    likes                INTEGER DEFAULT 0,
                    comments             INTEGER DEFAULT 0,
                    shares               INTEGER DEFAULT 0,
                    impressions          INTEGER DEFAULT 0,
                    reach                INTEGER DEFAULT 0,
                    views                INTEGER DEFAULT 0,
                    saves                INTEGER DEFAULT 0,
                    engagement_score     FLOAT   DEFAULT 0,
                    analytics_fetched_at TIMESTAMP,
                    raw_analytics        JSONB
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_concept_analytics_reels_platform_concept
                ON concept_analytics_reels(platform, concept_key)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_concept_analytics_reels_ayrshare
                ON concept_analytics_reels(ayrshare_post_id)
            """)
            conn.commit()
        log("✅ concept_analytics_reels table ready")
        return True
    except Exception as e:
        log(f"❌ Error initialising concept_analytics_reels: {e}")
        return False
    finally:
        conn.close()


def init_concept_performance_table():
    """
    concept_performance_reels — one row per (platform, concept_key).
    Aggregated averages + pause/stop lifecycle columns.
    """
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS concept_performance_reels (
                    platform               VARCHAR(50)  NOT NULL,
                    concept_key            VARCHAR(100) NOT NULL,
                    concept_title          VARCHAR(200),
                    total_posts            INTEGER      DEFAULT 0,
                    avg_engagement_score   FLOAT        DEFAULT 0,
                    total_engagement_score FLOAT        DEFAULT 0,
                    avg_likes              FLOAT        DEFAULT 0,
                    avg_comments           FLOAT        DEFAULT 0,
                    avg_shares             FLOAT        DEFAULT 0,
                    avg_impressions        FLOAT        DEFAULT 0,
                    avg_reach              FLOAT        DEFAULT 0,
                    avg_views              FLOAT        DEFAULT 0,
                    last_updated           TIMESTAMP    DEFAULT NOW(),
                    is_active              BOOLEAN      DEFAULT TRUE,
                    paused_until           TIMESTAMP,
                    pause_count            INTEGER      DEFAULT 0,
                    reeval_posts_given     INTEGER      DEFAULT 0,
                    stop_reason            TEXT,
                    PRIMARY KEY (platform, concept_key)
                )
            """)
            # Safe migrations
            for col, dtype in [
                ("paused_until",        "TIMESTAMP"),
                ("pause_count",         "INTEGER DEFAULT 0"),
                ("reeval_posts_given",  "INTEGER DEFAULT 0"),
                ("stop_reason",         "TEXT"),
            ]:
                cur.execute(f"""
                    ALTER TABLE concept_performance_reels
                    ADD COLUMN IF NOT EXISTS {col} {dtype}
                """)
            conn.commit()
        log("✅ concept_performance_reels table ready")
        return True
    except Exception as e:
        log(f"❌ Error initialising concept_performance_reels: {e}")
        return False
    finally:
        conn.close()

# ======================================================
# CAPTION HISTORY
# ======================================================
def load_caption_history(platform):
    conn = get_db_connection()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT caption, video_concept, platform
                FROM caption_history_marketing
                WHERE timestamp >= NOW() - INTERVAL '7 days'
                  AND platform = %s
            """, (platform,))
            return cur.fetchall()
    finally:
        conn.close()


def save_caption_to_history(caption, video_concept, platform):
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO caption_history_marketing (caption, video_concept, platform) VALUES (%s, %s, %s)",
                (caption, video_concept, platform)
            )
            conn.commit()
        return True
    finally:
        conn.close()

# ======================================================
# DUPLICATE PREVENTION
# ======================================================
def already_posted(platform, target_hour, target_minute):
    conn = get_db_connection()
    if not conn:
        return False
    try:
        today = datetime.now(TIMEZONE).date()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM posted_slots_reels
                WHERE platform = %s AND post_date = %s
                  AND target_hour = %s AND target_minute = %s
            """, (platform, today, target_hour, target_minute))
            return cur.fetchone() is not None
    except Exception as e:
        log(f"❌ Error checking posted status: {e}")
        return False
    finally:
        conn.close()


def mark_posted(platform, target_hour, target_minute,
                post_url=None, tracking_id=None,
                ayrshare_post_id=None, concept_key=None,
                social_post_id=None):
    """
    ayrshare_post_id : top-level 'id' from Ayrshare response
                       → used by /api/analytics/post endpoint
    social_post_id   : per-platform 'id' inside postIds[]
                       → platform-native ID (IG, TikTok, etc.)
    """
    conn = get_db_connection()
    if not conn:
        return False
    try:
        today = datetime.now(TIMEZONE).date()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO posted_slots_reels
                (platform, post_date, target_hour, target_minute,
                 post_url, tracking_id, ayrshare_post_id, concept_key, social_post_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (platform, post_date, target_hour, target_minute)
                DO UPDATE SET
                    posted_at        = NOW(),
                    post_url         = EXCLUDED.post_url,
                    tracking_id      = EXCLUDED.tracking_id,
                    ayrshare_post_id = EXCLUDED.ayrshare_post_id,
                    concept_key      = EXCLUDED.concept_key,
                    social_post_id   = EXCLUDED.social_post_id
            """, (platform, today, target_hour, target_minute,
                  post_url, tracking_id, ayrshare_post_id, concept_key, social_post_id))
            conn.commit()
        return True
    except Exception as e:
        log(f"❌ Error marking posted: {e}")
        return False
    finally:
        conn.close()

# ======================================================
# SCHEDULE CHECK
# ======================================================
def platforms_to_post_now():
    now = datetime.now(TIMEZONE)
    active = []
    for platform, slots in POSTING_SCHEDULE.items():
        for cfg in slots:
            if now.weekday() not in cfg["days"]:
                continue
            target_hour, target_minute = cfg["hour"], cfg["minute"]
            target  = target_hour * 60 + target_minute
            current = now.hour * 60 + now.minute
            if abs(current - target) <= 15:
                active.append((platform, target_hour, target_minute))
                break
    return active

# ======================================================
# AYRSHARE ANALYTICS FETCHER
# ======================================================
def fetch_ayrshare_analytics(ayrshare_post_id: str, platform: str) -> dict | None:
    """
    Call Ayrshare /api/analytics/post for one post on one platform.
    Returns the platform-specific analytics dict, or None on failure.
    """
    try:
        headers = {
            "Authorization": f"Bearer {AYRSHARE_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "id":        ayrshare_post_id,
            "platforms": [platform],
        }
        resp = requests.post(
            "https://api.ayrshare.com/api/analytics/post",
            json=payload, headers=headers, timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get(platform, {}).get("analytics")
        else:
            log(f"⚠️ Ayrshare analytics {resp.status_code}: {resp.text[:200]}")
            return None
    except Exception as e:
        log(f"❌ Error fetching Ayrshare analytics: {e}")
        return None


def parse_engagement_score(platform: str, analytics: dict) -> tuple[dict, float]:
    """
    Extract engagement fields from raw Ayrshare analytics (platform-aware).
    Reels also track views heavily — views weight is boosted vs image pipeline.

    Weights:
        views       × 0.3   (reels are view-driven)
        likes       × 1.0
        comments    × 2.0
        shares      × 3.0
        saves       × 2.5
        reach       × 0.1
        impressions × 0.05
    """
    if not analytics:
        return {}, 0.0

    fields = {
        "likes":       0,
        "comments":    0,
        "shares":      0,
        "impressions": 0,
        "reach":       0,
        "views":       0,
        "saves":       0,
    }

    p = platform.lower()

    if p == "instagram":
        fields["likes"]       = analytics.get("likeCount", 0) or 0
        fields["comments"]    = analytics.get("commentsCount", 0) or 0
        fields["shares"]      = analytics.get("sharesCount", 0) or 0
        fields["reach"]       = analytics.get("reachCount", 0) or 0
        fields["views"]       = analytics.get("viewsCount", 0) or analytics.get("plays", 0) or 0
        fields["saves"]       = analytics.get("savedCount", 0) or 0
        fields["impressions"] = fields["views"]

    elif p == "tiktok":
        fields["likes"]       = analytics.get("likeCount", 0) or 0
        fields["comments"]    = analytics.get("commentCount", 0) or 0
        fields["shares"]      = analytics.get("shareCount", 0) or 0
        fields["views"]       = analytics.get("playCount", 0) or analytics.get("viewCount", 0) or 0
        fields["impressions"] = fields["views"]

    elif p == "facebook":
        fields["likes"]       = analytics.get("likeCount", 0) or 0
        fields["comments"]    = analytics.get("commentsCount", 0) or 0
        fields["shares"]      = analytics.get("sharesCount", 0) or 0
        fields["impressions"] = analytics.get("impressionsUnique", 0) or 0
        fields["views"]       = analytics.get("videoViews", 0) or 0
        reactions             = analytics.get("reactions", {}) or {}
        fields["likes"]       = max(fields["likes"], reactions.get("total", 0) or 0)

    elif p == "linkedin":
        fields["likes"]       = analytics.get("likeCount", 0) or 0
        fields["comments"]    = analytics.get("commentCount", 0) or 0
        fields["shares"]      = analytics.get("shareCount", 0) or 0
        fields["impressions"] = analytics.get("impressionCount", 0) or 0
        fields["reach"]       = analytics.get("uniqueImpressionsCount", 0) or 0
        fields["views"]       = analytics.get("videoViews", 0) or 0

    elif p in ("x", "twitter"):
        pub = analytics.get("publicMetrics", {}) or {}
        fields["likes"]       = pub.get("likeCount", 0) or 0
        fields["comments"]    = pub.get("replyCount", 0) or 0
        fields["shares"]      = pub.get("retweetCount", 0) or 0
        fields["impressions"] = pub.get("impressionCount", 0) or 0
        fields["views"]       = pub.get("videoViewCount", 0) or 0

    elif p == "youtube":
        fields["likes"]       = analytics.get("likeCount", 0) or 0
        fields["comments"]    = analytics.get("commentCount", 0) or 0
        fields["views"]       = analytics.get("viewCount", 0) or 0
        fields["impressions"] = fields["views"]

    elif p == "threads":
        fields["likes"]       = analytics.get("likeCount", 0) or 0
        fields["comments"]    = analytics.get("repliesCount", 0) or 0
        fields["shares"]      = analytics.get("repostsCount", 0) or 0
        fields["views"]       = analytics.get("viewsCount", 0) or 0

    elif p == "telegram":
        fields["views"]       = analytics.get("views", 0) or 0
        fields["shares"]      = analytics.get("forwards", 0) or 0

    # Weighted engagement score — views boosted for reels
    score = (
        fields["views"]       * 0.3
        + fields["likes"]     * 1.0
        + fields["comments"]  * 2.0
        + fields["shares"]    * 3.0
        + fields["saves"]     * 2.5
        + fields["reach"]     * 0.1
        + fields["impressions"] * 0.05
    )
    return fields, round(score, 2)


# ======================================================
# SAVE / UPDATE ANALYTICS
# ======================================================
def save_concept_analytics(platform, concept_key, concept_title,
                            ayrshare_post_id, post_url, posted_at,
                            fields, score, raw):
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO concept_analytics_reels
                (platform, concept_key, concept_title, ayrshare_post_id, post_url,
                 posted_at, likes, comments, shares, impressions, reach, views, saves,
                 engagement_score, analytics_fetched_at, raw_analytics)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s)
            """, (
                platform, concept_key, concept_title, ayrshare_post_id, post_url,
                posted_at,
                fields.get("likes", 0),    fields.get("comments", 0),
                fields.get("shares", 0),   fields.get("impressions", 0),
                fields.get("reach", 0),    fields.get("views", 0),
                fields.get("saves", 0),    score,
                json.dumps(raw) if raw else None,
            ))
            conn.commit()
        log(f"💾 Saved reel analytics for '{concept_key}' on {platform} (score={score})")
    except Exception as e:
        log(f"❌ Error saving concept analytics: {e}")
    finally:
        conn.close()


def update_concept_performance(platform, concept_key, concept_title):
    """Recompute rolling averages for (platform, concept_key) and upsert."""
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*)              AS total_posts,
                    AVG(engagement_score) AS avg_score,
                    SUM(engagement_score) AS total_score,
                    AVG(likes)            AS avg_likes,
                    AVG(comments)         AS avg_comments,
                    AVG(shares)           AS avg_shares,
                    AVG(impressions)      AS avg_impressions,
                    AVG(reach)            AS avg_reach,
                    AVG(views)            AS avg_views
                FROM concept_analytics_reels
                WHERE platform = %s AND concept_key = %s
            """, (platform, concept_key))
            row = cur.fetchone()
            if not row or not row["total_posts"]:
                return

            cur.execute("""
                INSERT INTO concept_performance_reels
                (platform, concept_key, concept_title, total_posts,
                 avg_engagement_score, total_engagement_score,
                 avg_likes, avg_comments, avg_shares,
                 avg_impressions, avg_reach, avg_views, last_updated)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (platform, concept_key) DO UPDATE SET
                    concept_title          = EXCLUDED.concept_title,
                    total_posts            = EXCLUDED.total_posts,
                    avg_engagement_score   = EXCLUDED.avg_engagement_score,
                    total_engagement_score = EXCLUDED.total_engagement_score,
                    avg_likes              = EXCLUDED.avg_likes,
                    avg_comments           = EXCLUDED.avg_comments,
                    avg_shares             = EXCLUDED.avg_shares,
                    avg_impressions        = EXCLUDED.avg_impressions,
                    avg_reach              = EXCLUDED.avg_reach,
                    avg_views              = EXCLUDED.avg_views,
                    last_updated           = NOW()
            """, (
                platform, concept_key, concept_title,
                int(row["total_posts"]),
                float(row["avg_score"] or 0),
                float(row["total_score"] or 0),
                float(row["avg_likes"] or 0),
                float(row["avg_comments"] or 0),
                float(row["avg_shares"] or 0),
                float(row["avg_impressions"] or 0),
                float(row["avg_reach"] or 0),
                float(row["avg_views"] or 0),
            ))
            conn.commit()
        log(f"📊 Updated reel performance for '{concept_key}' on {platform}")
    except Exception as e:
        log(f"❌ Error updating concept performance: {e}")
    finally:
        conn.close()


# ======================================================
# FETCH & STORE ANALYTICS FOR RECENT REELS
# ======================================================
def fetch_and_store_recent_analytics(hours_back: int = 48):
    """
    Pull recent posted_slots_reels rows that have an ayrshare_post_id
    but whose analytics haven't been refreshed in the last 6 hours.
    Fetch from Ayrshare, store results, update performance aggregates.
    Called once at the start of every cron run.
    """
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT ps.platform, ps.ayrshare_post_id, ps.concept_key,
                       ps.post_url, ps.posted_at
                FROM posted_slots_reels ps
                WHERE ps.ayrshare_post_id IS NOT NULL
                  AND ps.concept_key      IS NOT NULL
                  AND ps.posted_at >= NOW() - (%s || ' hours')::INTERVAL
                  AND NOT EXISTS (
                      SELECT 1 FROM concept_analytics_reels ca
                      WHERE ca.ayrshare_post_id = ps.ayrshare_post_id
                        AND ca.analytics_fetched_at >= NOW() - INTERVAL '6 hours'
                  )
                ORDER BY ps.posted_at DESC
                LIMIT 50
            """, (str(hours_back),))
            rows = cur.fetchall()
    except Exception as e:
        log(f"❌ DB error querying recent reels for analytics: {e}")
        return
    finally:
        conn.close()

    if not rows:
        log("ℹ️  No reels need analytics refresh right now")
        return

    log(f"🔄 Fetching analytics for {len(rows)} recent reels …")
    concept_map = {c["concept"]: c["title"] for c in REEL_CONCEPTS}

    for row in rows:
        platform      = row["platform"]
        post_id       = row["ayrshare_post_id"]
        concept_key   = row["concept_key"]
        post_url      = row["post_url"]
        posted_at     = row["posted_at"]
        concept_title = concept_map.get(concept_key, concept_key)

        log(f"  📡 {platform} / {concept_key} — ayrshare_id={post_id}")
        analytics = fetch_ayrshare_analytics(post_id, platform)
        if analytics is None:
            log(f"  ⚠️  No analytics returned for {post_id}")
            continue

        fields, score = parse_engagement_score(platform, analytics)
        save_concept_analytics(platform, concept_key, concept_title,
                               post_id, post_url, posted_at,
                               fields, score, analytics)
        update_concept_performance(platform, concept_key, concept_title)
        time.sleep(0.5)


# ======================================================
# PERFORMANCE SUMMARY
# ======================================================
def print_performance_summary():
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT platform, concept_key, concept_title,
                       total_posts, avg_engagement_score,
                       avg_likes, avg_comments, avg_shares, avg_views,
                       is_active, pause_count, stop_reason
                FROM concept_performance_reels
                ORDER BY platform, avg_engagement_score DESC
            """)
            rows = cur.fetchall()

        if not rows:
            log("ℹ️  No reel performance data yet")
            return

        log("\n" + "="*70)
        log("📊 REEL CONCEPT PERFORMANCE SUMMARY")
        log("="*70)
        current_platform = None
        for row in rows:
            if row["platform"] != current_platform:
                current_platform = row["platform"]
                log(f"\n  📱 {current_platform.upper()}")
                log(f"  {'Concept':<30} {'Posts':>5} {'Score':>8} {'Views':>7} {'Likes':>7} {'Cmts':>6} {'Shares':>7} {'Status':>10}")
                log("  " + "-"*80)
            status = "🛑 STOPPED" if (row["stop_reason"] or "").startswith("PERMANENTLY") \
                     else ("⏸️  PAUSED" if not row["is_active"] else "✅ active")
            log(
                f"  {(row['concept_title'] or row['concept_key']):<30}"
                f" {int(row['total_posts']):>5}"
                f" {row['avg_engagement_score']:>8.1f}"
                f" {row['avg_views']:>7.1f}"
                f" {row['avg_likes']:>7.1f}"
                f" {row['avg_comments']:>6.1f}"
                f" {row['avg_shares']:>7.1f}"
                f" {status:>10}"
            )
        log("\n" + "="*70)
    except Exception as e:
        log(f"❌ Error printing performance summary: {e}")
    finally:
        conn.close()


# ======================================================
# CONCEPT LIFECYCLE — PAUSE / STOP BAD CONCEPTS
# ======================================================
def _ensure_pause_columns():
    """Add pause-tracking columns if they don't exist (safe no-op if already present)."""
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            for col, dtype in [
                ("paused_until",       "TIMESTAMP"),
                ("pause_count",        "INTEGER DEFAULT 0"),
                ("stop_reason",        "TEXT"),
                ("reeval_posts_given", "INTEGER DEFAULT 0"),
            ]:
                cur.execute(f"""
                    ALTER TABLE concept_performance_reels
                    ADD COLUMN IF NOT EXISTS {col} {dtype}
                """)
            conn.commit()
    except Exception as e:
        log(f"❌ Error adding pause columns: {e}")
    finally:
        conn.close()


def pause_concept(platform: str, concept_key: str, concept_title: str, reason: str):
    """
    Pause a concept temporarily (PAUSE_DAYS days).
    If pause_count >= MAX_PAUSE_COUNT → permanently stop it.
    """
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT pause_count FROM concept_performance_reels
                WHERE platform = %s AND concept_key = %s
            """, (platform, concept_key))
            row = cur.fetchone()
            current_pauses = int(row["pause_count"] or 0) if row else 0

            if current_pauses >= MAX_PAUSE_COUNT:
                # Permanent stop
                cur.execute("""
                    UPDATE concept_performance_reels
                    SET is_active    = FALSE,
                        paused_until = NULL,
                        stop_reason  = %s,
                        last_updated = NOW()
                    WHERE platform = %s AND concept_key = %s
                """, (f"PERMANENTLY STOPPED after {current_pauses} pauses. {reason}",
                      platform, concept_key))
                conn.commit()
                log(f"🛑 PERMANENTLY STOPPED reel concept '{concept_title}' on {platform}")
                log(f"   Reason: {reason} (paused {current_pauses} times before)")
            else:
                # Temporary pause
                paused_until = datetime.now(TIMEZONE).replace(tzinfo=None) + timedelta(days=PAUSE_DAYS)
                cur.execute("""
                    UPDATE concept_performance_reels
                    SET is_active          = FALSE,
                        paused_until       = %s,
                        pause_count        = pause_count + 1,
                        reeval_posts_given = 0,
                        stop_reason        = NULL,
                        last_updated       = NOW()
                    WHERE platform = %s AND concept_key = %s
                """, (paused_until, platform, concept_key))
                conn.commit()
                log(f"⏸️  PAUSED reel concept '{concept_title}' on {platform} until {paused_until.date()}")
                log(f"   Reason: {reason}  (pause #{current_pauses + 1} of {MAX_PAUSE_COUNT} max)")
    except Exception as e:
        log(f"❌ Error pausing concept: {e}")
    finally:
        conn.close()


def reactivate_concept(platform: str, concept_key: str, concept_title: str):
    """Re-enable a paused concept for re-evaluation."""
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE concept_performance_reels
                SET is_active          = TRUE,
                    paused_until       = NULL,
                    reeval_posts_given = 0,
                    stop_reason        = NULL,
                    last_updated       = NOW()
                WHERE platform = %s AND concept_key = %s
            """, (platform, concept_key))
            conn.commit()
        log(f"♻️  Re-activated reel concept '{concept_title}' on {platform} for re-evaluation")
    except Exception as e:
        log(f"❌ Error reactivating concept: {e}")
    finally:
        conn.close()


def apply_concept_lifecycle(platform: str) -> set:
    """
    Run at the start of every concept selection.
    Checks concept_performance_reels and:
      - Pauses newly-underperforming concepts
      - Re-activates concepts whose cooldown has expired
      - Permanently stops concepts that failed re-evaluation twice
    Returns: set of concept_keys that are BLOCKED right now.
    """
    _ensure_pause_columns()
    blocked: set = set()

    conn = get_db_connection()
    if not conn:
        return blocked

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT concept_key, concept_title, is_active,
                       avg_engagement_score, total_posts,
                       paused_until, pause_count, reeval_posts_given, stop_reason
                FROM concept_performance_reels
                WHERE platform = %s
            """, (platform,))
            rows = cur.fetchall()

        if not rows:
            return blocked

        evaluated = [r for r in rows if int(r["total_posts"] or 0) >= MIN_POSTS_TO_EVALUATE]
        if not evaluated:
            return blocked   # still in evaluation stage

        active_scores = [float(r["avg_engagement_score"] or 0)
                         for r in evaluated if r["is_active"]]
        best_score = max(active_scores) if active_scores else 0
        threshold  = best_score * UNDERPERFORM_RATIO
        now        = datetime.now(TIMEZONE).replace(tzinfo=None)

        for row in evaluated:
            key          = row["concept_key"]
            title        = row["concept_title"] or key
            is_active    = row["is_active"]
            score        = float(row["avg_engagement_score"] or 0)
            paused_until = row["paused_until"]
            stop_reason  = row["stop_reason"]

            # Already permanently stopped
            if stop_reason and stop_reason.startswith("PERMANENTLY STOPPED"):
                blocked.add(key)
                log(f"🚫 {platform} / '{title}': permanently stopped — skipping")
                continue

            # Currently paused
            if not is_active and paused_until:
                if now < paused_until:
                    blocked.add(key)
                    days_left = (paused_until - now).days
                    log(f"⏸️  {platform} / '{title}': paused ({days_left}d left)")
                else:
                    reactivate_concept(platform, key, title)
                    log(f"🔄 {platform} / '{title}': cooldown expired, re-evaluating")
                continue

            # Active — check if it should be paused
            if is_active and score < threshold and best_score > 0:
                reason = (f"avg_score={score:.1f} < threshold={threshold:.1f} "
                          f"(best={best_score:.1f}, ratio={UNDERPERFORM_RATIO})")
                pause_concept(platform, key, title, reason)
                blocked.add(key)
                continue

            # Inactive with no paused_until → permanent stop path
            if not is_active:
                blocked.add(key)

        return blocked

    except Exception as e:
        log(f"❌ Error in apply_concept_lifecycle: {e}")
        return blocked
    finally:
        conn.close()


def increment_reeval_count(platform: str, concept_key: str):
    """Increment re-evaluation post counter after each re-eval post."""
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE concept_performance_reels
                SET reeval_posts_given = reeval_posts_given + 1,
                    last_updated       = NOW()
                WHERE platform = %s AND concept_key = %s
            """, (platform, concept_key))
            conn.commit()
    except Exception as e:
        log(f"❌ Error incrementing reeval count: {e}")
    finally:
        conn.close()


def get_best_concept_for_platform(platform: str) -> dict | None:
    """Return the active REEL_CONCEPTS entry with the highest avg_engagement_score."""
    conn = get_db_connection()
    if not conn:
        return None
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT concept_key, avg_engagement_score
                FROM concept_performance_reels
                WHERE platform = %s
                  AND total_posts >= %s
                  AND is_active = TRUE
                  AND (stop_reason IS NULL OR stop_reason NOT LIKE 'PERMANENTLY STOPPED%%')
                ORDER BY avg_engagement_score DESC
                LIMIT 1
            """, (platform, MIN_POSTS_TO_EVALUATE))
            row = cur.fetchone()
            if not row:
                return None
            best_key = row["concept_key"]
            for c in REEL_CONCEPTS:
                if c["concept"] == best_key:
                    return c
            return None
    except Exception as e:
        log(f"❌ Error fetching best concept: {e}")
        return None
    finally:
        conn.close()


def get_next_concept_smart(platform: str) -> dict:
    """
    Pick the next reel concept for this platform.

    Stage 1 — Evaluation (< MIN_POSTS_TO_EVALUATE posts):
        Round-robin through all concepts via per-platform index file.

    Stage 2 — Enough data:
        Run lifecycle check → build blocked set.
        70% exploit best concept.
        30% explore random non-blocked concept.

    Safety: if all concepts blocked → allow full pool.
    """
    _ensure_pause_columns()

    # Run lifecycle — get blocked set
    blocked = apply_concept_lifecycle(platform)
    log(f"🔒 Blocked reel concepts on {platform}: {blocked if blocked else 'none'}")

    available = [c for c in REEL_CONCEPTS if c["concept"] not in blocked]
    if not available:
        log(f"⚠️  ALL reel concepts blocked on {platform} — safety fallback to full pool")
        available = REEL_CONCEPTS

    # Check if still in evaluation stage
    conn = get_db_connection()
    has_enough_data = False
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM concept_performance_reels
                    WHERE platform = %s AND total_posts >= %s
                """, (platform, MIN_POSTS_TO_EVALUATE))
                has_enough_data = cur.fetchone()[0] > 0
        except:
            pass
        finally:
            conn.close()

    if not has_enough_data:
        # Evaluation stage — rotate through all concepts equally
        index_file = f"eval_reel_index_{platform}.txt"
        idx = 0
        if os.path.exists(index_file):
            try:
                idx = int(open(index_file).read().strip())
            except:
                idx = 0
        concept = available[idx % len(available)]
        with open(index_file, "w") as f:
            f.write(str((idx + 1) % len(available)))
        log(f"🧪 EVALUATION stage — {platform}: reel concept #{idx + 1}: {concept['title']}")
        return concept

    # Enough data — exploit or explore
    best = get_best_concept_for_platform(platform)

    if best and random.random() < 0.70:
        log(f"🏆 EXPLOIT best reel concept for {platform}: {best['title']}")
        return best

    non_best = [c for c in available if not best or c["concept"] != best["concept"]]
    pool = non_best if non_best else available
    chosen = random.choice(pool)
    log(f"🎲 EXPLORE reel concept for {platform}: {chosen['title']}")
    return chosen


# ======================================================
# TRACKING LINK
# ======================================================
def generate_tracking_link(platform, badge_type="marketing", concept_key=None):
    try:
        response = requests.post(
            f"{TRACKING_API_URL}/api/generate-tracking-url",
            json={"platform": platform, "badge_type": badge_type,
                  "username": "nonai_official", "concept_key": concept_key},
            timeout=10,
        )
        if response.status_code == 200:
            data = response.json()
            tracking_url = data.get("tracking_url")
            tracking_id  = data.get("tracking_id")
            log(f"✅ Generated tracking link: {tracking_url}")
            return tracking_url, tracking_id
        else:
            log(f"❌ Tracking API error: {response.status_code}")
            return "https://nonai.life/", None
    except Exception as e:
        log(f"❌ Error generating tracking link: {e}")
        return "https://nonai.life/", None


def confirm_tracking_post(tracking_id, post_url, platform,
                           ayrshare_post_id=None, social_post_id=None):
    if not tracking_id:
        return False
    try:
        response = requests.post(
            f"{TRACKING_API_URL}/api/confirm-post",
            json={"tracking_id": tracking_id, "post_url": post_url,
                  "platform": platform, "username": "nonai_official",
                  "ayrshare_post_id": ayrshare_post_id,
                  "social_post_id":   social_post_id},
            timeout=10,
        )
        if response.status_code == 200:
            log(f"✅ Tracking confirmed for {tracking_id}")
            return True
        else:
            log(f"⚠️ Failed to confirm tracking: {response.status_code}")
            return False
    except Exception as e:
        log(f"❌ Error confirming tracking: {e}")
        return False

# ======================================================
# HASHTAG HELPERS
# ======================================================
def generate_auto_hashtags(caption, max_hashtags=2):
    try:
        log(f"🏷️ Generating auto hashtags (max: {max_hashtags})")
        headers = {
            "Authorization": f"Bearer {AYRSHARE_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {"post": caption, "max": max_hashtags, "position": "end"}
        response = requests.post(
            "https://api.ayrshare.com/api/hashtags/auto",
            json=payload, headers=headers, timeout=30,
        )
        if response.status_code == 200:
            data = response.json()
            log("✅ Auto hashtags generated")
            return data.get("post", caption)
        else:
            log(f"⚠️ Auto hashtag API returned {response.status_code}")
            return caption
    except Exception as e:
        log(f"⚠️ Auto hashtag generation error: {e}")
        return caption


def build_marketing_hashtags(platform="instagram"):
    if platform.lower() == "instagram":
        return " ".join(BRAND_HASHTAGS_MARKETING["core"])
    elif platform.lower() in ["tiktok", "x", "twitter"]:
        tags = BRAND_HASHTAGS_MARKETING["core"][:2]
        tags.append(random.choice(BRAND_HASHTAGS_MARKETING["social"]))
        return " ".join(tags)
    elif platform.lower() == "linkedin":
        tags = list(BRAND_HASHTAGS_MARKETING["core"])
        tags.extend(random.sample(BRAND_HASHTAGS_MARKETING["business"], k=2))
        return " ".join(tags)
    else:
        tags = list(BRAND_HASHTAGS_MARKETING["core"])
        tags.extend(random.sample(BRAND_HASHTAGS_MARKETING["secondary"], k=2))
        return " ".join(tags)

# ======================================================
# CAPTION GENERATION
# ======================================================
try:
    from caption_generation_marketing import generate_marketing_caption as generate_base_caption
    CAPTION_GENERATOR_IMPORTED = True
    log("✅ Imported caption generator from caption_generation_marketing.py")
except ImportError:
    CAPTION_GENERATOR_IMPORTED = False
    log("⚠️ Could not import caption generator — using inline fallback")


def generate_marketing_caption(concept, tracking_url, platform="instagram", retry_limit=5):
    """
    Generate caption for this reel concept.
    Instagram: NO URL (QR in video/bio serves as CTA).
    Other platforms: tracking URL in caption.
    """
    video_concept = concept["concept"]
    base_caption  = ""

    if CAPTION_GENERATOR_IMPORTED:
        try:
            base_caption = generate_base_caption(
                video_concept=video_concept,
                platform=platform,
                retry_limit=retry_limit,
            )
            if platform.lower() == "instagram":
                base_caption = base_caption.replace("https://nonai.life/", "").replace(tracking_url, "")
                base_caption = re.sub(r"\n{3,}", "\n\n", base_caption)
            else:
                base_caption = base_caption.replace("https://nonai.life/", tracking_url)
            log(f"✅ Generated caption for {platform}")
        except Exception as e:
            log(f"⚠️ Caption generator error: {e} — using fallback")

    if not base_caption:
        hashtags = build_marketing_hashtags(platform)
        if platform.lower() == "instagram":
            base_caption = (
                f"{concept['title']}\n\n"
                "In a world where AI can fake anything, proving you're human matters more than ever.\n\n"
                "EssentiaScan: Multi-factor biological verification that AI can't fake.\n\n"
                "Join 50,000+ verified humans.\n\n"
                f"{hashtags}"
            )
        else:
            base_caption = (
                f"{concept['title']}\n\n"
                "In a world where AI can fake anything, proving you're human matters more than ever.\n\n"
                "EssentiaScan: Multi-factor biological verification that AI can't fake.\n\n"
                "Join 50,000+ verified humans.\n\n"
                f"{tracking_url}\n\n"
                f"{hashtags}"
            )

    max_auto = 2 if platform.lower() == "instagram" else 3
    final_caption = generate_auto_hashtags(base_caption, max_hashtags=max_auto)
    save_caption_to_history(final_caption, video_concept, platform)
    return final_caption

# ======================================================
# GEMINI VIDEO GENERATION
# ======================================================
def generate_video(concept):
    """Generate a 15-second vertical reel using Gemini Veo for the given concept."""
    filename = f"reel_{int(time.time())}.mp4"
    prompt   = concept["prompt"]
    try:
        log(f"🎬 Requesting reel from Gemini Veo: {concept['title']}")
        operation = client.models.generate_videos(
            model="veo-3.0-fast-generate-001",
            prompt=prompt,
            config=types.GenerateVideosConfig(aspect_ratio="9:16"),
        )
        while not operation.done:
            log("⏳ Generating reel …")
            time.sleep(10)
            operation = client.operations.get(operation)

        if not operation.response:
            log("❌ No response from Gemini Veo")
            return None

        video_file = operation.response.generated_videos[0].video
        client.files.download(file=video_file)
        video_file.save(filename)

        if os.path.exists(filename):
            log(f"✅ Reel saved: {filename}")
            return filename
        else:
            log("❌ File save failed")
            return None
    except Exception as e:
        log(f"💥 Gemini Veo error: {e}")
        return None

# ======================================================
# AYRSHARE UPLOAD
# ======================================================
def upload_media(path, retries=3):
    headers = {"Authorization": f"Bearer {AYRSHARE_API_KEY}"}
    for attempt in range(retries):
        try:
            log(f"📤 Upload attempt {attempt + 1}")
            with open(path, "rb") as f:
                files = {"file": (os.path.basename(path), f, "video/mp4")}
                res = requests.post(
                    "https://app.ayrshare.com/api/media/upload",
                    headers=headers, files=files, timeout=120,
                )
            log(f"📡 Status: {res.status_code}")
            try:
                data = res.json()
            except:
                log(res.text[:500])
                data = {}
            if res.status_code == 200 and data.get("url"):
                log("✅ Upload success")
                return data["url"]
        except Exception as e:
            log(f"💥 Upload exception: {e}")
        time.sleep(3)
    return None

# ======================================================
# AYRSHARE POST
# ======================================================
def create_post(media_url, caption, platforms, retries=3):
    """
    Publish to Ayrshare and return:
        (success, post_urls, ayrshare_post_id, social_post_ids)

    ayrshare_post_id : top-level data['id'] — used for /api/analytics/post
    social_post_ids  : dict platform → platform-native post ID
    """
    headers = {
        "Authorization": f"Bearer {AYRSHARE_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "post":      caption,
        "platforms": platforms,
        "mediaUrls": [media_url],
        "isVideo":   True,   # explicit flag for non-.mp4 URLs
    }

    for attempt in range(retries):
        try:
            log(f"📲 Post attempt {attempt + 1} → {platforms}")
            res = requests.post(
                "https://api.ayrshare.com/api/post",   # correct hostname
                json=payload, headers=headers, timeout=60,
            )
            log(f"📡 Status: {res.status_code}")
            try:
                data = res.json()
            except Exception:
                log(res.text[:500])
                data = {}

            log(f"📋 Full response: {json.dumps(data, indent=2)}")

            if res.status_code == 200 and data.get("status") == "success":
                log("✅ POST SUCCESS")

                # Top-level Ayrshare Post ID — used for analytics API
                ayrshare_post_id = data.get("id")
                log(f"🆔 Ayrshare Post ID: {ayrshare_post_id}")

                post_urls       = {}
                social_post_ids = {}

                for item in data.get("postIds", []):
                    if not isinstance(item, dict):
                        continue
                    pname          = item.get("platform")
                    post_url       = item.get("postUrl")
                    social_post_id = item.get("id")

                    if pname:
                        if post_url:
                            post_urls[pname] = post_url
                            log(f"  ✅ {pname} postUrl: {post_url}")
                        else:
                            log(f"  ⚠️  {pname} posted but no postUrl")

                        if social_post_id and str(social_post_id) != "pending":
                            social_post_ids[pname] = str(social_post_id)
                            log(f"  🔖 {pname} social post ID: {social_post_id}")
                        else:
                            log(f"  ℹ️  {pname} social post ID: {social_post_id} (pending/missing)")

                for p in platforms:
                    if p not in post_urls:
                        log(f"  ⚠️  No postUrl for {p}")

                return True, post_urls, ayrshare_post_id, social_post_ids

        except Exception as e:
            log(f"💥 Post exception: {e}")
        time.sleep(5)

    return False, {}, None, {}

# ======================================================
# MAIN RUN
# ======================================================
def run():
    log("🚀 NonAI/EssentiaScan REEL Scheduler — Smart Concept Selection + Analytics")

    # Init / migrate all tables
    init_caption_table()
    init_posted_slots_table()
    init_concept_analytics_table()
    init_concept_performance_table()

    # ── Step 1: Refresh Ayrshare analytics for recent reels ──
    log("\n📡 Refreshing Ayrshare analytics for recent reels …")
    fetch_and_store_recent_analytics(hours_back=48)

    # ── Step 2: Print performance summary ────────────────────
    print_performance_summary()

    # ── Step 3: Check schedule ───────────────────────────────
    platform_slots = platforms_to_post_now()
    if not platform_slots:
        log("⏭ Nothing scheduled now")
        return

    pending_slots = [
        (platform, hour, minute)
        for platform, hour, minute in platform_slots
        if not already_posted(platform, hour, minute)
    ]

    if not pending_slots:
        log("⏭ Already posted for all platforms in this slot")
        return

    log(f"📋 Posting reels to {len(pending_slots)} platforms:")
    for platform, hour, minute in pending_slots:
        log(f"   • {platform.capitalize()} (slot: {hour:02d}:{minute:02d})")

    # ── Step 4: Process each platform ───────────────────────
    tracking_data = {}

    for platform, target_hour, target_minute in pending_slots:
        log(f"\n{'='*60}")
        log(f"📱 Processing {platform.upper()}")
        log(f"{'='*60}")

        # Smart concept selection (lifecycle check happens inside)
        concept = get_next_concept_smart(platform)
        log(f"🎬 Concept selected: {concept['title']} ({concept['concept']})")

        # Generate tracking URL — pass concept_key immediately
        tracking_url, tracking_id = generate_tracking_link(
            platform, badge_type="marketing", concept_key=concept["concept"]
        )

        tracking_data[platform] = {
            "concept":        concept,
            "tracking_url":   tracking_url,
            "tracking_id":    tracking_id,
            "target_hour":    target_hour,
            "target_minute":  target_minute,
        }

    # ── Step 5: Generate ONE video per unique concept ────────
    # (multiple platforms might share the same concept this run)
    concept_videos: dict = {}   # concept_key → local filename

    for platform, td in tracking_data.items():
        ck = td["concept"]["concept"]
        if ck not in concept_videos:
            video_file = generate_video(td["concept"])
            concept_videos[ck] = video_file   # may be None if generation failed

    # ── Step 6: Upload each unique video once ────────────────
    concept_media_urls: dict = {}   # concept_key → CDN URL

    for ck, video_file in concept_videos.items():
        if not video_file:
            log(f"❌ Skipping concept '{ck}': video generation failed")
            continue
        media_url = upload_media(video_file)
        concept_media_urls[ck] = media_url   # may be None if upload failed
        if not media_url:
            log(f"❌ Upload failed for concept '{ck}'")

    # ── Step 7: Generate captions per platform ───────────────
    for platform, td in tracking_data.items():
        caption = generate_marketing_caption(
            concept=td["concept"],
            tracking_url=td["tracking_url"],
            platform=platform,
        )
        td["caption"] = caption
        log(f"📝 Caption ready for {platform}: {caption[:120]}{'…' if len(caption) > 120 else ''}")

    # ── Step 8: Post each platform ───────────────────────────
    log("\n" + "="*60)
    log("📱 POSTING REELS TO PLATFORMS")
    log("="*60)

    for platform in tracking_data:
        log(f"\n{'─'*60}")
        log(f"📤 Posting reel to {platform.upper()}")
        log(f"{'─'*60}")

        td            = tracking_data[platform]
        concept       = td["concept"]
        caption       = td.get("caption", "")
        tracking_id   = td["tracking_id"]
        tracking_url  = td["tracking_url"]
        target_hour   = td["target_hour"]
        target_minute = td["target_minute"]

        media_url = concept_media_urls.get(concept["concept"])
        if not media_url:
            log(f"❌ No media URL for {platform} / {concept['concept']} — skipping")
            continue

        success, post_urls, ayrshare_post_id, social_post_ids = create_post(
            media_url, caption, [platform]
        )

        if success:
            actual_post_url = post_urls.get(platform)
            social_post_id  = social_post_ids.get(platform)

            mark_posted(
                platform, target_hour, target_minute,
                actual_post_url, tracking_id,
                ayrshare_post_id=ayrshare_post_id,
                concept_key=concept["concept"],
                social_post_id=social_post_id,
            )

            if tracking_id and actual_post_url:
                confirm_tracking_post(
                    tracking_id, actual_post_url, platform,
                    ayrshare_post_id=ayrshare_post_id,
                    social_post_id=social_post_id,
                )

            # Track re-evaluation posts if concept was recently re-activated
            increment_reeval_count(platform, concept["concept"])

            log(f"✅ {platform.capitalize()} reel posted")
            log(f"   📊 Tracking:          {tracking_url}")
            log(f"   🔗 Post URL:          {actual_post_url}")
            log(f"   🎬 Concept:           {concept['title']} ({concept['concept']})")
            log(f"   🆔 Ayrshare Post ID:  {ayrshare_post_id}  ← used for analytics API")
            log(f"   🔖 Social Post ID:    {social_post_id}  ← platform-native ID")
            log(f"   🕐 Slot:              {target_hour:02d}:{target_minute:02d}")

            if platform.lower() == "instagram":
                log(f"   📝 Caption: NO URL (Instagram links not clickable)")
                log(f"   🏷️  Hashtags: 3 brand + 2 Ayrshare auto = 5 total")
            else:
                log(f"   📝 Caption: includes tracking URL")
                log(f"   🏷️  Hashtags: 3 brand + 3 Ayrshare auto = 6 total")
        else:
            log(f"❌ Failed to post reel to {platform}")

        time.sleep(2)

    # ── Step 9: Cleanup local video files ────────────────────
    for video_file in concept_videos.values():
        if video_file and os.path.exists(video_file):
            os.remove(video_file)
            log(f"🗑 Cleaned up {video_file}")

    log("\n" + "="*60)
    log("✅ REEL POSTING COMPLETE")
    log("="*60)


if __name__ == "__main__":
    run()