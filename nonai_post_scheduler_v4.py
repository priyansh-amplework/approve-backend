"""
nonai_scheduled_image_posts.py
Run via cron every 15 minutes.

Purpose:
- Create & upload STATIC IMAGES for EssentiaScan/NonAI campaigns ONLY
- Posts to all platforms according to client weekly schedule
- Generates unique captions with tracking links using imported caption generator
- Uses brand hashtags + Ayrshare auto-hashtags (5 total for Instagram)
- Avoids duplicate posting per platform per slot
- Logs all activity
- Uses Gemini 2.5 Flash Image for AI-generated marketing images
- TRACKS concept performance per platform via Ayrshare analytics
- AUTO-REPLACES underperforming concepts with top performers
"""

import os
import sys
import time
import json
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import google.generativeai as genai_caption
from pinecone import Pinecone
import psycopg2
from psycopg2.extras import RealDictCursor
import re
import random
import qrcode
from PIL import Image


# ======================================================
# ENV
# ======================================================
load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
AYRSHARE_API_KEY = os.getenv("AYESHARE_API_KEY")
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
TRACKING_API_URL = os.getenv("TRACKING_API_URL", "http://44.193.35.107:8000")
IMAGEN_API_KEY = os.getenv("GEMINI_API_KEY")

if not GEMINI_API_KEY or not AYRSHARE_API_KEY or not DATABASE_URL:
    print("❌ Missing API keys")
    sys.exit(1)

# ======================================================
# CONFIG
# ======================================================
TIMEZONE = ZoneInfo("America/New_York")
PROMPT_FILE = "last_prompt_index.txt"

# ── Performance thresholds ────────────────────────────
# A concept is "underperforming" if its avg engagement score is below this
# relative to the top concept on that platform.
UNDERPERFORM_RATIO = 0.4   # < 40% of the best concept's score → replace
MIN_POSTS_TO_EVALUATE = 3   # need at least this many posts before judging

# ======================================================
# STATIC IMAGE POSTING SCHEDULE (ET)
# ======================================================
# POSTING_SCHEDULE = {
#     "instagram": [
#         {"days": [0, 1, 2, 3, 4], "hour": 9,  "minute": 0},
#         {"days": [0, 1, 2, 3, 4], "hour": 12, "minute": 30},
#         {"days": [0, 1, 2, 3, 4], "hour": 18, "minute": 0},
#         {"days": [5, 6],          "hour": 10, "minute": 0},
#         {"days": [5, 6],          "hour": 19, "minute": 0},
#     ],
#     "facebook": [
#         {"days": [0, 2, 4], "hour": 10, "minute": 0},
#         {"days": [1, 3],    "hour": 14, "minute": 0},
#         {"days": [5, 6],    "hour": 11, "minute": 0},
#     ],
#     "linkedin": [
#         {"days": [0, 2, 4], "hour": 8,  "minute": 30},
#         {"days": [1, 3],    "hour": 12, "minute": 0},
#     ],
#     "x": [
#         {"days": [0, 1, 2, 3, 4], "hour": 11, "minute": 0},
#         {"days": [0, 1, 2, 3, 4], "hour": 15, "minute": 0},
#         {"days": [5, 6],          "hour": 13, "minute": 0},
#     ],
# }
POSTING_SCHEDULE = {
     "instagram": [
         {"days": [0, 1, 2, 3, 4], "hour": 9,  "minute": 0},
         {"days": [0, 1, 2, 3, 4], "hour": 12, "minute": 30},
         {"days": [0, 1, 2, 3, 4], "hour": 18, "minute": 0},
         {"days": [5, 6],          "hour": 10, "minute": 0},
         {"days": [5, 6],          "hour": 19, "minute": 0},
     ]
}

# ======================================================
# IMAGE CONCEPTS - EssentiaScan/NonAI ONLY
# ======================================================
IMAGE_CONCEPTS = [
    {
        "campaign": "EssentiaScan",
        "title": "Deepfake Panic Call",
        "concept": "deepfake_protection",
        "description": "Cyber awareness: phone showing AI voice clone warning, verification screen, Non-AI Score check",
        "text_overlay": "AI can copy a voice.\nDon't trust panic.\nVerify the human.\n\nEssentiaScan",
        "color_scheme": "tech_red_alert",
        "style": "modern, tech UI, security focused"
    },
    {
        "campaign": "EssentiaScan",
        "title": "Catfish Reveal",
        "concept": "social_media_trust",
        "description": "Dating safety: dating profile glitching, AI-generated images warning, verification failed",
        "text_overlay": "If it feels unreal...\nIt might be.\nVerify before you trust.\n\nEssentiaScan",
        "color_scheme": "purple_caution",
        "style": "modern dating app aesthetic, glitch effects"
    },
    {
        "campaign": "EssentiaScan",
        "title": "Non-AI Score Challenge",
        "concept": "human_verification_intro",
        "description": "Social fun: friends comparing Non-AI Scores, leaderboard, competitive energy, coffee shop",
        "text_overlay": "What's your Non-AI Score?\nBeat my score.\nProve you're real.\n\nEssentiaScan",
        "color_scheme": "vibrant_social",
        "style": "energetic, playful, social media aesthetic"
    },
    {
        "campaign": "EssentiaScan",
        "title": "Gaming & Streaming Proof",
        "concept": "ai_impersonation_fight",
        "description": "Gaming: stream overlay, bot lobby warning, Non-AI Score verification, real players badge",
        "text_overlay": "Bots ruin games.\nVerify players.\nKeep it human.\n\nEssentiaScan",
        "color_scheme": "neon_gaming",
        "style": "gaming stream overlay, cyberpunk aesthetic"
    },
    {
        "campaign": "EssentiaScan",
        "title": "Social Proof: Verified Human",
        "concept": "creator_authenticity",
        "description": "Creator empowerment: content creator posting, Verified Human badge, Non-AI Score 90+",
        "text_overlay": "Trust is scarce.\nProof is power.\nShare your score.\n\nEssentiaScan",
        "color_scheme": "creator_gold",
        "style": "inspiring, modern creator aesthetic, badge overlay"
    },
    {
        "campaign": "EssentiaScan",
        "title": "Identity Theft Protection",
        "concept": "deepfake_protection",
        "description": "Security alert: warning screen showing deepfake detected, biometric verification active",
        "text_overlay": "AI can steal your face.\nYour voice. Your identity.\nFight back.\n\nEssentiaScan",
        "color_scheme": "security_red",
        "style": "urgent, security-focused, alert aesthetic"
    },
    {
        "campaign": "EssentiaScan",
        "title": "Workplace Verification",
        "concept": "human_verification_intro",
        "description": "Professional setting: business person verifying identity for remote meeting, corporate security",
        "text_overlay": "Remote work requires trust.\nVerify who's really on the call.\n\nEssentiaScan",
        "color_scheme": "professional_blue",
        "style": "corporate, professional, trustworthy"
    },
    {
        "campaign": "EssentiaScan",
        "title": "Bot Army Detection",
        "concept": "ai_impersonation_fight",
        "description": "Social media dashboard showing bot accounts being flagged, verification filters active",
        "text_overlay": "Bots flood your feed.\nFilter the fake.\nKeep it human.\n\nEssentiaScan",
        "color_scheme": "matrix_green",
        "style": "tech dashboard, data visualization, alert system"
    },
    {
        "campaign": "EssentiaScan",
        "title": "Future of Identity",
        "concept": "creator_authenticity",
        "description": "Futuristic biometric scan, human verification technology, digital identity protection",
        "text_overlay": "The future needs proof.\nYou are human.\nProve it.\n\nEssentiaScan",
        "color_scheme": "futuristic_cyan",
        "style": "sci-fi, futuristic, biometric technology"
    },
    {
        "campaign": "EssentiaScan",
        "title": "Family Safety Online",
        "concept": "social_media_trust",
        "description": "Parent and child using device safely, parental controls with human verification enabled",
        "text_overlay": "Protect who you love.\nVerify who they talk to.\n\nEssentiaScan",
        "color_scheme": "warm_protective",
        "style": "family-friendly, protective, caring"
    },
]

# ======================================================
# BRAND HASHTAGS
# ======================================================
BRAND_HASHTAGS_ESSENTIASCAN = {
    "core":      ["#NonAI", "#EssentiaScan", "#VerifiedHuman"],
    "secondary": ["#HumanVerification", "#BiologicalFirewall", "#ProveYoureHuman"],
    "business":  ["#DeepfakeDefense", "#IdentitySecurity", "#ZeroTrust"],
    "social":    ["#RealHuman", "#HumanOnly", "#AIFree"]
}

# ======================================================
# GEMINI CLIENT
# ======================================================
genai_caption.configure(api_key=GEMINI_API_KEY)
model_caption = genai_caption.GenerativeModel("gemini-2.0-flash-exp")
model_image   = genai_caption.GenerativeModel("gemini-2.5-flash-image")

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
        return psycopg2.connect(DATABASE_URL )
    except Exception as e:
        log(f"❌ DB connection error: {e}")
        return None

# ──────────────────────────────────────────────────────
# SCHEMA INIT
# ──────────────────────────────────────────────────────
def init_caption_table():
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS caption_history_marketing (
                    id SERIAL PRIMARY KEY,
                    caption TEXT NOT NULL,
                    video_concept VARCHAR(100),
                    platform VARCHAR(50) NOT NULL,
                    timestamp TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            conn.commit()
        return True
    finally:
        conn.close()

def init_posted_slots_table():
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS posted_slots (
                    id SERIAL PRIMARY KEY,
                    platform VARCHAR(50) NOT NULL,
                    post_date DATE NOT NULL,
                    target_hour INTEGER NOT NULL,
                    target_minute INTEGER NOT NULL,
                    posted_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    post_url TEXT,
                    tracking_id VARCHAR(100),
                    ayrshare_post_id TEXT,
                    concept_key VARCHAR(100),
                    UNIQUE(platform, post_date, target_hour, target_minute)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_posted_slots_lookup
                ON posted_slots(platform, post_date, target_hour, target_minute)
            """)
            conn.commit()
        log("✅ Posted slots table initialized")
        return True
    except Exception as e:
        log(f"❌ Error initializing posted_slots table: {e}")
        return False
    finally:
        conn.close()

def init_concept_analytics_table():
    """
    New table: stores per-concept, per-platform engagement snapshots
    pulled from Ayrshare analytics.
    """
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS concept_analytics (
                    id SERIAL PRIMARY KEY,
                    platform VARCHAR(50) NOT NULL,
                    concept_key VARCHAR(100) NOT NULL,
                    concept_title VARCHAR(200),
                    ayrshare_post_id TEXT,
                    post_url TEXT,
                    posted_at TIMESTAMP,
                    -- Raw analytics fields (nullable – not every platform returns all)
                    likes INTEGER DEFAULT 0,
                    comments INTEGER DEFAULT 0,
                    shares INTEGER DEFAULT 0,
                    impressions INTEGER DEFAULT 0,
                    reach INTEGER DEFAULT 0,
                    views INTEGER DEFAULT 0,
                    saves INTEGER DEFAULT 0,
                    -- Computed engagement score (weighted sum)
                    engagement_score FLOAT DEFAULT 0,
                    analytics_fetched_at TIMESTAMP,
                    raw_analytics JSONB
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_concept_analytics_platform_concept
                ON concept_analytics(platform, concept_key)
            """)
            conn.commit()
        log("✅ Concept analytics table initialized")
        return True
    except Exception as e:
        log(f"❌ Error initializing concept_analytics table: {e}")
        return False
    finally:
        conn.close()

def init_concept_performance_table():
    """
    Aggregated view: best/worst concepts per platform.
    Upserted after every analytics fetch.
    """
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS concept_performance (
                    platform VARCHAR(50) NOT NULL,
                    concept_key VARCHAR(100) NOT NULL,
                    concept_title VARCHAR(200),
                    total_posts INTEGER DEFAULT 0,
                    avg_engagement_score FLOAT DEFAULT 0,
                    total_engagement_score FLOAT DEFAULT 0,
                    avg_likes FLOAT DEFAULT 0,
                    avg_comments FLOAT DEFAULT 0,
                    avg_shares FLOAT DEFAULT 0,
                    avg_impressions FLOAT DEFAULT 0,
                    avg_reach FLOAT DEFAULT 0,
                    avg_views FLOAT DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT NOW(),
                    is_active BOOLEAN DEFAULT TRUE,
                    PRIMARY KEY (platform, concept_key)
                )
            """)
            conn.commit()
        log("✅ Concept performance table initialized")
        return True
    except Exception as e:
        log(f"❌ Error initializing concept_performance table: {e}")
        return False
    finally:
        conn.close()

# ──────────────────────────────────────────────────────
# CAPTION HISTORY
# ──────────────────────────────────────────────────────
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

# ──────────────────────────────────────────────────────
# DUPLICATE PREVENTION
# ──────────────────────────────────────────────────────
def already_posted(platform, target_hour, target_minute):
    conn = get_db_connection()
    if not conn:
        return False
    try:
        today = datetime.now(TIMEZONE).date()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM posted_slots
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
    ayrshare_post_id : top-level "id" from Ayrshare response
                       -> used by /api/analytics/post endpoint
    social_post_id   : per-platform "id" inside postIds[]
                       -> the social network own post ID (IG, FB, etc.)
    """
    conn = get_db_connection()
    if not conn:
        return False
    try:
        today = datetime.now(TIMEZONE).date()
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE posted_slots
                ADD COLUMN IF NOT EXISTS social_post_id TEXT
            """)
            cur.execute("""
                INSERT INTO posted_slots
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
    Fetch post analytics from Ayrshare for a specific post.
    Returns the analytics dict for the given platform, or None on failure.
    """
    try:
        headers = {
            "Authorization": f"Bearer {AYRSHARE_API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "id": ayrshare_post_id,
            "platforms": [platform]
        }
        resp = requests.post(
            "https://api.ayrshare.com/api/analytics/post",
            json=payload,
            headers=headers,
            timeout=30
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get(platform, {}).get("analytics")
        else:
            log(f"⚠️ Ayrshare analytics returned {resp.status_code}: {resp.text[:200]}")
            return None
    except Exception as e:
        log(f"❌ Error fetching Ayrshare analytics: {e}")
        return None

def parse_engagement_score(platform: str, analytics: dict) -> tuple[dict, float]:
    """
    Extract numeric engagement fields from raw Ayrshare analytics and
    compute a weighted engagement score (platform-aware).

    Returns (fields_dict, score_float).
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
        fields["views"]       = analytics.get("viewsCount", 0) or 0
        fields["saves"]       = analytics.get("savedCount", 0) or 0
        fields["impressions"] = fields["views"]

    elif p == "facebook":
        fields["likes"]       = analytics.get("likeCount", 0) or 0
        fields["comments"]    = analytics.get("commentsCount", 0) or 0
        fields["shares"]      = analytics.get("sharesCount", 0) or 0
        fields["impressions"] = analytics.get("impressionsUnique", 0) or 0
        reactions             = analytics.get("reactions", {}) or {}
        fields["likes"]       = max(fields["likes"], reactions.get("total", 0) or 0)

    elif p == "linkedin":
        fields["likes"]       = analytics.get("likeCount", 0) or 0
        fields["comments"]    = analytics.get("commentCount", 0) or 0
        fields["shares"]      = analytics.get("shareCount", 0) or 0
        fields["impressions"] = analytics.get("impressionCount", 0) or 0
        fields["reach"]       = analytics.get("uniqueImpressionsCount", 0) or 0

    elif p in ("x", "twitter"):
        pub = analytics.get("publicMetrics", {}) or {}
        fields["likes"]       = pub.get("likeCount", 0) or 0
        fields["comments"]    = pub.get("replyCount", 0) or 0
        fields["shares"]      = pub.get("retweetCount", 0) or 0
        fields["impressions"] = pub.get("impressionCount", 0) or 0

    # Weighted engagement score
    score = (
        fields["likes"]       * 1.0
        + fields["comments"]  * 2.0
        + fields["shares"]    * 3.0
        + fields["saves"]     * 2.5
        + fields["reach"]     * 0.1
        + fields["impressions"] * 0.05
    )
    return fields, round(score, 2)

# ======================================================
# SAVE / UPDATE CONCEPT ANALYTICS
# ======================================================
def save_concept_analytics(platform: str, concept_key: str, concept_title: str,
                            ayrshare_post_id: str, post_url: str,
                            posted_at, fields: dict, score: float,
                            raw: dict):
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO concept_analytics
                (platform, concept_key, concept_title, ayrshare_post_id, post_url,
                 posted_at, likes, comments, shares, impressions, reach, views, saves,
                 engagement_score, analytics_fetched_at, raw_analytics)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s)
            """, (
                platform, concept_key, concept_title, ayrshare_post_id, post_url,
                posted_at,
                fields.get("likes", 0), fields.get("comments", 0),
                fields.get("shares", 0), fields.get("impressions", 0),
                fields.get("reach", 0), fields.get("views", 0),
                fields.get("saves", 0), score,
                json.dumps(raw) if raw else None
            ))
            conn.commit()
        log(f"💾 Saved analytics for concept '{concept_key}' on {platform} (score={score})")
    except Exception as e:
        log(f"❌ Error saving concept analytics: {e}")
    finally:
        conn.close()

def update_concept_performance(platform: str, concept_key: str, concept_title: str):
    """
    Recompute aggregate stats for (platform, concept_key) from all stored rows
    and upsert into concept_performance.
    """
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*)            AS total_posts,
                    AVG(engagement_score) AS avg_score,
                    SUM(engagement_score) AS total_score,
                    AVG(likes)          AS avg_likes,
                    AVG(comments)       AS avg_comments,
                    AVG(shares)         AS avg_shares,
                    AVG(impressions)    AS avg_impressions,
                    AVG(reach)          AS avg_reach,
                    AVG(views)          AS avg_views
                FROM concept_analytics
                WHERE platform = %s AND concept_key = %s
            """, (platform, concept_key))
            row = cur.fetchone()
            if not row or not row["total_posts"]:
                return

            cur.execute("""
                INSERT INTO concept_performance
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
        log(f"📊 Updated performance for '{concept_key}' on {platform}")
    except Exception as e:
        log(f"❌ Error updating concept performance: {e}")
    finally:
        conn.close()

# ======================================================
# FETCH & STORE ANALYTICS FOR RECENT POSTS
# ======================================================
def fetch_and_store_recent_analytics(hours_back: int = 48):
    """
    Pull recent posted_slots rows that have an ayrshare_post_id but whose
    analytics haven't been fetched yet (or were fetched > 6 h ago).
    Fetch from Ayrshare, store in concept_analytics, update performance.

    Called once per scheduler run so analytics stay fresh.
    """
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Posts from the last `hours_back` hours that have an ayrshare_post_id
            cur.execute("""
                SELECT ps.platform, ps.ayrshare_post_id, ps.concept_key,
                       ps.post_url, ps.posted_at
                FROM posted_slots ps
                WHERE ps.ayrshare_post_id IS NOT NULL
                  AND ps.concept_key IS NOT NULL
                  AND ps.posted_at >= NOW() - INTERVAL '%s hours'
                  AND NOT EXISTS (
                      SELECT 1 FROM concept_analytics ca
                      WHERE ca.ayrshare_post_id = ps.ayrshare_post_id
                        AND ca.analytics_fetched_at >= NOW() - INTERVAL '6 hours'
                  )
                ORDER BY ps.posted_at DESC
                LIMIT 50
            """, (hours_back,))
            rows = cur.fetchall()
    except Exception as e:
        log(f"❌ DB error querying recent posts for analytics: {e}")
        return
    finally:
        conn.close()

    if not rows:
        log("ℹ️  No posts need analytics refresh right now")
        return

    log(f"🔄 Fetching analytics for {len(rows)} recent posts …")

    # Build a lookup for concept title
    concept_map = {c["concept"]: c["title"] for c in IMAGE_CONCEPTS}

    for row in rows:
        platform       = row["platform"]
        post_id        = row["ayrshare_post_id"]
        concept_key    = row["concept_key"]
        post_url       = row["post_url"]
        posted_at      = row["posted_at"]
        concept_title  = concept_map.get(concept_key, concept_key)

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
        time.sleep(0.5)   # avoid hammering the API

# ======================================================
# SMART CONCEPT SELECTION  —  PAUSE / STOP BAD CONCEPTS
# ======================================================
#
# How it works (3-stage lifecycle per platform):
#
#   STAGE 1 — EVALUATION (total_posts < MIN_POSTS_TO_EVALUATE)
#       All concepts are eligible. Round-robin so every concept
#       gets tested before any judgement is made.
#
#   STAGE 2 — ACTIVE  (is_active = TRUE, not paused)
#       Concept has enough data.
#       Score >= UNDERPERFORM_RATIO × best_score  → keep posting (weighted random)
#       Score <  UNDERPERFORM_RATIO × best_score  → mark PAUSED with timestamp
#
#   STAGE 3 — PAUSED  (is_active = FALSE, paused_until set)
#       Concept is COMPLETELY skipped — no posts generated at all.
#       After PAUSE_DAYS days it gets one "re-evaluation" post.
#       If it still underperforms after RE_EVAL_POSTS more posts → STOPPED permanently.
#       If it recovers (score above threshold) → reactivated to ACTIVE.
#
# The concept_performance table drives everything:
#   is_active      BOOLEAN  — FALSE = paused/stopped
#   paused_until   TIMESTAMP — when to give it another chance
#   pause_count    INTEGER  — how many times it has been paused
#   stop_reason    TEXT     — human-readable reason logged when stopped permanently
# ======================================================

PAUSE_DAYS       = 7    # days before a paused concept gets a re-evaluation post
RE_EVAL_POSTS    = 2    # extra posts given during re-evaluation before final verdict
MAX_PAUSE_COUNT  = 2    # after this many pauses → concept is stopped permanently


def _ensure_pause_columns():
    """Add pause-tracking columns to concept_performance if they don't exist yet."""
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            for col, dtype in [
                ("paused_until", "TIMESTAMP"),
                ("pause_count",  "INTEGER DEFAULT 0"),
                ("stop_reason",  "TEXT"),
                ("reeval_posts_given", "INTEGER DEFAULT 0"),
            ]:
                cur.execute(f"""
                    ALTER TABLE concept_performance
                    ADD COLUMN IF NOT EXISTS {col} {dtype}
                """)
            conn.commit()
    except Exception as e:
        log(f"❌ Error adding pause columns: {e}")
    finally:
        conn.close()


def pause_concept(platform: str, concept_key: str, concept_title: str, reason: str):
    """
    Mark a concept as paused for PAUSE_DAYS days.
    If it has been paused MAX_PAUSE_COUNT times already → stop it permanently.
    """
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get current pause_count
            cur.execute("""
                SELECT pause_count FROM concept_performance
                WHERE platform = %s AND concept_key = %s
            """, (platform, concept_key))
            row = cur.fetchone()
            current_pauses = int(row["pause_count"] or 0) if row else 0

            if current_pauses >= MAX_PAUSE_COUNT:
                # Permanent stop
                cur.execute("""
                    UPDATE concept_performance
                    SET is_active    = FALSE,
                        paused_until = NULL,
                        stop_reason  = %s,
                        last_updated = NOW()
                    WHERE platform = %s AND concept_key = %s
                """, (f"PERMANENTLY STOPPED after {current_pauses} pauses. {reason}",
                      platform, concept_key))
                conn.commit()
                log(f"🛑 PERMANENTLY STOPPED concept '{concept_title}' on {platform}: {reason}")
                log(f"   (had been paused {current_pauses} times before)")
            else:
                # Temporary pause
                paused_until = datetime.now(TIMEZONE).replace(tzinfo=None) + timedelta(days=PAUSE_DAYS)
                cur.execute("""
                    UPDATE concept_performance
                    SET is_active          = FALSE,
                        paused_until       = %s,
                        pause_count        = pause_count + 1,
                        reeval_posts_given = 0,
                        stop_reason        = NULL,
                        last_updated       = NOW()
                    WHERE platform = %s AND concept_key = %s
                """, (paused_until, platform, concept_key))
                conn.commit()
                log(f"⏸️  PAUSED concept '{concept_title}' on {platform} until {paused_until.date()}")
                log(f"   Reason: {reason}  (pause #{current_pauses + 1} of {MAX_PAUSE_COUNT} max)")
    except Exception as e:
        log(f"❌ Error pausing concept: {e}")
    finally:
        conn.close()


def reactivate_concept(platform: str, concept_key: str, concept_title: str):
    """Re-enable a paused concept and give it a clean slate for re-evaluation."""
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE concept_performance
                SET is_active          = TRUE,
                    paused_until       = NULL,
                    reeval_posts_given = 0,
                    stop_reason        = NULL,
                    last_updated       = NOW()
                WHERE platform = %s AND concept_key = %s
            """, (platform, concept_key))
            conn.commit()
        log(f"♻️  Re-activated concept '{concept_title}' on {platform} for re-evaluation")
    except Exception as e:
        log(f"❌ Error reactivating concept: {e}")
    finally:
        conn.close()


def apply_concept_lifecycle(platform: str):
    """
    Called once per scheduler run BEFORE concept selection.
    Reads concept_performance and:
      - Pauses newly-underperforming concepts
      - Re-activates concepts whose PAUSE_DAYS window has expired
      - Permanently stops concepts that failed re-evaluation
    Returns set of concept_keys that are BLOCKED (paused or stopped) right now.
    """
    _ensure_pause_columns()

    blocked: set[str] = set()
    conn = get_db_connection()
    if not conn:
        return blocked

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT concept_key, concept_title, is_active,
                       avg_engagement_score, total_posts,
                       paused_until, pause_count, reeval_posts_given, stop_reason
                FROM concept_performance
                WHERE platform = %s
            """, (platform,))
            rows = cur.fetchall()

        if not rows:
            return blocked

        # Best score among active concepts with enough data
        evaluated = [r for r in rows if int(r["total_posts"] or 0) >= MIN_POSTS_TO_EVALUATE]
        if not evaluated:
            return blocked  # still in evaluation stage — nothing to block

        active_scores = [float(r["avg_engagement_score"] or 0)
                         for r in evaluated if r["is_active"]]
        best_score = max(active_scores) if active_scores else 0
        threshold  = best_score * UNDERPERFORM_RATIO

        now = datetime.now(TIMEZONE).replace(tzinfo=None)

        for row in evaluated:
            key       = row["concept_key"]
            title     = row["concept_title"] or key
            is_active = row["is_active"]
            score     = float(row["avg_engagement_score"] or 0)
            paused_until = row["paused_until"]
            stop_reason  = row["stop_reason"]
            reeval_given = int(row["reeval_posts_given"] or 0)

            # ── Already permanently stopped ───────────────────────────────
            if stop_reason and stop_reason.startswith("PERMANENTLY STOPPED"):
                blocked.add(key)
                log(f"🚫 {platform} / '{title}': permanently stopped — skipping")
                continue

            # ── Currently paused ──────────────────────────────────────────
            if not is_active and paused_until:
                if now < paused_until:
                    # Still in cooldown window → keep blocked
                    blocked.add(key)
                    days_left = (paused_until - now).days
                    log(f"⏸️  {platform} / '{title}': paused ({days_left}d left)")
                else:
                    # Cooldown expired → re-evaluate
                    reactivate_concept(platform, key, title)
                    log(f"🔄 {platform} / '{title}': cooldown expired, re-evaluating")
                    # Will get RE_EVAL_POSTS posts before a new verdict
                continue

            # ── Active concept — check if it should be paused ─────────────
            if is_active and score < threshold and best_score > 0:
                reason = (f"avg_score={score:.1f} < threshold={threshold:.1f} "
                          f"(best={best_score:.1f}, ratio={UNDERPERFORM_RATIO})")
                pause_concept(platform, key, title, reason)
                blocked.add(key)
                continue

            # ── Re-evaluation post tracking ───────────────────────────────
            # If concept was paused before, check if re-eval is over
            if not is_active:
                # paused_until is None but still inactive means permanent stop
                blocked.add(key)

        return blocked

    except Exception as e:
        log(f"❌ Error in apply_concept_lifecycle: {e}")
        return blocked
    finally:
        conn.close()


def increment_reeval_count(platform: str, concept_key: str):
    """
    Call this after posting a re-evaluation post.
    Once reeval_posts_given >= RE_EVAL_POSTS the concept gets a final verdict
    on the NEXT analytics refresh cycle.
    """
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE concept_performance
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
    """Return the active concept with the highest avg_engagement_score."""
    conn = get_db_connection()
    if not conn:
        return None
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT concept_key, avg_engagement_score, total_posts
                FROM concept_performance
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
            for c in IMAGE_CONCEPTS:
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
    Pick the next concept to post for this platform.

    Decision flow:
      1. Run lifecycle check — pause bad ones, unblock expired cooldowns.
         Build `blocked` set of concepts that must NOT be posted right now.
      2. If ALL concepts are blocked (edge case) → log warning, pick any.
      3. If still in pure evaluation stage (no concept has MIN_POSTS yet)
         → round-robin through ALL concepts so each gets tested equally.
      4. If enough data exists:
           70 % → pick the proven #1 concept (exploitation)
           30 % → pick randomly from non-blocked concepts (exploration)
    """
    _ensure_pause_columns()

    # Step 1: run lifecycle, get blocked set
    blocked = apply_concept_lifecycle(platform)
    log(f"🔒 Blocked concepts on {platform}: {blocked if blocked else 'none'}")

    # Step 2: safety fallback — if everything is blocked, allow all
    available = [c for c in IMAGE_CONCEPTS if c["concept"] not in blocked]
    if not available:
        log(f"⚠️  ALL concepts blocked on {platform} — safety fallback to full pool")
        available = IMAGE_CONCEPTS

    # Step 3: check if still in evaluation stage
    conn = get_db_connection()
    has_enough_data = False
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM concept_performance
                    WHERE platform = %s AND total_posts >= %s
                """, (platform, MIN_POSTS_TO_EVALUATE))
                has_enough_data = cur.fetchone()[0] > 0
        except:
            pass
        finally:
            conn.close()

    if not has_enough_data:
        # Evaluation stage: rotate through ALL concepts to gather data
        # Use a per-platform index file
        index_file = f"eval_index_{platform}.txt"
        idx = 0
        if os.path.exists(index_file):
            try:
                idx = int(open(index_file).read().strip())
            except:
                idx = 0
        concept = available[idx % len(available)]
        with open(index_file, "w") as f:
            f.write(str((idx + 1) % len(available)))
        log(f"🧪 EVALUATION stage — {platform}: testing concept #{idx + 1}: {concept['title']}")
        return concept

    # Step 4: enough data — exploit best, explore occasionally
    best = get_best_concept_for_platform(platform)

    if best and random.random() < 0.70:
        log(f"🏆 EXPLOIT best concept for {platform}: {best['title']} (score leader)")
        return best

    # Exploration — exclude blocked
    non_best_available = [c for c in available if not best or c["concept"] != best["concept"]]
    pool = non_best_available if non_best_available else available
    chosen = random.choice(pool)
    log(f"🎲 EXPLORE concept for {platform}: {chosen['title']}")
    return chosen


# ──────────────────────────────────────────────────────
# Legacy round-robin (still used for single-concept flow)
# ──────────────────────────────────────────────────────
def get_next_concept():
    index = 0
    if os.path.exists(PROMPT_FILE):
        try:
            with open(PROMPT_FILE, "r") as f:
                index = int(f.read().strip())
        except:
            index = 0
    concept = IMAGE_CONCEPTS[index % len(IMAGE_CONCEPTS)]
    with open(PROMPT_FILE, "w") as f:
        f.write(str((index + 1) % len(IMAGE_CONCEPTS)))
    log(f"🎨 Using concept #{index + 1}: EssentiaScan - {concept['title']}")
    return concept

# ======================================================
# QR CODE GENERATION
# ======================================================
def generate_qr_code(tracking_url):
    try:
        log(f"🔲 Generating QR code for: {tracking_url}")
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_H,
            box_size=5,
            border=2,
        )
        qr.add_data(tracking_url)
        qr.make(fit=True)
        qr_img = qr.make_image(fill_color="black", back_color="white")
        qr_filename = f"qr_{int(time.time())}.png"
        qr_img.save(qr_filename)
        log(f"✅ QR code generated: {qr_filename}")
        return qr_filename
    except Exception as e:
        log(f"❌ QR code generation error: {e}")
        return None

def add_qr_to_image(image_path, qr_path):
    try:
        log(f"🖼️ Adding QR code to image")
        base_img = Image.open(image_path)
        qr_img   = Image.open(qr_path)
        qr_img   = qr_img.resize((150, 150), Image.Resampling.LANCZOS)
        img_width, img_height = base_img.size
        qr_position = (img_width - 150 - 20, img_height - 150 - 20)
        base_img.paste(qr_img, qr_position)
        output_filename = f"final_{int(time.time())}.jpg"
        base_img.save(output_filename, "JPEG", quality=95)
        log(f"✅ QR code added to image: {output_filename}")
        return output_filename
    except Exception as e:
        log(f"❌ Error adding QR to image: {e}")
        return None

# ======================================================
# AYRSHARE AUTO HASHTAG
# ======================================================
def generate_auto_hashtags(caption, max_hashtags=2):
    try:
        log(f"🏷️ Generating auto hashtags (max: {max_hashtags})")
        headers = {
            "Authorization": f"Bearer {AYRSHARE_API_KEY}",
            "Content-Type": "application/json"
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

# ======================================================
# TRACKING LINK
# ======================================================
def generate_tracking_link(platform, badge_type="marketing", concept_key=None):
    try:
        response = requests.post(
            f"{TRACKING_API_URL}/api/generate-tracking-url",
            json={"platform": platform, "badge_type": badge_type,
                  "username": "nonai_official", "concept_key": concept_key},
            timeout=10
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
                  "social_post_id": social_post_id},
            timeout=10
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
# HASHTAG BUILDER
# ======================================================
def build_marketing_hashtags(platform='instagram'):
    d = BRAND_HASHTAGS_ESSENTIASCAN
    if platform.lower() == 'instagram':
        return " ".join(d['core'])
    elif platform.lower() in ['tiktok', 'x', 'twitter']:
        tags = d['core'][:2] + [random.choice(d['social'])]
        return " ".join(tags)
    elif platform.lower() == 'linkedin':
        return " ".join(d['core'] + random.sample(d['business'], k=2))
    else:
        return " ".join(d['core'] + random.sample(d['secondary'], k=2))

# ======================================================
# CAPTION GENERATION
# ======================================================
try:
    from caption_generation_marketing import generate_marketing_caption as generate_base_caption
    CAPTION_GENERATOR_IMPORTED = True
    log("✅ Imported caption generator")
except ImportError:
    CAPTION_GENERATOR_IMPORTED = False
    log("⚠️ Using inline caption generation")

def generate_marketing_caption(concept, tracking_url, platform='instagram', retry_limit=5):
    base_caption = ""
    if CAPTION_GENERATOR_IMPORTED:
        try:
            base_caption = generate_base_caption(
                video_concept=concept['concept'], platform=platform, retry_limit=retry_limit
            )
            if platform.lower() == 'instagram':
                base_caption = base_caption.replace("https://nonai.life/", "").replace(tracking_url, "")
                base_caption = re.sub(r'\n{3,}', '\n\n', base_caption)
            else:
                base_caption = base_caption.replace("https://nonai.life/", tracking_url)
            log(f"✅ Generated base caption for {platform}")
        except Exception as e:
            log(f"⚠️ Error using imported caption: {e}")

    if not base_caption:
        hashtags = build_marketing_hashtags(platform)
        if platform.lower() == 'instagram':
            base_caption = (
                f"{concept['title']}\n\n"
                "In a world where AI can fake anything, proving you're human matters more than ever.\n\n"
                "EssentiaScan: Multi-factor biological verification that AI can't fake.\n\n"
                "Join 50,000+ verified humans.\n\n"
                "Scan the QR code to verify yourself! 👇\n\n"
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

    max_auto_hashtags = 2 if platform.lower() == 'instagram' else 3
    final_caption = generate_auto_hashtags(base_caption, max_hashtags=max_auto_hashtags)
    save_caption_to_history(final_caption, concept['concept'], platform)
    return final_caption

# ======================================================
# IMAGE GENERATION
# ======================================================
def generate_image(concept):
    filename = f"image_{int(time.time())}.jpg"
    try:
        log(f"🎨 Generating image with Gemini for: {concept['title']}")
        prompt = f"""
Create an Instagram-ready image for "EssentiaScan" - AI verification technology by NonAI.life.

CONCEPT: "{concept['title']}"
VISUAL DESCRIPTION: {concept['description']}
TEXT OVERLAY: "{concept['text_overlay']}"
COLOR SCHEME: {concept['color_scheme']}
STYLE: {concept['style']}

REQUIREMENTS:
- Square format: 1080x1080 pixels for Instagram
- Modern tech aesthetic with bold text overlay
- {concept['color_scheme']} color treatment
- Professional, sleek design
- Leave bottom-right corner clear (320x320px) for QR code that will be added later
- Text should be bold, clear, and impactful
- Cyber-security/tech vibes
- No URLs, logos, or brand names in the image itself
- DO NOT include any QR code - it will be added programmatically

CRITICAL: Create a powerful image that conveys trust, security, and human authenticity in the age of AI.
"""
        response = model_image.generate_content([prompt])
        if response.candidates:
            for candidate in response.candidates:
                if hasattr(candidate.content, "parts"):
                    for part in candidate.content.parts:
                        if hasattr(part, "inline_data") and part.inline_data:
                            with open(filename, "wb") as f:
                                f.write(part.inline_data.data)
                            log("✅ Gemini image generated successfully")
                            return filename
        log("⚠️ Gemini returned no image")
        return None
    except Exception as e:
        log(f"💥 Gemini image generation error: {e}")
        return None

# ======================================================
# AYRSHARE UPLOAD & POST
# ======================================================
def upload_media(path, retries=3):
    headers = {"Authorization": f"Bearer {AYRSHARE_API_KEY}"}
    for attempt in range(retries):
        try:
            log(f"📤 Upload attempt {attempt+1}")
            with open(path, "rb") as f:
                files = {"file": (os.path.basename(path), f, "image/jpeg")}
                res = requests.post(
                    "https://app.ayrshare.com/api/media/upload",
                    headers=headers, files=files, timeout=60,
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

def create_post(media_url, caption, platforms, retries=3):
    """
    Publish to Ayrshare and return:
        (success: bool,
         post_urls: dict[platform → postUrl],
         ayrshare_post_id: str,          # top-level "id" – used for analytics API
         social_post_ids: dict[platform → social-network post id])

    The top-level "id" in the Ayrshare response (e.g. "RhrbDtYh7hdSMc67zC8H") is
    what the /api/analytics/post endpoint expects.
    The per-platform "id" inside postIds[] is the social network's own post ID.
    Both are stored separately so analytics can always be fetched by ayrshare_post_id.
    """
    headers = {
        "Authorization": f"Bearer {AYRSHARE_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {"post": caption, "platforms": platforms, "mediaUrls": [media_url]}

    for attempt in range(retries):
        try:
            log(f"📲 Post attempt {attempt+1} → {platforms}")
            res = requests.post(
                "https://api.ayrshare.com/api/post",   # ← correct hostname
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

                # ── Top-level Ayrshare Post ID (used for analytics + delete) ──
                ayrshare_post_id = data.get("id")
                log(f"🆔 Ayrshare Post ID: {ayrshare_post_id}")

                post_urls       = {}   # platform → postUrl
                social_post_ids = {}   # platform → social-network post id

                post_ids_list = data.get("postIds", [])
                if isinstance(post_ids_list, list):
                    for item in post_ids_list:
                        if not isinstance(item, dict):
                            continue
                        pname          = item.get("platform")
                        post_url       = item.get("postUrl")
                        social_post_id = item.get("id")   # e.g. "17878176260289172" for IG

                        if pname:
                            if post_url:
                                post_urls[pname] = post_url
                                log(f"  ✅ {pname} postUrl: {post_url}")
                            else:
                                log(f"  ⚠️  {pname} posted but no postUrl in response")

                            if social_post_id and str(social_post_id) != "pending":
                                social_post_ids[pname] = str(social_post_id)
                                log(f"  🔖 {pname} social post ID: {social_post_id}")
                            else:
                                log(f"  ℹ️  {pname} social post ID: {social_post_id} (pending/missing)")

                for p in platforms:
                    if p not in post_urls:
                        log(f"  ⚠️  No postUrl found for {p}")

                return True, post_urls, ayrshare_post_id, social_post_ids

        except Exception as e:
            log(f"💥 Post exception: {e}")
        time.sleep(5)

    return False, {}, None, {}

# ======================================================
# PRINT PERFORMANCE SUMMARY
# ======================================================
def print_performance_summary():
    """Log a quick summary of concept performance across all platforms."""
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT platform, concept_key, concept_title,
                       total_posts, avg_engagement_score,
                       avg_likes, avg_comments, avg_shares
                FROM concept_performance
                ORDER BY platform, avg_engagement_score DESC
            """)
            rows = cur.fetchall()

        if not rows:
            log("📊 No concept performance data yet")
            return

        log("\n" + "="*60)
        log("📊 CONCEPT PERFORMANCE SUMMARY")
        log("="*60)
        current_platform = None
        for row in rows:
            if row["platform"] != current_platform:
                current_platform = row["platform"]
                log(f"\n  📱 {current_platform.upper()}")
                log(f"  {'Concept':<30} {'Posts':>5} {'Score':>8} {'Likes':>7} {'Cmts':>6} {'Shares':>7}")
                log("  " + "-"*65)
            log(
                f"  {(row['concept_title'] or row['concept_key']):<30}"
                f" {int(row['total_posts']):>5}"
                f" {row['avg_engagement_score']:>8.1f}"
                f" {row['avg_likes']:>7.1f}"
                f" {row['avg_comments']:>6.1f}"
                f" {row['avg_shares']:>7.1f}"
            )
        log("\n" + "="*60)
    except Exception as e:
        log(f"❌ Error printing performance summary: {e}")
    finally:
        conn.close()

# ======================================================
# MAIN RUN
# ======================================================
def run():
    log("🚀 NonAI/EssentiaScan Image Scheduler — Human Verification Campaign")

    # Init all tables
    init_caption_table()
    init_posted_slots_table()
    init_concept_analytics_table()
    init_concept_performance_table()

    # ── Step 1: Refresh analytics for recent posts ──────
    log("\n📡 Refreshing Ayrshare analytics for recent posts …")
    fetch_and_store_recent_analytics(hours_back=48)

    # ── Step 2: Print performance summary ───────────────
    print_performance_summary()

    # ── Step 3: Check schedule ───────────────────────────
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

    log(f"📋 Posting to {len(pending_slots)} platforms:")
    for platform, hour, minute in pending_slots:
        log(f"   • {platform.capitalize()} (slot: {hour:02d}:{minute:02d})")

    # ── Step 4: Process each platform ────────────────────
    tracking_data = {}

    for platform, target_hour, target_minute in pending_slots:
        log(f"\n{'='*60}")
        log(f"📱 Processing {platform.upper()}")
        log(f"{'='*60}")

        # Smart concept selection
        concept = get_next_concept_smart(platform)
        log(f"📝 Concept selected: {concept['title']} ({concept['concept']})")

        tracking_url, tracking_id = generate_tracking_link(platform, badge_type="marketing", concept_key=concept["concept"])

        log(f"🎨 Generating base image for {platform}")
        base_image = generate_image(concept)
        if not base_image:
            log(f"❌ Stopping {platform}: base image not generated")
            continue

        qr_code = generate_qr_code(tracking_url)
        if not qr_code:
            log(f"⚠️ QR code generation failed for {platform}, using image without QR")
            final_image = base_image
        else:
            final_image = add_qr_to_image(base_image, qr_code)
            if not final_image:
                log(f"⚠️ Failed to add QR code for {platform}, using base image")
                final_image = base_image
            else:
                if os.path.exists(qr_code):
                    os.remove(qr_code)

        media_url = upload_media(final_image)
        if not media_url:
            log(f"❌ Stopping {platform}: upload failed")
            for fp in [base_image, final_image]:
                if fp and os.path.exists(fp):
                    os.remove(fp)
            continue

        caption = generate_marketing_caption(
            concept=concept, tracking_url=tracking_url, platform=platform
        )
        log(f"📝 Caption preview: {caption[:200]}{'...' if len(caption) > 200 else ''}")

        tracking_data[platform] = {
            "concept":        concept,
            "tracking_id":    tracking_id,
            "tracking_url":   tracking_url,
            "caption":        caption,
            "media_url":      media_url,
            "target_hour":    target_hour,
            "target_minute":  target_minute,
            "base_image":     base_image,
            "final_image":    final_image,
        }

    if not tracking_data:
        log("❌ No platforms ready to post")
        return

    log("\n" + "="*60)
    log("📱 POSTING TO PLATFORMS")
    log("="*60)

    for platform in tracking_data.keys():
        log(f"\n{'─'*60}")
        log(f"📤 Posting to {platform.upper()}")
        log(f"{'─'*60}")

        td            = tracking_data[platform]
        concept       = td["concept"]
        caption       = td["caption"]
        tracking_id   = td["tracking_id"]
        tracking_url  = td["tracking_url"]
        media_url     = td["media_url"]
        target_hour   = td["target_hour"]
        target_minute = td["target_minute"]

        success, post_urls, ayrshare_post_id, social_post_ids = create_post(media_url, caption, [platform])

        if success:
            actual_post_url  = post_urls.get(platform)
            social_post_id   = social_post_ids.get(platform)

            mark_posted(
                platform, target_hour, target_minute,
                actual_post_url, tracking_id,
                ayrshare_post_id=ayrshare_post_id,
                concept_key=concept["concept"],
                social_post_id=social_post_id
            )

            if tracking_id and actual_post_url:
                confirm_tracking_post(tracking_id, actual_post_url, platform,
                                      ayrshare_post_id=ayrshare_post_id,
                                      social_post_id=social_post_id)

            # If concept is in re-evaluation, increment its counter
            increment_reeval_count(platform, concept["concept"])

            log(f"✅ {platform.capitalize()} posted")
            log(f"   📊 Tracking:          {tracking_url}")
            log(f"   🔗 Post URL:          {actual_post_url}")
            log(f"   🎨 Concept:           {concept['title']} ({concept['concept']})")
            log(f"   🆔 Ayrshare Post ID:  {ayrshare_post_id}  ← used for analytics API")
            log(f"   🔖 Social Post ID:    {social_post_id}  ← platform-native ID")
            log(f"   🕐 Slot:              {target_hour:02d}:{target_minute:02d}")
        else:
            log(f"❌ Failed to post to {platform}")

        time.sleep(2)

    # ── Step 5: Cleanup ──────────────────────────────────
    for td in tracking_data.values():
        for fp in [td.get("base_image"), td.get("final_image")]:
            if fp and os.path.exists(fp):
                os.remove(fp)
    log("🗑 Cleaned up image files")

    log("\n" + "="*60)
    log("✅ POSTING COMPLETE")
    log("="*60)


if __name__ == "__main__":
    run()