"""
badge_posting_scheduler.py
Run via cron every 15 minutes.

PURPOSE:
  For each verified NonAI user:
    1. Fetch their referral code from the NonAI API
    2. Generate a personalised badge image (via badge_qr.py / Gemini)
    3. Replace any existing QR in the badge with a NEW tracking QR that links to:
           https://nonai.life/?ref={referral_code}
    4. Upload badge to Ayrshare CDN
    5. Generate a tracking URL (stored in click-tracking server with referral_code + nonai_user_id)
    6. Post to social media via Ayrshare
    7. Confirm tracking URL → clicks now counted
    8. Periodically sync lead/conversion data back from NonAI API

FULL FUNNEL CLOSED:
  Social post → User B clicks QR/link
              → goes to nonai.life/?ref=UUID   ← referral captured by NonAI
              → signs up              ← lead created in NonAI DB
              → verifies              ← conversion counted in NonAI DB
              → /api/sync-referral-leads pulls latest counts
              → /api/referral-report shows full funnel per post/user/concept/platform
"""

import os
import sys
import time
import json
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
import random
import re

# ── Import badge generation ───────────────────────────────────────────────────
try:
    from badge_qr import generate_personalized_badge, get_badge_info
    BADGE_MODULE_AVAILABLE = True
except ImportError:
    BADGE_MODULE_AVAILABLE = False
    print("⚠️ badge_qr.py not found — badge generation will be skipped")

load_dotenv()

# ======================================================
# ENV
# ======================================================
GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY")
AYRSHARE_API_KEY = os.getenv("AYESHARE_API_KEY")
#DATABASE_URL     = os.getenv("DATABASE_URL")
DATABASE_URL     = "postgresql://test_analytics_user:IDu9oj1HeWvqewu5EV2T5TJxmnsNLnHL@dpg-d6gn00lm5p6s73b66lt0-a.oregon-postgres.render.com/test_analytics"
#TRACKING_API_URL = os.getenv("TRACKING_API_URL", "http://44.193.35.107:8000")
#PUBLIC_URL       = os.getenv("PUBLIC_URL",        "http://44.193.35.107:8000")
TRACKING_API_URL = "https://analyticstrack-production.up.railway.app/"
PUBLIC_URL       = "https://analyticstrack-production.up.railway.app/"
# NonAI API
#NONAI_API_BASE = "https://api.nonai.life/api/v1"
NONAI_API_BASE = "https://api.nonai.life/api/v1"    ##staging 
NONAI_API_KEY  = os.getenv("NONAI_API_KEY_HEADER",
                            "Api-Key VSr7lXcF.VEvhSiuHvPjiJ7j2pQdQ1eYa1lKNrJda")
NONAI_HEADERS  = {"Authorization": NONAI_API_KEY}

if not AYRSHARE_API_KEY or not DATABASE_URL:
    print("❌ Missing AYESHARE_API_KEY or DATABASE_URL")
    sys.exit(1)

# ======================================================
# CONFIG
# ======================================================
TIMEZONE = ZoneInfo("America/New_York")

# How many users to process per cron run (avoid flooding)
MAX_USERS_PER_RUN = 5

# Platforms to post badges to
BADGE_PLATFORMS = ["instagram", "facebook", "linkedin", "x"]

# How often to re-post for same user (days) — avoid spamming
REPOST_COOLDOWN_DAYS = 7

# Brand hashtags for badge posts
BADGE_HASHTAGS = {
    "instagram": "#NonAI #EssentiaScan #VerifiedHuman",
    "facebook":  "#NonAI #EssentiaScan #VerifiedHuman #HumanVerification",
    "linkedin":  "#NonAI #EssentiaScan #VerifiedHuman #IdentitySecurity #DeepfakeDefense",
    "x":         "#NonAI #EssentiaScan #VerifiedHuman",
}

# ======================================================
# LOGGING
# ======================================================
def log(msg):
    now = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}")

# ======================================================
# DATABASE
# ======================================================
def get_db_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        log(f"❌ DB connection error: {e}")
        return None


def init_badge_posts_table():
    """
    badge_posts — tracks which user has been posted for, on which platform,
    when, and what tracking/referral IDs were used.
    Separate from posted_slots (image) and posted_slots_reels.
    """
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS badge_posts (
                    id                   SERIAL PRIMARY KEY,
                    nonai_user_id        INTEGER      NOT NULL,
                    username             VARCHAR(255),
                    platform             VARCHAR(50)  NOT NULL,
                    referral_code        TEXT         NOT NULL,
                    tracking_id          VARCHAR(20),
                    tracking_url         TEXT,
                    ayrshare_post_id     TEXT,
                    social_post_id       TEXT,
                    post_url             TEXT,
                    badge_template       TEXT,
                    posted_at            TIMESTAMP    DEFAULT NOW(),
                    referral_leads       INTEGER      DEFAULT 0,
                    referral_conversions INTEGER      DEFAULT 0,
                    referral_last_synced TIMESTAMP
                )
            """)
            for col, dtype in [
                ("ayrshare_post_id", "TEXT"),
                ("social_post_id",   "TEXT"),
                ("badge_template",   "TEXT"),
                ("referral_leads",        "INTEGER DEFAULT 0"),
                ("referral_conversions",  "INTEGER DEFAULT 0"),
                ("referral_last_synced",  "TIMESTAMP"),
            ]:
                cur.execute(f"""
                    ALTER TABLE badge_posts
                    ADD COLUMN IF NOT EXISTS {col} {dtype}
                """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_badge_posts_user
                ON badge_posts(nonai_user_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_badge_posts_referral
                ON badge_posts(referral_code)
            """)
            conn.commit()
        log("✅ badge_posts table ready")
        return True
    except Exception as e:
        log(f"❌ Error initialising badge_posts: {e}")
        return False
    finally:
        conn.close()


def already_posted_badge(nonai_user_id: int, platform: str) -> bool:
    """Return True if this user was posted on this platform within REPOST_COOLDOWN_DAYS."""
    conn = get_db_connection()
    if not conn:
        return False
    try:
        cutoff = datetime.now() - timedelta(days=REPOST_COOLDOWN_DAYS)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM badge_posts
                WHERE nonai_user_id = %s
                  AND platform      = %s
                  AND posted_at     >= %s
            """, (nonai_user_id, platform, cutoff))
            return cur.fetchone() is not None
    except Exception as e:
        log(f"❌ DB check error: {e}")
        return False
    finally:
        conn.close()


def save_badge_post(nonai_user_id, username, platform, referral_code,
                    tracking_id, tracking_url, ayrshare_post_id,
                    social_post_id, post_url, badge_template):
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO badge_posts
                (nonai_user_id, username, platform, referral_code,
                 tracking_id, tracking_url, ayrshare_post_id,
                 social_post_id, post_url, badge_template)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (nonai_user_id, username, platform, referral_code,
                  tracking_id, tracking_url, ayrshare_post_id,
                  social_post_id, post_url, badge_template))
            conn.commit()
    except Exception as e:
        log(f"❌ Error saving badge post: {e}")
    finally:
        conn.close()

# ======================================================
# NONAI API CALLS
# ======================================================
def fetch_verified_users(limit: int = 100) -> list:
    """
    GET /verified/users/ — returns verified NonAI users.
    Paginates automatically until we have `limit` users or no more pages.
    """
    url = f"{NONAI_API_BASE}/verified/users/"
    users = []
    try:
        while url and len(users) < limit:
            resp = requests.get(url, headers=NONAI_HEADERS, timeout=15)
            if resp.status_code != 200:
                log(f"⚠️ NonAI /verified/users/ returned {resp.status_code}: {resp.text[:200]}")
                break
            body = resp.json()
            if not body.get("success"):
                log(f"⚠️ NonAI API success=false: {body.get('message')}")
                break
            data    = body.get("data", {})
            results = data.get("results", [])
            users.extend(results)
            url = data.get("next")   # next page URL or None
        log(f"📋 Fetched {len(users)} verified users from NonAI")
        return users[:limit]
    except Exception as e:
        log(f"❌ Error fetching verified users: {e}")
        return []


def fetch_user_referral_codes(nonai_user_id: int) -> list:
    """
    GET /user-referrals — find referral codes for a specific user ID.
    Returns list of referral code UUIDs for this user, or [] if none.
    """
    try:
        url  = f"{NONAI_API_BASE}/user-referrals"
        resp = requests.get(url, headers=NONAI_HEADERS, timeout=15)
        if resp.status_code != 200:
            return []
        body = resp.json()
        if not body.get("success"):
            return []
        results = body.get("data", {}).get("results", [])
        for row in results:
            if row.get("referer_user") == nonai_user_id:
                codes = row.get("referal_code", [])
                log(f"   🔑 User {nonai_user_id} has {len(codes)} referral code(s)")
                return codes
        log(f"   ℹ️  No referral codes found for user {nonai_user_id}")
        return []
    except Exception as e:
        log(f"❌ Error fetching referral codes: {e}")
        return []


def fetch_referral_leads(referral_code: str) -> dict:
    """GET /referal-code-leads/{referral_code} — returns leads + platform for one code."""
    try:
        url  = f"{NONAI_API_BASE}/referal-code-leads/{referral_code}"
        resp = requests.get(url, headers=NONAI_HEADERS, timeout=10)
        if resp.status_code == 200:
            body = resp.json()
            if body.get("success"):
                return body.get("data", {})
        return {}
    except Exception as e:
        log(f"❌ Error fetching referral leads: {e}")
        return {}

# ======================================================
# TRACKING URL
# ======================================================
def generate_tracking_url(platform: str, username: str,
                           nonai_user_id: int, referral_code: str) -> tuple:
    """
    Call our click-tracking server to generate a short URL.
    Passes referral_code + nonai_user_id so the redirect goes to
    nonai.life/?ref={referral_code} and clicks are attributed to the user.
    Returns (tracking_url, tracking_id) or (fallback_url, None).
    """
    fallback = f"https://nonai.life/?ref={referral_code}"
    try:
        resp = requests.post(
            f"{TRACKING_API_URL}/api/generate-tracking-url",
            json={
                "platform":      platform,
                "badge_type":    "marketing",
                "username":      username,
                "concept_key":   "badge_referral",
                "nonai_user_id": nonai_user_id,
                "referral_code": referral_code,
            },
            timeout=10,
        )
        if resp.status_code == 200:
            data         = resp.json()
            tracking_url = data.get("tracking_url")
            tracking_id  = data.get("tracking_id")
            log(f"   ✅ Tracking URL: {tracking_url}")
            log(f"   🔗 Destination:  {data.get('destination_url')}")
            return tracking_url, tracking_id
        else:
            log(f"   ⚠️ Tracking API {resp.status_code} — using fallback URL")
            return fallback, None
    except Exception as e:
        log(f"   ❌ Tracking API error: {e} — using fallback URL")
        return fallback, None


def confirm_tracking_url(tracking_id: str, post_url: str, platform: str,
                          username: str, ayrshare_post_id: str = None,
                          social_post_id: str = None) -> bool:
    if not tracking_id:
        return False
    try:
        resp = requests.post(
            f"{TRACKING_API_URL}/api/confirm-post",
            json={
                "tracking_id":     tracking_id,
                "post_url":        post_url,
                "platform":        platform,
                "username":        username,
                "ayrshare_post_id": ayrshare_post_id,
                "social_post_id":  social_post_id,
            },
            timeout=10,
        )
        if resp.status_code == 200:
            log(f"   ✅ Tracking confirmed: {tracking_id}")
            return True
        else:
            log(f"   ⚠️ Confirm failed: {resp.status_code}")
            return False
    except Exception as e:
        log(f"   ❌ Confirm error: {e}")
        return False

# ======================================================
# BADGE GENERATION
# ======================================================
def generate_badge_with_referral_qr(username: str, platform: str,
                                     tracking_url: str) -> tuple:
    """
    Call badge_qr.generate_personalized_badge():
      - Selects a template for the platform
      - Asks Gemini to personalise it with the user's name
      - Removes any existing QR
      - Adds NEW QR pointing to our tracking URL (which redirects to nonai.life/?ref=UUID)
    Returns (success, output_path, template_used)
    """
    if not BADGE_MODULE_AVAILABLE:
        log("   ❌ badge_qr module not available")
        return False, None, None

    try:
        success, output_path, error = generate_personalized_badge(
            name=username,
            platform=platform,
            tracking_url=tracking_url,
            remove_existing_qr=True,
        )
        if success:
            template_used = os.path.basename(output_path)
            log(f"   ✅ Badge generated: {output_path}")
            return True, output_path, template_used
        else:
            log(f"   ❌ Badge generation failed: {error}")
            return False, None, None
    except Exception as e:
        log(f"   ❌ Badge exception: {e}")
        return False, None, None

# ======================================================
# AYRSHARE UPLOAD + POST
# ======================================================
def upload_badge_to_ayrshare(image_path: str) -> str | None:
    """Upload badge image to Ayrshare CDN. Returns CDN URL or None."""
    headers = {"Authorization": f"Bearer {AYRSHARE_API_KEY}"}
    for attempt in range(3):
        try:
            log(f"   📤 Uploading badge (attempt {attempt + 1})")
            with open(image_path, "rb") as f:
                ext      = os.path.splitext(image_path)[1].lower()
                mime     = "image/jpeg" if ext in (".jpg", ".jpeg") else "image/png"
                files    = {"file": (os.path.basename(image_path), f, mime)}
                resp     = requests.post(
                    "https://app.ayrshare.com/api/media/upload",
                    headers=headers, files=files, timeout=60,
                )
            if resp.status_code == 200:
                cdn_url = resp.json().get("url")
                if cdn_url:
                    log(f"   ✅ Badge uploaded: {cdn_url}")
                    return cdn_url
        except Exception as e:
            log(f"   ⚠️ Upload attempt {attempt + 1} failed: {e}")
        time.sleep(3)
    return None


def post_badge_to_ayrshare(cdn_url: str, caption: str,
                            platform: str) -> tuple:
    """
    Post badge to Ayrshare.
    Returns (success, post_url, ayrshare_post_id, social_post_id)
    """
    headers = {
        "Authorization": f"Bearer {AYRSHARE_API_KEY}",
        "Content-Type":  "application/json",
    }
    payload = {
        "post":      caption,
        "platforms": [platform],
        "mediaUrls": [cdn_url],
    }
    for attempt in range(3):
        try:
            log(f"   📲 Posting badge to {platform} (attempt {attempt + 1})")
            resp = requests.post(
                "https://api.ayrshare.com/api/post",
                json=payload, headers=headers, timeout=60,
            )
            log(f"   📡 Status: {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "success":
                    ayrshare_post_id = data.get("id")
                    post_url         = None
                    social_post_id   = None
                    for item in data.get("postIds", []):
                        if isinstance(item, dict) and item.get("platform") == platform:
                            post_url       = item.get("postUrl")
                            raw_id         = item.get("id")
                            social_post_id = str(raw_id) if raw_id and str(raw_id) != "pending" else None
                    log(f"   ✅ Posted! ayrshare_id={ayrshare_post_id}  post_url={post_url}")
                    return True, post_url, ayrshare_post_id, social_post_id
        except Exception as e:
            log(f"   ⚠️ Post attempt {attempt + 1} failed: {e}")
        time.sleep(5)
    return False, None, None, None

# ======================================================
# CAPTION GENERATION
# ======================================================
def build_badge_caption(username: str, platform: str,
                         tracking_url: str, referral_code: str) -> str:
    """
    Build a caption for the badge post.
    Instagram: NO clickable URLs — the QR code in the image is the CTA.
    Other platforms: include the tracking URL (which redirects to nonai.life/?ref=UUID).
    """
    hashtags = BADGE_HASHTAGS.get(platform, "#NonAI #EssentiaScan #VerifiedHuman")

    if platform.lower() == "instagram":
        caption = (
            f"🎉 {username} is officially a Verified Human on EssentiaScan!\n\n"
            "In a world full of AI-generated identities, this badge proves authentic humanity.\n\n"
            "Scan the QR code in this image to verify YOUR humanity and join the movement.\n\n"
            f"{hashtags}"
        )
    else:
        caption = (
            f"🎉 {username} is officially a Verified Human on EssentiaScan!\n\n"
            "In a world full of AI-generated identities, this badge proves authentic humanity.\n\n"
            f"Click to verify YOUR humanity: {tracking_url}\n\n"
            f"{hashtags}"
        )
    return caption

# ======================================================
# REFERRAL SYNC
# ======================================================
def sync_badge_referral_leads():
    """
    Call our own click-tracking server's /api/sync-referral-leads endpoint
    to pull fresh lead/conversion counts from NonAI API.
    """
    try:
        resp = requests.post(
            f"{TRACKING_API_URL}/api/sync-referral-leads",
            timeout=60,
        )
        if resp.status_code == 200:
            data = resp.json()
            log(f"📊 Referral sync complete: {data.get('synced_codes', 0)} codes updated")
        else:
            log(f"⚠️ Referral sync returned {resp.status_code}")
    except Exception as e:
        log(f"❌ Referral sync error: {e}")

# ======================================================
# MAIN RUN
# ======================================================
def run():
    log("🚀 Badge Posting Scheduler — Referral Integration")

    # ── Init tables ──────────────────────────────────────────────────────────
    init_badge_posts_table()

    # ── Step 1: Sync referral leads from NonAI API (runs every time) ─────────
    log("\n📡 Syncing referral lead data from NonAI API …")
    sync_badge_referral_leads()

    # ── Step 2: Fetch verified users ─────────────────────────────────────────
    log("\n👥 Fetching verified users from NonAI …")
    users = fetch_verified_users(limit=100)

    if not users:
        log("⚠️ No verified users found — nothing to post")
        return

    log(f"✅ {len(users)} verified users available")

    # ── Step 3: Select users to post for this run ────────────────────────────
    # Filter to users who haven't been posted recently on at least one platform
    candidates = []
    for user in users:
        # User object structure from NonAI API — adapt field names as needed
        # Common fields: id, username, email, first_name, last_name, etc.
        user_id  = user.get("id") or user.get("user_id") or user.get("user")
        username = (
            user.get("username")
            or user.get("first_name")
            or user.get("name")
            or f"User{user_id}"
        )
        if not user_id:
            continue

        for platform in BADGE_PLATFORMS:
            if not already_posted_badge(user_id, platform):
                candidates.append((user_id, username, platform))

    if not candidates:
        log("⏭ All users already posted recently — nothing to do")
        return

    # Shuffle so different users get picked each run
    random.shuffle(candidates)
    to_process = candidates[:MAX_USERS_PER_RUN]
    log(f"📋 Processing {len(to_process)} badge post(s) this run")

    # ── Step 4: Process each (user, platform) pair ───────────────────────────
    for nonai_user_id, username, platform in to_process:
        log(f"\n{'='*60}")
        log(f"👤 {username}  (user_id={nonai_user_id})  →  {platform.upper()}")
        log(f"{'='*60}")

        # Fetch referral codes for this user
        referral_codes = fetch_user_referral_codes(nonai_user_id)
        if not referral_codes:
            log(f"   ⚠️ No referral codes found — skipping")
            continue

        # Use first referral code (or pick the one matching platform if available)
        referral_code = referral_codes[0]

        # Check which platform the code was generated for
        ref_data = fetch_referral_leads(referral_code)
        ref_platform = ref_data.get("platform", "").lower()
        if ref_platform and ref_platform != platform:
            # Try to find a code matching this platform
            for code in referral_codes:
                d = fetch_referral_leads(code)
                if d.get("platform", "").lower() == platform:
                    referral_code = code
                    ref_data      = d
                    log(f"   🎯 Matched referral code for {platform}: {code[:8]}…")
                    break

        log(f"   🔑 Referral code: {referral_code[:8]}…  (existing leads: {ref_data.get('total_leads', 0)})")

        # Generate tracking URL (embeds referral_code in redirect)
        tracking_url, tracking_id = generate_tracking_url(
            platform=platform,
            username=username,
            nonai_user_id=nonai_user_id,
            referral_code=referral_code,
        )

        # Generate personalised badge with referral QR
        badge_ok, badge_path, template_used = generate_badge_with_referral_qr(
            username=username,
            platform=platform,
            tracking_url=tracking_url,
        )
        if not badge_ok or not badge_path:
            log(f"   ❌ Badge generation failed — skipping {username} on {platform}")
            continue

        # Upload badge to Ayrshare CDN
        cdn_url = upload_badge_to_ayrshare(badge_path)
        if not cdn_url:
            log(f"   ❌ Upload failed — skipping")
            # Clean up
            try: os.remove(badge_path)
            except: pass
            continue

        # Build caption
        caption = build_badge_caption(username, platform, tracking_url, referral_code)
        log(f"   📝 Caption ({platform}): {caption[:100]}…")

        # Post to Ayrshare
        post_ok, post_url, ayrshare_post_id, social_post_id = post_badge_to_ayrshare(
            cdn_url, caption, platform
        )

        if post_ok:
            # Confirm tracking URL (activates click counting)
            if tracking_id and post_url:
                confirm_tracking_url(
                    tracking_id=tracking_id,
                    post_url=post_url,
                    platform=platform,
                    username=username,
                    ayrshare_post_id=ayrshare_post_id,
                    social_post_id=social_post_id,
                )

            # Record in badge_posts table
            save_badge_post(
                nonai_user_id=nonai_user_id,
                username=username,
                platform=platform,
                referral_code=referral_code,
                tracking_id=tracking_id,
                tracking_url=tracking_url,
                ayrshare_post_id=ayrshare_post_id,
                social_post_id=social_post_id,
                post_url=post_url,
                badge_template=template_used,
            )

            log(f"   ✅ BADGE POST COMPLETE")
            log(f"      🆔 Ayrshare Post ID:  {ayrshare_post_id}")
            log(f"      🔖 Social Post ID:    {social_post_id}")
            log(f"      📊 Tracking URL:      {tracking_url}")
            log(f"      🔗 Referral Code:     {referral_code}")
            log(f"      🌐 Redirect to:       https://nonai.life/?ref={referral_code}")
            log(f"      🎯 Post URL:          {post_url}")
        else:
            log(f"   ❌ Ayrshare post failed for {username} on {platform}")

        # Clean up badge file
        try:
            if badge_path and os.path.exists(badge_path):
                os.remove(badge_path)
        except Exception:
            pass

        time.sleep(2)   # gentle rate limiting between posts

    log("\n" + "="*60)
    log("✅ BADGE POSTING RUN COMPLETE")
    log("="*60)


if __name__ == "__main__":
    run()