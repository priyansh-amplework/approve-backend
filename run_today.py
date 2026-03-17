"""
dad_scheduled_posts_to_drive.py
Run via cron every 15 minutes.

Purpose:
- Create Diapers & Dominoes (DAD) IMAGE POSTS (not videos)
- Saves images to Google Drive instead of posting to social media
- Generates unique captions and saves them with the image
- Follows BARIM client weekly schedule
- Avoids duplicate generation per slot using DATABASE.
- Logs all activity.
- Generates 3 images per scheduled slot.
"""

import os
import sys
import time
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import google.generativeai as genai_caption
from pinecone import Pinecone
import psycopg2
from psycopg2.extras import RealDictCursor
import re
import random
import requests  # For Ayrshare API
from io import BytesIO
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload
import qrcode
from PIL import Image

# ======================================================
# ENV
# ======================================================
load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
AYRSHARE_API_KEY = os.getenv("AYESHARE_API_KEY")
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv(
    "GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json"
)
### "10BBOLwI61vvOyF0Ihh7mF2sBM6219Oym"  # DAD posts folder
GDRIVE_FOLDER_ID = "0AEgAK3q0uTUwUk9PVA"
if not GEMINI_API_KEY or not DATABASE_URL:
    print("❌ Missing API keys")
    sys.exit(1)

if not AYRSHARE_API_KEY:
    print("⚠️ Warning: AYRSHARE_API_KEY not found - hashtag generation will be limited")
    print("   Add AYRSHARE_API_KEY to your .env file for auto hashtags")

# ======================================================
# CONFIG
# ======================================================
# TIMEZONE = ZoneInfo("America/New_York")  # Client posts in ET
TIMEZONE = ZoneInfo("Asia/Kolkata")
PROMPT_FILE = "last_dad_post_prompt_index.txt"

# Number of images to generate per scheduled slot
IMAGES_PER_SLOT = 10

# Amazon book link for QR code
AMAZON_BOOK_LINK = "https://www.amazon.com/dp/B0GF92XXM8"

# Google Drive scopes
SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive",
]

# ======================================================
# POST GENERATION SCHEDULE (EASTERN TIME)
# Per BARIM client schedule - Generate posts at specific times
# ======================================================
GENERATION_SCHEDULE = {
    "dad_posts": [
        # MONDAY
        # {"days": [0], "hour": 12, "minute": 15, "theme": "Problem framing"},
        {"days": [0], "hour": 11, "minute": 49, "theme": "Problem framing"},
        # TUESDAY
        {"days": [1], "hour": 12, "minute": 15, "theme": "Did you know?"},
        {"days": [1], "hour": 19, "minute": 30, "theme": "Emotional storytelling"},
        # WEDNESDAY
        {"days": [2], "hour": 19, "minute": 30, "theme": "Culture/human moment"},
        # THURSDAY
        {"days": [3], "hour": 17, "minute": 56, "theme": "Here's how we solve this"},
        # FRIDAY
        {"days": [4], "hour": 12, "minute": 15, "theme": "Fast, punchy, shareable"},
        {"days": [4], "hour": 19, "minute": 30, "theme": "Week recap"},
        # SATURDAY
        {"days": [5], "hour": 19, "minute": 30, "theme": "Informal, personal"},
    ],
}

# ======================================================
# Diapers & Dominoes (DAD) IMAGE POST Prompts
# Visual storytelling through powerful imagery
# QR code will be added to bottom-right corner (320x320px) after generation
# ======================================================
DAD_IMAGE_PROMPTS = [
    # 1) DAD — "Before Dawn Provider"
    """Create a powerful vertical (9:16) social media image post for "Diapers & Dominoes (DAD)". 
    
Style: Cinematic, emotional, photorealistic, mother's POV perspective
Mood: Tender, appreciative, warm
Color palette: Soft dawn light, warm oranges and blues
Lighting: Natural predawn window light

VISUAL COMPOSITION:
Main subject: Father in predawn kitchen, silhouetted against window light, quietly packing lunch. On counter: crumpled budget sheet, baby bottle, packed lunch bag. In background through doorway: soft glow from bedroom where mom and baby sleep.

Secondary elements: 
- Tired but determined hands
- Simple family photos on fridge
- Coffee brewing quietly
- Work boots by door ready

Text overlay (elegant, readable typography):
Top third: "Before the world wakes up…"
Bottom third: "He's already providing."

Brand elements: Subtle DAD logo bottom corner, book spine visible on counter

CRITICAL: Leave bottom-right corner clear (320x320px) for QR code that will be added later

Emotional tone: Quiet sacrifice, unwavering commitment, the invisible work of fatherhood

Photography style: Documentary realism, intimate moment, shallow depth of field, film grain texture""",
    # 2) DAD — "Shield in the Storm"
    """Create a powerful vertical (9:16) social media image post for "Diapers & Dominoes (DAD)".

Style: Dramatic, protective, cinematic realism
Mood: Strong, safe, steadfast
Color palette: Dark stormy blues, warm interior gold tones
Lighting: Dramatic contrast between storm outside and safety inside

VISUAL COMPOSITION:
Main subject: Father standing at threshold of home during rainy night, jacket draped over mom and child behind him. His posture protective, stance firm. Rain visible outside, warm light from inside home.

Secondary elements:
- Rain droplets catching light
- His protective hand on doorframe
- Mom's hand on his shoulder (gratitude)
- Child's small hand gripping his shirt
- Warm home interior glow

Text overlay (bold, strong typography):
Top third: "When danger gets loud…"
Bottom third: "He becomes the shield."

Brand elements: DAD logo bottom corner, book visible on entry table

CRITICAL: Leave bottom-right corner clear (320x320px) for QR code that will be added later

Emotional tone: Protection, unwavering presence, safety, strength tempered with gentleness

Photography style: Cinematic lighting, dramatic weather, documentary moment, high contrast""",
    # 3) DAD — "Hospital Night Watch"
    """Create a powerful vertical (9:16) social media image post for "Diapers & Dominoes (DAD)".

Style: Raw, intimate, emotional documentary
Mood: Faithful, committed, tender strength
Color palette: Hospital blues and greens, soft ambient lighting
Lighting: Dim hospital room light, monitor glow, window moonlight

VISUAL COMPOSITION:
Main subject: Father in hospital chair beside child's bed, holding tiny hand, eyes closed in prayer or exhaustion. Mom leaning on his shoulder. Both in vigil mode, utterly present.

Secondary elements:
- Faint monitor lights (no specific medical brands)
- Small toy on bedside table
- Dad's other hand supporting mom
- Crumpled tissues
- Window showing night sky

Text overlay (gentle, sincere typography):
Top third: "Some nights…"
Middle: "You just hold on."
Bottom third: "And you don't quit."

Brand elements: DAD logo subtle in corner, book on chair

CRITICAL: Leave bottom-right corner clear (320x320px) for QR code that will be added later

Emotional tone: Faith without preaching, commitment in crisis, strength through exhaustion

Photography style: Low-key lighting, documentary intimacy, emotional realism, soft focus background""",
    # 4) DAD — "When Dads Need Dads"
    """Create a powerful vertical (9:16) social media image post for "Diapers & Dominoes (DAD)".

Style: Authentic brotherhood, healing, communal strength
Mood: Supportive, genuine, masculine vulnerability
Color palette: Warm wood tones, evening light, earth tones
Lighting: Warm ambient room light, soft shadows

VISUAL COMPOSITION:
Main subject: Four diverse men (different ages, ethnicities) around simple domino table. One man with head down, shoulders heavy. Another man's hand on his shoulder. Others leaning in with quiet support. No big gestures, just presence.

Secondary elements:
- Dominos on table mid-game
- Coffee cups, water glasses
- Simple room, humble setting
- Nodding heads, listening faces
- One man's relieved exhale visible

Text overlay (strong, brotherhood typography):
Top third: "Strong doesn't mean alone."
Bottom third: "A father needs a circle."

Brand elements: DAD logo, book on table edge

CRITICAL: Leave bottom-right corner clear (320x320px) for QR code that will be added later

Emotional tone: Brotherhood, vulnerability accepted, men supporting men, healing through community

Photography style: Warm documentary, natural moment, group intimacy, honest emotion""",
    # 5) DAD — "The Little Things She Sees"
    """Create a powerful vertical (9:16) social media image post for "Diapers & Dominoes (DAD)".

Style: Tender, observational, mother's appreciative gaze
Mood: Gentle, nurturing, heartwarming
Color palette: Soft morning light, warm home tones, golden hour
Lighting: Natural window light, warm and gentle

VISUAL COMPOSITION:
Main subject: Father kneeling at child's level, carefully braiding daughter's hair OR patiently tying child's shoes. His large hands gentle, face patient and focused. Child trusting, still.

Secondary elements:
- Mom watching from doorway (visible in mirror or frame edge)
- Her tearful smile of recognition
- Lunch box with handwritten note visible
- Morning light streaming in
- Simple home setting, lived-in and loved

Text overlay (warm, appreciative typography):
Top third: "He doesn't just provide…"
Middle: "He teaches. He nurtures."
Bottom third: "And I see it."

Brand elements: DAD logo corner, book on nearby shelf

CRITICAL: Leave bottom-right corner clear (320x320px) for QR code that will be added later

Emotional tone: Quiet appreciation, gentle masculinity, teaching moments, patient love

Photography style: Natural light, documentary warmth, intimate domestic moment, shallow depth of field""",
]

# Post concept mapping for DAD campaign
POST_CONCEPTS = [
    "before_dawn_provider",
    "shield_in_storm",
    "hospital_night_watch",
    "when_dads_need_dads",
    "little_things_she_sees",
]

# ======================================================
# GEMINI CLIENTS
# ======================================================
genai_caption.configure(api_key=GEMINI_API_KEY)
model_caption = genai_caption.GenerativeModel("gemini-2.0-flash-exp")
model_image = genai_caption.GenerativeModel(
    "gemini-2.5-flash-image"
)  # For image generation

# Initialize Pinecone (if needed for caption generation)
if PINECONE_API_KEY:
    pc = Pinecone(api_key=PINECONE_API_KEY)


# ======================================================
# LOGGING
# ======================================================
def log(msg):
    now = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}")


# ======================================================
# GOOGLE DRIVE FUNCTIONS
# ======================================================


def get_gdrive_service():
    """Create and return authenticated Google Drive service using service account"""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            GOOGLE_SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        service = build("drive", "v3", credentials=credentials, cache_discovery=False)
        return service
    except FileNotFoundError:
        log(f"❌ Service account file not found: {GOOGLE_SERVICE_ACCOUNT_FILE}")
        raise Exception(
            f"Service account file not found: {GOOGLE_SERVICE_ACCOUNT_FILE}"
        )
    except Exception as e:
        log(f"❌ Failed to authenticate with Google Drive: {str(e)}")
        raise Exception(f"Failed to authenticate with Google Drive: {str(e)}")


def check_drive_access(folder_id):
    """Verify access to Google Drive folder"""
    try:
        service = get_gdrive_service()
        folder = (
            service.files()
            .get(fileId=folder_id, fields="id, name", supportsAllDrives=True)
            .execute()
        )
        log(f"✅ Google Drive access confirmed: {folder['name']}")
        return True
    except Exception as e:
        log(f"⚠️ Warning: Could not verify Google Drive access: {e}")
        return False


def upload_to_gdrive(file_path: str, folder_id: str = None, caption: str = None):
    """
    Upload a file to Google Drive using service account
    Also creates a companion text file with the caption

    Args:
        file_path: Path to the local file to upload
        folder_id: Google Drive folder ID (optional)
        caption: Social media caption to save alongside image

    Returns:
        Dict with file info (id, name, webViewLink)
    """
    try:
        service = get_gdrive_service()

        file_name = os.path.basename(file_path)

        # Determine MIME type based on file extension
        if file_path.endswith(".png"):
            mime_type = "image/png"
        elif file_path.endswith(".jpg") or file_path.endswith(".jpeg"):
            mime_type = "image/jpeg"
        else:
            mime_type = "application/octet-stream"

        file_metadata = {"name": file_name, "mimeType": mime_type}

        # If folder_id is provided, set it as parent
        if folder_id:
            file_metadata["parents"] = [folder_id]

        media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)

        log(f"📤 Uploading {file_name} to Google Drive...")

        file = (
            service.files()
            .create(
                body=file_metadata,
                media_body=media,
                fields="id, name, webViewLink",
                supportsAllDrives=True,
            )
            .execute()
        )

        log(f"✅ Image uploaded to Google Drive!")
        log(f"   📄 File name: {file.get('name')}")
        log(f"   🔗 File ID: {file.get('id')}")
        log(f"   🌐 View link: {file.get('webViewLink')}")

        # Upload caption as companion text file
        caption_result = None
        if caption:
            caption_result = upload_caption_to_gdrive(
                caption=caption,
                image_name=file_name,
                folder_id=folder_id,
                service=service,
            )

        return {
            "id": file.get("id"),
            "name": file.get("name"),
            "link": file.get("webViewLink"),
            "caption_file": caption_result,
        }

    except Exception as e:
        log(f"❌ Error uploading to Google Drive: {e}")
        return None


def upload_caption_to_gdrive(
    caption: str, image_name: str, folder_id: str = None, service=None
):
    """
    Upload caption as a text file to Google Drive

    Args:
        caption: The caption text
        image_name: Name of the image file (for naming the caption file)
        folder_id: Google Drive folder ID
        service: Existing Drive service (to avoid re-authentication)

    Returns:
        Dict with caption file info
    """
    try:
        if not service:
            service = get_gdrive_service()

        # Create caption filename
        caption_filename = f"{os.path.splitext(image_name)[0]}_CAPTION.txt"

        file_metadata = {"name": caption_filename, "mimeType": "text/plain"}

        if folder_id:
            file_metadata["parents"] = [folder_id]

        # Create file content
        caption_content = f"""DIAPERS & DOMINOES (DAD) - SOCIAL MEDIA CAPTION
Generated: {datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S ET")}

{caption}

---
Image file: {image_name}
Campaign: Diapers & Dominoes (DAD)
Post Type: Image Post
"""

        # Upload caption file
        media = MediaIoBaseUpload(
            BytesIO(caption_content.encode("utf-8")),
            mimetype="text/plain",
            resumable=True,
        )

        caption_file = (
            service.files()
            .create(
                body=file_metadata,
                media_body=media,
                fields="id, name, webViewLink",
                supportsAllDrives=True,
            )
            .execute()
        )

        log(f"✅ Caption file uploaded: {caption_file.get('name')}")

        return {
            "id": caption_file.get("id"),
            "name": caption_file.get("name"),
            "link": caption_file.get("webViewLink"),
        }

    except Exception as e:
        log(f"⚠️ Error uploading caption file: {e}")
        return None


# ======================================================
# DATABASE CONNECTION
# ======================================================
def get_db_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        log(f"❌ DB connection error: {e}")
        return None


def init_caption_table():
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            # Create table if it doesn't exist
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS caption_history_dad (
                    id SERIAL PRIMARY KEY,
                    caption TEXT NOT NULL,
                    post_concept VARCHAR(100),
                    platform VARCHAR(50) NOT NULL,
                    post_type VARCHAR(50) DEFAULT 'image',
                    timestamp TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """
            )

            # Add post_concept column if it doesn't exist (for existing tables)
            cur.execute(
                """
                DO $$ 
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'caption_history_dad' 
                        AND column_name = 'post_concept'
                    ) THEN
                        ALTER TABLE caption_history_dad 
                        ADD COLUMN post_concept VARCHAR(100);
                    END IF;
                END $$;
            """
            )

            # Add post_type column if it doesn't exist (for existing tables)
            cur.execute(
                """
                DO $$ 
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'caption_history_dad' 
                        AND column_name = 'post_type'
                    ) THEN
                        ALTER TABLE caption_history_dad 
                        ADD COLUMN post_type VARCHAR(50) DEFAULT 'image';
                    END IF;
                END $$;
            """
            )

            conn.commit()
            log("✅ Caption history table initialized/updated")
        return True
    except Exception as e:
        log(f"❌ Error initializing caption table: {e}")
        return False
    finally:
        conn.close()


def save_caption_to_history(caption, post_concept, platform="all"):
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO caption_history_dad (caption, post_concept, platform, post_type) VALUES (%s, %s, %s, %s)",
                (caption, post_concept, platform, "image"),
            )
            conn.commit()
            return True
    finally:
        conn.close()


# ======================================================
# DUPLICATE PREVENTION - DATABASE
# ======================================================
def init_generated_slots_table():
    """Initialize the generated_slots_posts table in database"""
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS generated_slots_posts (
                    id SERIAL PRIMARY KEY,
                    generation_date DATE NOT NULL,
                    target_hour INTEGER NOT NULL,
                    target_minute INTEGER NOT NULL,
                    generated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    gdrive_file_id TEXT,
                    gdrive_link TEXT,
                    campaign VARCHAR(50) DEFAULT 'DAD',
                    theme VARCHAR(200),
                    post_type VARCHAR(50) DEFAULT 'image',
                    UNIQUE(generation_date, target_hour, target_minute)
                )
            """
            )
            # Create index for faster lookups
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_generated_slots_posts_lookup 
                ON generated_slots_posts(generation_date, target_hour, target_minute)
            """
            )
            conn.commit()
        log("✅ Generated slots table initialized (POSTS)")
        return True
    except Exception as e:
        log(f"❌ Error initializing generated_slots_posts table: {e}")
        return False
    finally:
        conn.close()


def already_generated(target_hour, target_minute):
    """Check if we already generated a post for this slot today"""
    conn = get_db_connection()
    if not conn:
        return False
    try:
        today = datetime.now(TIMEZONE).date()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM generated_slots_posts 
                WHERE generation_date = %s 
                AND target_hour = %s 
                AND target_minute = %s
            """,
                (today, target_hour, target_minute),
            )
            result = cur.fetchone()
            return result is not None
    except Exception as e:
        log(f"❌ Error checking generated status: {e}")
        return False
    finally:
        conn.close()


def mark_generated(
    target_hour, target_minute, gdrive_file_id=None, gdrive_link=None, theme=None
):
    """Mark a slot as generated in the database"""
    conn = get_db_connection()
    if not conn:
        return False
    try:
        today = datetime.now(TIMEZONE).date()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO generated_slots_posts 
                (generation_date, target_hour, target_minute, gdrive_file_id, gdrive_link, campaign, theme, post_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (generation_date, target_hour, target_minute) 
                DO UPDATE SET 
                    generated_at = NOW(),
                    gdrive_file_id = EXCLUDED.gdrive_file_id,
                    gdrive_link = EXCLUDED.gdrive_link,
                    campaign = EXCLUDED.campaign,
                    theme = EXCLUDED.theme,
                    post_type = EXCLUDED.post_type
            """,
                (
                    today,
                    target_hour,
                    target_minute,
                    gdrive_file_id,
                    gdrive_link,
                    "DAD",
                    theme,
                    "image",
                ),
            )
            conn.commit()
        return True
    except Exception as e:
        log(f"❌ Error marking generated: {e}")
        return False
    finally:
        conn.close()


# ======================================================
# SCHEDULE CHECK
# ======================================================
def slots_to_generate_now():
    """
    Check which slots should generate now and return their target times.
    Returns: List of tuples (target_hour, target_minute, theme)
    """
    now = datetime.now(TIMEZONE)
    active = []

    for slot_config in GENERATION_SCHEDULE["dad_posts"]:
        if now.weekday() not in slot_config["days"]:
            continue
        target_hour = slot_config["hour"]
        target_minute = slot_config["minute"]
        theme = slot_config.get("theme", "General")
        target = target_hour * 60 + target_minute
        current = now.hour * 60 + now.minute
        if abs(current - target) <= 15:  # 15 min tolerance for cron
            active.append((target_hour, target_minute, theme))
            break  # Only one slot at a time

    return active


# ======================================================
# ROTATE PROMPTS
# ======================================================
def get_next_prompts(count=IMAGES_PER_SLOT):
    """
    Returns `count` consecutive prompts and their concepts,
    advancing the saved index by `count` positions.

    Returns:
        List of (prompt, post_concept) tuples, length == count
    """
    index = 0
    if os.path.exists(PROMPT_FILE):
        try:
            with open(PROMPT_FILE, "r") as f:
                index = int(f.read().strip())
        except:
            index = 0

    results = []
    for i in range(count):
        idx = (index + i) % len(DAD_IMAGE_PROMPTS)
        results.append((DAD_IMAGE_PROMPTS[idx], POST_CONCEPTS[idx]))
        log(f"🧠 Image {i + 1}/{count} — prompt #{idx + 1}, concept: {POST_CONCEPTS[idx]}")

    # Advance index by `count`
    new_index = (index + count) % len(DAD_IMAGE_PROMPTS)
    with open(PROMPT_FILE, "w") as f:
        f.write(str(new_index))

    return results


# ======================================================
# QR CODE GENERATION
# ======================================================
def generate_qr_code(book_url):
    """
    Generate a black and white QR code with the Amazon book link
    Returns the filename of the generated QR code
    """
    try:
        log(f"🔲 Generating QR code for: {book_url}")

        # Create QR code instance
        qr = qrcode.QRCode(
            version=1,  # Controls size (1 is smallest, auto-adjusts if needed)
            error_correction=qrcode.constants.ERROR_CORRECT_H,  # High error correction
            box_size=5,  # Size of each box in pixels
            border=2,  # Border size in boxes
        )

        # Add Amazon book URL data
        qr.add_data(book_url)
        qr.make(fit=True)

        # Create black and white image
        qr_img = qr.make_image(fill_color="black", back_color="white")

        # Save QR code
        qr_filename = f"qr_dad_{int(time.time())}.png"
        qr_img.save(qr_filename)

        log(f"✅ QR code generated: {qr_filename}")
        return qr_filename

    except Exception as e:
        log(f"❌ QR code generation error: {e}")
        return None


def add_qr_to_image(image_path, qr_path):
    """
    Add QR code to bottom-right corner of the image
    """
    try:
        log(f"🖼️ Adding QR code to image")

        # Open both images
        base_img = Image.open(image_path)
        qr_img = Image.open(qr_path)

        # Resize QR code to 300x300 (as specified in prompt)
        qr_img = qr_img.resize((150, 150), Image.Resampling.LANCZOS)

        # Get image dimensions
        img_width, img_height = base_img.size

        # Calculate position for bottom-right corner
        # Leave some padding (20 pixels from edges)
        qr_position = (img_width - 150 - 20, img_height - 150 - 20)

        # Paste QR code onto base image
        base_img.paste(qr_img, qr_position)

        # Save the combined image
        output_filename = f"final_dad_{int(time.time())}.jpg"
        base_img.save(output_filename, "JPEG", quality=95)

        log(f"✅ QR code added to image: {output_filename}")
        return output_filename

    except Exception as e:
        log(f"❌ Error adding QR to image: {e}")
        return None


# ======================================================
# AYRSHARE AUTO HASHTAG GENERATION
# ======================================================
def generate_auto_hashtags_ayrshare(caption_text, max_hashtags=5):
    """
    Generate contextual hashtags using Ayrshare's Auto Hashtags API

    Args:
        caption_text: The caption text to generate hashtags for
        max_hashtags: Maximum number of hashtags to generate (1-10)

    Returns:
        Caption text with auto-generated hashtags added
    """
    if not AYRSHARE_API_KEY:
        log("⚠️ Ayrshare API key not configured, using fallback hashtags")
        return (
            caption_text
            + "\n\n#Fatherhood #DadLife #ParentingJourney #FamilyFirst #RealDad"
        )

    try:
        url = "https://app.ayrshare.com/api/hashtags/auto"

        headers = {
            "Authorization": f"Bearer {AYRSHARE_API_KEY}",
            "Content-Type": "application/json",
        }

        payload = {
            "post": caption_text,
            "max": min(max(max_hashtags, 1), 10),  # Ensure range 1-10
            "position": "end",  # Add hashtags at the end
        }

        log(f"🔍 Requesting auto hashtags from Ayrshare (max: {payload['max']})...")

        response = requests.post(url, headers=headers, json=payload, timeout=30)

        if response.status_code == 200:
            result = response.json()

            # Ayrshare returns the post with hashtags already added
            if "post" in result:
                enhanced_caption = result["post"]
                log(f"✅ Ayrshare auto hashtags added successfully")
                return enhanced_caption
            else:
                log(f"⚠️ Unexpected Ayrshare response format: {result}")
                return caption_text + "\n\n#Fatherhood #DadLife #ParentingJourney"
        else:
            log(f"⚠️ Ayrshare API error: {response.status_code} - {response.text}")
            return caption_text + "\n\n#Fatherhood #DadLife #ParentingJourney"

    except requests.exceptions.Timeout:
        log("⚠️ Ayrshare API timeout, using fallback hashtags")
        return caption_text + "\n\n#Fatherhood #DadLife #ParentingJourney"
    except Exception as e:
        log(f"⚠️ Error calling Ayrshare API: {e}")
        return caption_text + "\n\n#Fatherhood #DadLife #ParentingJourney"


# ======================================================
# CAPTION GENERATION FOR DAD
# ======================================================
def generate_dad_caption(post_concept):
    """
    Generate DAD caption with Ayrshare auto-generated hashtags
    Simple emotional copy + Ayrshare contextual hashtags

    Args:
        post_concept: The post concept name

    Returns:
        Caption text with auto hashtags
    """

    # Map post concepts to emotional copy
    caption_templates = {
        "before_dawn_provider": {
            "hook": "He wakes up before the world does. 🌅",
            "body": "Not for recognition. Not for applause. Just to make sure his family has what they need.\n\nThis is fatherhood—quiet, relentless, beautiful.\n\nDiapers & Dominoes tells the story every dad needs to read.",
        },
        "shield_in_storm": {
            "hook": "When danger shows up, so does he. 🛡️",
            "body": "He doesn't need to be asked. He doesn't hesitate. He steps in front of what matters most.\n\nThis is protection. This is love.\n\nDiapers & Dominoes—fatherhood from a mother's eyes.",
        },
        "hospital_night_watch": {
            "hook": "Some nights test everything you have. 💔",
            "body": "And he stays. When I'm breaking, he holds me. When our child needs strength, he becomes it.\n\nFatherhood doesn't come with a manual—it comes with commitment.\n\nDiapers & Dominoes.",
        },
        "when_dads_need_dads": {
            "hook": "Fatherhood can break you... if you carry it alone. 🤝",
            "body": "That's why real dads build a brotherhood—men who keep you standing when life hits hardest.\n\nEvery father needs a circle.\n\nDiapers & Dominoes.",
        },
        "little_things_she_sees": {
            "hook": "It's the quiet moments that break my heart. 💙",
            "body": "How gentle he is. How patient he becomes. How he shows our child what love looks like.\n\nFatherhood is in the details.\n\nDiapers & Dominoes—a story that hits home.",
        },
    }

    template = caption_templates.get(
        post_concept, caption_templates["before_dawn_provider"]
    )

    # Create caption without hashtags first
    caption_body = f"""{template['hook']}

{template['body']}"""

    # Use Ayrshare to generate and add auto hashtags
    caption_with_hashtags = generate_auto_hashtags_ayrshare(
        caption_body, max_hashtags=5
    )

    save_caption_to_history(caption_with_hashtags, post_concept, "all")
    return caption_with_hashtags


# ======================================================
# GEMINI IMAGE GENERATION
# ======================================================
def generate_image(prompt):
    """
    Generate image using Gemini 2.5 Flash Image model
    Same process as nonai_scheduled_image_posts.py

    Args:
        prompt: The image generation prompt

    Returns:
        Filename of saved image or None
    """
    filename = f"dad_image_post_{int(time.time())}.jpg"

    try:
        log(f"🎨 Generating DAD image with Gemini 2.5 Flash Image")

        # Generate image with Gemini (same method as nonai_scheduled)
        response = model_image.generate_content([prompt])

        if response.candidates:
            for candidate in response.candidates:
                if hasattr(candidate.content, "parts"):
                    for part in candidate.content.parts:
                        if hasattr(part, "inline_data") and part.inline_data:
                            with open(filename, "wb") as f:
                                f.write(part.inline_data.data)
                            log(f"✅ Gemini image generated successfully: {filename}")
                            return filename

        log("⚠️ Gemini returned no image")
        return None

    except Exception as e:
        log(f"💥 Gemini image generation error: {e}")
        return None


# ======================================================
# GENERATE + UPLOAD ONE IMAGE (helper)
# ======================================================
def generate_and_upload_one(prompt, post_concept, image_number):
    """
    Full pipeline for a single image: generate → QR → upload.

    Args:
        prompt: Image generation prompt
        post_concept: Post concept key
        image_number: 1-based index for logging

    Returns:
        gdrive_result dict or None
    """
    log(f"\n{'─' * 50}")
    log(f"🖼️  Generating image {image_number} of {IMAGES_PER_SLOT} — concept: {post_concept}")
    log(f"{'─' * 50}")

    # --- Generate base image ---
    base_image = generate_image(prompt)
    if not base_image:
        log(f"❌ Image {image_number}: base image generation failed, skipping")
        return None

    # --- Generate QR code and composite ---
    qr_code = generate_qr_code(AMAZON_BOOK_LINK)
    if not qr_code:
        log(f"⚠️ Image {image_number}: QR code generation failed, uploading without QR")
        final_image = base_image
    else:
        final_image = add_qr_to_image(base_image, qr_code)
        if not final_image:
            log(f"⚠️ Image {image_number}: failed to composite QR, using base image")
            final_image = base_image
        # Clean up QR file
        if qr_code and os.path.exists(qr_code):
            os.remove(qr_code)
            log("🗑 Cleaned up QR code file")

    # --- Generate caption ---
    caption = generate_dad_caption(post_concept)
    log(f"\n📝 Caption for image {image_number}:")
    log("=" * 60)
    log(caption)
    log("=" * 60)

    # --- Upload to Drive ---
    log(f"\n📤 Uploading image {image_number} to Google Drive...")
    gdrive_result = upload_to_gdrive(
        file_path=final_image,
        folder_id=GDRIVE_FOLDER_ID,
        caption=caption,
    )

    # --- Cleanup local files ---
    for f in set([base_image, final_image]):
        if f and os.path.exists(f):
            os.remove(f)
    log("🗑 Cleaned up local image files")

    if gdrive_result:
        log(f"✅ Image {image_number} uploaded!")
        log(f"   📄 {gdrive_result['name']}  🔗 {gdrive_result['link']}")
        if gdrive_result.get("caption_file"):
            log(f"   📝 Caption: {gdrive_result['caption_file']['name']}")
    else:
        log(f"⚠️ Image {image_number}: Drive upload failed")

    return gdrive_result


# ======================================================
# MAIN RUN
# ======================================================
def run():
    log("🚀 DAD Post Generator - IMAGE POSTS with Ayrshare Auto Hashtags")
    log(f"🖼️  Generating {IMAGES_PER_SLOT} images per scheduled slot")
    log("📅 BARIM Client Schedule - Eastern Time Zone")
    log("🎯 Using client-provided DAD concepts")
    log("🏷️ Hashtags powered by Ayrshare Auto Hashtags API")
    log("🎨 Using Gemini 2.5 Flash Image model")
    log(f"🔲 QR code with Amazon book link: {AMAZON_BOOK_LINK}")

    # Initialize database tables
    init_caption_table()
    init_generated_slots_table()

    # Check Google Drive access
    if GDRIVE_FOLDER_ID:
        check_drive_access(GDRIVE_FOLDER_ID)

    # Get slots to generate now
    generation_slots = slots_to_generate_now()
    if not generation_slots:
        log("⏭ Nothing scheduled now")
        return

    # Filter out already-generated slots
    pending_slots = [
        (hour, minute, theme)
        for hour, minute, theme in generation_slots
        if not already_generated(hour, minute)
    ]

    if not pending_slots:
        log("⏭ Already generated for all slots today")
        return

    log(f"📋 Processing {len(pending_slots)} slot(s), {IMAGES_PER_SLOT} images each:")
    for hour, minute, theme in pending_slots:
        log(f"   • Slot {hour:02d}:{minute:02d} — Theme: {theme}")

    # ── Process each pending slot ──────────────────────────────────────────
    for hour, minute, theme in pending_slots:
        log(f"\n{'═' * 60}")
        log(f"⏰ SLOT {hour:02d}:{minute:02d} | Theme: {theme}")
        log(f"{'═' * 60}")

        # Get IMAGES_PER_SLOT consecutive prompts, advancing the file pointer once
        prompt_batch = get_next_prompts(IMAGES_PER_SLOT)

        slot_results = []

        for img_num, (prompt, post_concept) in enumerate(prompt_batch, start=1):
            result = generate_and_upload_one(prompt, post_concept, img_num)
            if result:
                slot_results.append(result)
            # Small pause between Gemini calls to avoid rate-limiting
            if img_num < IMAGES_PER_SLOT:
                time.sleep(3)

        # Mark slot as done — record the first successful upload's Drive ID/link
        first_ok = next((r for r in slot_results if r), None)
        mark_generated(
            target_hour=hour,
            target_minute=minute,
            gdrive_file_id=first_ok["id"] if first_ok else None,
            gdrive_link=first_ok["link"] if first_ok else None,
            theme=theme,
        )

        log(f"\n✅ Slot {hour:02d}:{minute:02d} complete — "
            f"{len(slot_results)}/{IMAGES_PER_SLOT} images uploaded")

    # ── Final summary ──────────────────────────────────────────────────────
    log("\n" + "=" * 60)
    log("✅ GENERATION COMPLETE - DAD Campaign")
    log(f"🖼️  {IMAGES_PER_SLOT} image posts generated per slot")
    log("🎨 Generated using Gemini 2.5 Flash Image model")
    log("🔲 QR code embedded with Amazon book link")
    log(f"📚 Book link: {AMAZON_BOOK_LINK}")
    log("=" * 60)


# ======================================================
if __name__ == "__main__":
    run()