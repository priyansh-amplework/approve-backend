"""
dad_scheduled_reels_to_drive.py
Run via cron every 15 minutes.

Purpose:
- Create 8-second Diapers & Dominoes (DAD) reels using client prompts
- Saves videos to Google Drive instead of posting to social media
- Generates unique captions with auto-generated hashtags
- Follows BARIM client weekly schedule
- Avoids duplicate generation per slot using DATABASE.
- Logs all activity.
"""

import os
import sys
import time
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from google import genai
from google.genai import types
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload
from dotenv import load_dotenv
import google.generativeai as genai_caption
from pinecone import Pinecone
import psycopg2
from psycopg2.extras import RealDictCursor
import re
import random
import requests  # For Ayrshare API
from io import BytesIO

# ======================================================
# ENV
# ======================================================
load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
AYRSHARE_API_KEY = os.getenv("AYESHARE_API_KEY")
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json")
GDRIVE_FOLDER_ID = "0AKzS0OiVTERSUk9PVA"  # DAD videos folder
#GDRIVE_FOLDER_ID = "1LDHgvbmxeJpSY-AqJ-opRIfw0x90vuEF"
if not GEMINI_API_KEY or not DATABASE_URL:
    print("❌ Missing API keys")
    sys.exit(1)

if not AYRSHARE_API_KEY:
    print("⚠️ Warning: AYRSHARE_API_KEY not found - hashtag generation will be limited")
    print("   Add AYRSHARE_API_KEY to your .env file for auto hashtags")

# ======================================================
# CONFIG
# ======================================================
#TIMEZONE = ZoneInfo("America/New_York")  # Client posts in ET
TIMEZONE = ZoneInfo("Asia/Kolkata")
PROMPT_FILE = "last_dad_reel_prompt_index.txt"

# Google Drive scopes
SCOPES = [
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive'
]

# ======================================================
# REEL GENERATION SCHEDULE (EASTERN TIME)
# Per BARIM client schedule - Generate reels at specific times
# ======================================================
GENERATION_SCHEDULE = {
    "dad_reels": [
        # MONDAY
        {"days": [0], "hour": 13, "minute": 00, "theme": "Problem framing"},
        
        # TUESDAY
        {"days": [1], "hour": 12, "minute": 15, "theme": "Did you know?"},
        {"days": [1], "hour": 19, "minute": 30, "theme": "Emotional storytelling"},
        
        # WEDNESDAY
        {"days": [2], "hour": 19, "minute": 30, "theme": "Culture/human moment"},
        
        # THURSDAY
        {"days": [3], "hour": 16, "minute": 10, "theme": "Here's how we solve this"},#12:15
        
        # FRIDAY
        {"days": [4], "hour": 12, "minute": 15, "theme": "Fast, punchy, shareable"},
        {"days": [4], "hour": 19, "minute": 30, "theme": "Week recap"},
        
        # SATURDAY
        {"days": [5], "hour": 19, "minute": 30, "theme": "Informal, personal"},
    ],
}

# ======================================================
# Diapers & Dominoes (DAD) 8-sec Prompts (Client Provided)
# Adapted from 15-sec to 8-sec format
# ======================================================
DAD_PROMPTS = [
    # 1) DAD — "Before Dawn Provider"
    """Create an 8-second vertical (9:16) cinematic mini-story for "Diapers & Dominoes (DAD)". Tone: tender, emotional, mother's POV. Use realistic b-roll (no celebrity faces). Add burned-in captions (big, readable). Soft piano.

SCENES (total 8s):
0–3s: Predawn kitchen. Dad quietly packing lunch, checking a crumpled budget sheet. He glances toward the bedroom where mom and baby sleep.
3–6s: Quick cut: dad on bus, hands tired, then grocery bag with diapers placed on counter.
6–8s: Dad reads bedtime story, baby asleep on his chest, mom watching with watery eyes. End card with [DAD LOGO] + [BOOK COVER].

ON-SCREEN TEXT:
0–3s: "Before the world wakes up…"
3–6s: "He's already providing."
6–8s: "This is fatherhood. This is love."

VOICEOVER (warm female narrator, calm, heartfelt):
"While we sleep, he's working. Not for applause—just to keep our baby safe. Diapers & Dominoes… for the dads who show up."

CTA (end card): "Diapers & Dominoes (DAD) — Real fatherhood. Real love." """,
    
    # 2) DAD — "Shield in the Storm"
    """Create an 8-second vertical (9:16) cinematic video for "Diapers & Dominoes (DAD)". Tone: protective, emotional, mother's POV. Realistic b-roll. Add captions. Add subtle storm SFX + warm strings.

SCENES:
0–3s: Rainy night. Mom and child near a car, wind blowing. Dad instantly steps in front, wraps his jacket around them.
3–6s: Dad calmly guides them inside, locks the door, exhales.
6–8s: Inside: dad checks child's face, smiles gently. Mom touches dad's shoulder—gratitude. End card [DAD LOGO] + [BOOK COVER].

ON-SCREEN TEXT:
0–3s: "When danger gets loud…"
3–6s: "He becomes the shield."
6–8s: "He protects what matters."

VOICEOVER (soft female narrator):
"I need the man who steps between chaos and our child—instantly, every time. Diapers & Dominoes. The love you always feel."

CTA: "DAD — Fatherhood is protection." """,
    
    # 3) DAD — "Hospital Night Watch"
    """Create an 8-second vertical (9:16) cinematic video for "Diapers & Dominoes (DAD)". Tone: raw, heartfelt, faith-adjacent without preaching. Realistic hospital b-roll (no brand names). Captions on. Gentle ambient pads + faint monitor beeps.

SCENES:
0–3s: Hospital room. Child asleep. Dad in chair, eyes red, holding a tiny hand.
3–5s: Mom leans into dad, exhausted. Dad wraps an arm around her.
5–8s: Dad stands, wipes tears, looks determined, kisses child's forehead. End card [DAD LOGO] + [BOOK COVER].

ON-SCREEN TEXT:
0–3s: "Some nights…"
3–5s: "You just hold on."
5–8s: "And you don't quit."

VOICEOVER (female narrator, restrained emotion):
"When it's scary… he stays. When our baby needs strength… he becomes it. Diapers & Dominoes—commitment over everything."

CTA: "Diapers & Dominoes (DAD)" """,
    
    # 4) DAD — "When Dads Need Dads"
    """Create an 8-second vertical (9:16) cinematic video for "Diapers & Dominoes (DAD)". Tone: brotherhood, healing, authentic. Realistic b-roll. Captions on. Warm guitar/piano.

SCENES:
0–3s: Dad alone in car at night, head down, hands shaking.
3–6s: Cut to domino table with 3–4 men. One man puts hand on dad's shoulder. Another hands him water. Quiet nods.
6–8s: Dad exhales, tears fall, small relieved smile. Brief hug. End card [DAD LOGO] + [BOOK COVER].

ON-SCREEN TEXT:
0–3s: "Strong doesn't mean alone."
3–6s: "A father needs a circle."
6–8s: "Men holding men up."

VOICEOVER (male narrator, low and sincere):
"Real dads build a brotherhood—men who keep you standing when life hits hardest. Diapers & Dominoes… for every father who refuses to quit."

CTA: "DAD — Build the circle." """,
    
    # 5) DAD — "The Little Things She Sees"
    """Create an 8-second vertical (9:16) cinematic video for "Diapers & Dominoes (DAD)". Tone: mother's quiet appreciation. Realistic home b-roll. Captions on. Gentle piano.

SCENES:
0–3s: Dad braiding daughter's hair OR packing lunch with note: "Proud of you."
3–5s: Dad teaching child to tie shoes, patient smile, hands guiding gently.
5–8s: Mom watching from doorway, tearful smile. Dad looks up, gives tiny "we got this" nod. End card [DAD LOGO] + [BOOK COVER].

ON-SCREEN TEXT:
0–3s: "He doesn't just provide…"
3–5s: "He teaches. He nurtures."
5–8s: "And I see it."

VOICEOVER (female narrator, warm):
"The quiet moments make me cry—how gentle he is, how he shows our child what love looks like. Diapers & Dominoes… a story that hits home."

CTA: "Diapers & Dominoes (DAD)" """
]

# Video concept mapping for DAD campaign
VIDEO_CONCEPTS = [
    "before_dawn_provider",
    "shield_in_storm",
    "hospital_night_watch",
    "when_dads_need_dads",
    "little_things_she_sees"
]

# ======================================================
# GEMINI CLIENTS
# ======================================================
client = genai.Client(api_key=GEMINI_API_KEY)
genai_caption.configure(api_key=GEMINI_API_KEY)
model_caption = genai_caption.GenerativeModel("gemini-2.0-flash-exp")

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
            GOOGLE_SERVICE_ACCOUNT_FILE,
            scopes=SCOPES
        )
        service = build('drive', 'v3', credentials=credentials, cache_discovery=False)
        return service
    except FileNotFoundError:
        log(f"❌ Service account file not found: {GOOGLE_SERVICE_ACCOUNT_FILE}")
        raise Exception(f"Service account file not found: {GOOGLE_SERVICE_ACCOUNT_FILE}")
    except Exception as e:
        log(f"❌ Failed to authenticate with Google Drive: {str(e)}")
        raise Exception(f"Failed to authenticate with Google Drive: {str(e)}")


def check_drive_access(folder_id):
    """Verify access to Google Drive folder"""
    try:
        service = get_gdrive_service()
        folder = service.files().get(
            fileId=folder_id,
            fields="id, name",
            supportsAllDrives=True
        ).execute()
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
        caption: Social media caption to save alongside video
    
    Returns:
        Dict with file info (id, name, webViewLink)
    """
    try:
        service = get_gdrive_service()
        
        file_name = os.path.basename(file_path)
        
        # Determine MIME type based on file extension
        mime_type = 'video/mp4' if file_path.endswith('.mp4') else 'application/octet-stream'
        
        file_metadata = {
            'name': file_name,
            'mimeType': mime_type
        }
        
        # If folder_id is provided, set it as parent
        if folder_id:
            file_metadata['parents'] = [folder_id]
        
        media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)
        
        log(f"📤 Uploading {file_name} to Google Drive...")
        
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, webViewLink',
            supportsAllDrives=True
        ).execute()
        
        log(f"✅ Video uploaded to Google Drive!")
        log(f"   📄 File name: {file.get('name')}")
        log(f"   🔗 File ID: {file.get('id')}")
        log(f"   🌐 View link: {file.get('webViewLink')}")
        
        # Upload caption as companion text file
        caption_result = None
        if caption:
            caption_result = upload_caption_to_gdrive(
                caption=caption,
                video_name=file_name,
                folder_id=folder_id,
                service=service
            )
        
        return {
            'id': file.get('id'),
            'name': file.get('name'),
            'link': file.get('webViewLink'),
            'caption_file': caption_result
        }
        
    except Exception as e:
        log(f"❌ Error uploading to Google Drive: {e}")
        return None


def upload_caption_to_gdrive(caption: str, video_name: str, folder_id: str = None, service=None):
    """
    Upload caption as a text file to Google Drive
    
    Args:
        caption: The caption text
        video_name: Name of the video file (for naming the caption file)
        folder_id: Google Drive folder ID
        service: Existing Drive service (to avoid re-authentication)
    
    Returns:
        Dict with caption file info
    """
    try:
        if not service:
            service = get_gdrive_service()
        
        # Create caption filename
        caption_filename = f"{os.path.splitext(video_name)[0]}_CAPTION.txt"
        
        file_metadata = {
            'name': caption_filename,
            'mimeType': 'text/plain'
        }
        
        if folder_id:
            file_metadata['parents'] = [folder_id]
        
        # Create file content
        caption_content = f"""DIAPERS & DOMINOES (DAD) - SOCIAL MEDIA CAPTION
Generated: {datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S ET")}

{caption}

---
Video file: {video_name}
Campaign: Diapers & Dominoes (DAD)
"""
        
        # Upload caption file
        media = MediaIoBaseUpload(
            BytesIO(caption_content.encode('utf-8')),
            mimetype='text/plain',
            resumable=True
        )
        
        caption_file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, webViewLink',
            supportsAllDrives=True
        ).execute()
        
        log(f"✅ Caption file uploaded: {caption_file.get('name')}")
        
        return {
            'id': caption_file.get('id'),
            'name': caption_file.get('name'),
            'link': caption_file.get('webViewLink')
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
            cur.execute("""
                CREATE TABLE IF NOT EXISTS caption_history_dad (
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

def save_caption_to_history(caption, video_concept, platform="all"):
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO caption_history_dad (caption, video_concept, platform) VALUES (%s, %s, %s)",
                (caption, video_concept, platform)
            )
            conn.commit()
            return True
    finally:
        conn.close()

# ======================================================
# DUPLICATE PREVENTION - DATABASE
# ======================================================
def init_generated_slots_table():
    """Initialize the dad_generated_slots_reels table in database"""
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dad_generated_slots_reels (
                    id SERIAL PRIMARY KEY,
                    generation_date DATE NOT NULL,
                    target_hour INTEGER NOT NULL,
                    target_minute INTEGER NOT NULL,
                    generated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    gdrive_file_id TEXT,
                    gdrive_link TEXT,
                    campaign VARCHAR(50) DEFAULT 'DAD',
                    theme VARCHAR(200),
                    UNIQUE(generation_date, target_hour, target_minute)
                )
            """)
            # Create index for faster lookups
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_dad_generated_slots_reels_lookup 
                ON dad_generated_slots_reels(generation_date, target_hour, target_minute)
            """)
            conn.commit()
        log("✅ Generated slots table initialized (REELS)")
        return True
    except Exception as e:
        log(f"❌ Error initializing dad_generated_slots_reels table: {e}")
        return False
    finally:
        conn.close()

def already_generated(target_hour, target_minute):
    """Check if we already generated a video for this slot today"""
    conn = get_db_connection()
    if not conn:
        return False
    try:
        today = datetime.now(TIMEZONE).date()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM dad_generated_slots_reels 
                WHERE generation_date = %s 
                AND target_hour = %s 
                AND target_minute = %s
            """, (today, target_hour, target_minute))
            result = cur.fetchone()
            return result is not None
    except Exception as e:
        log(f"❌ Error checking generated status: {e}")
        return False
    finally:
        conn.close()

def mark_generated(target_hour, target_minute, gdrive_file_id=None, gdrive_link=None, theme=None):
    """Mark a slot as generated in the database"""
    conn = get_db_connection()
    if not conn:
        return False
    try:
        today = datetime.now(TIMEZONE).date()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO dad_generated_slots_reels 
                (generation_date, target_hour, target_minute, gdrive_file_id, gdrive_link, campaign, theme)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (generation_date, target_hour, target_minute) 
                DO UPDATE SET 
                    generated_at = NOW(),
                    gdrive_file_id = EXCLUDED.gdrive_file_id,
                    gdrive_link = EXCLUDED.gdrive_link,
                    campaign = EXCLUDED.campaign,
                    theme = EXCLUDED.theme
            """, (today, target_hour, target_minute, gdrive_file_id, gdrive_link, 'DAD', theme))
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

    for slot_config in GENERATION_SCHEDULE["dad_reels"]:
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
def get_next_prompt():
    index = 0
    if os.path.exists(PROMPT_FILE):
        try:
            with open(PROMPT_FILE, "r") as f:
                index = int(f.read().strip())
        except:
            index = 0

    prompt = DAD_PROMPTS[index % len(DAD_PROMPTS)]
    video_concept = VIDEO_CONCEPTS[index % len(VIDEO_CONCEPTS)]

    with open(PROMPT_FILE, "w") as f:
        f.write(str((index + 1) % len(DAD_PROMPTS)))

    log(f"🧠 Using DAD prompt #{index + 1}")
    log(f"📝 Video concept: {video_concept}")
    return prompt, video_concept

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
        return caption_text + "\n\n#Fatherhood #DadLife #ParentingJourney #FamilyFirst #RealDad"
    
    try:
        url = "https://app.ayrshare.com/api/hashtags/auto"
        
        headers = {
            "Authorization": f"Bearer {AYRSHARE_API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "post": caption_text,
            "max": min(max(max_hashtags, 1), 10),  # Ensure range 1-10
            "position": "end"  # Add hashtags at the end
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
def generate_dad_caption(video_concept):
    """
    Generate DAD caption with Ayrshare auto-generated hashtags
    Simple emotional copy + Ayrshare contextual hashtags
    
    Args:
        video_concept: The video concept name
    
    Returns:
        Caption text with auto hashtags
    """
    
    # Map video concepts to emotional copy
    caption_templates = {
        "before_dawn_provider": {
            "hook": "He wakes up before the world does. 🌅",
            "body": "Not for recognition. Not for applause. Just to make sure his family has what they need.\n\nThis is fatherhood—quiet, relentless, beautiful.\n\nDiapers & Dominoes tells the story every dad needs to read."
        },
        "shield_in_storm": {
            "hook": "When danger shows up, so does he. 🛡️",
            "body": "He doesn't need to be asked. He doesn't hesitate. He steps in front of what matters most.\n\nThis is protection. This is love.\n\nDiapers & Dominoes—fatherhood from a mother's eyes."
        },
        "hospital_night_watch": {
            "hook": "Some nights test everything you have. 💔",
            "body": "And he stays. When I'm breaking, he holds me. When our child needs strength, he becomes it.\n\nFatherhood doesn't come with a manual—it comes with commitment.\n\nDiapers & Dominoes."
        },
        "when_dads_need_dads": {
            "hook": "Fatherhood can break you... if you carry it alone. 🤝",
            "body": "That's why real dads build a brotherhood—men who keep you standing when life hits hardest.\n\nEvery father needs a circle.\n\nDiapers & Dominoes."
        },
        "little_things_she_sees": {
            "hook": "It's the quiet moments that break my heart. 💙",
            "body": "How gentle he is. How patient he becomes. How he shows our child what love looks like.\n\nFatherhood is in the details.\n\nDiapers & Dominoes—a story that hits home."
        }
    }
    
    template = caption_templates.get(video_concept, caption_templates["before_dawn_provider"])
    
    # Create caption without hashtags first
    caption_body = f"""{template['hook']}

{template['body']}"""
    
    # Use Ayrshare to generate and add auto hashtags
    caption_with_hashtags = generate_auto_hashtags_ayrshare(caption_body, max_hashtags=5)
    
    save_caption_to_history(caption_with_hashtags, video_concept, "all")
    return caption_with_hashtags

# ======================================================
# GEMINI VIDEO GENERATION (8 SECONDS)
# ======================================================
def generate_video(prompt, retries=3):
    filename = f"dad_video_8sec_{int(time.time())}.mp4"
    try:
        log("🎬 Requesting 8-second DAD video from Gemini")
        
        operation = None
        for attempt in range(retries):
            try:
                operation = client.models.generate_videos(
                    model="veo-3.0-fast-generate-001",
                    prompt=prompt,
                    config=types.GenerateVideosConfig(
                        aspect_ratio="9:16",
                         # 8-second videos instead of 15
                    ),
                )
                break
            except Exception as e:
                log(f"⚠️ Attempt {attempt + 1} failed: {e}")
                time.sleep(60)
        
        if not operation:
            log("❌ Failed to start generation")
            return None

        # Poll until finished
        attempt_start = time.time()
        while not operation.done:
            elapsed = int(time.time() - attempt_start)
            log(f"⏳ Generating 8-sec video... {elapsed}s elapsed (checking in 10s)")
            time.sleep(10)
            try:
                operation = client.operations.get(operation)
            except Exception as e:
                log(f"⚠️ Error checking status: {e}")
                time.sleep(5)
                continue

        if not operation.response:
            error_msg = operation.error if hasattr(operation, 'error') else "Unknown error"
            log(f"❌ No response from model: {error_msg}")
            return None

        response = operation.response

        # Handle Veo fast responses
        video_file = None

        if hasattr(response, "generated_videos") and response.generated_videos:
            video_file = response.generated_videos[0].video
        elif hasattr(response, "files") and response.files:
            video_file = response.files[0]
        else:
            log("❌ No downloadable video found")
            return None

        # Download with retry logic
        for attempt in range(3):
            try:
                log(f"📥 Download attempt {attempt + 1}/3...")
                client.files.download(file=video_file)
                video_file.save(filename)
                log(f"✅ 8-second video saved: {filename}")
                return filename
            except Exception as e:
                if attempt < 2:
                    log(f"⚠️ Download attempt {attempt + 1} failed, retrying in 10 seconds...")
                    log(f"   Error: {e}")
                    time.sleep(10)
                else:
                    log(f"❌ All download attempts failed: {e}")
                    return None

        return None

    except Exception as e:
        log(f"💥 Gemini error: {e}")
        return None

# ======================================================
# MAIN RUN
# ======================================================
def run():
    log("🚀 DAD Reel Generator - 8-Second Videos with Ayrshare Auto Hashtags")
    log("📅 BARIM Client Schedule - Eastern Time Zone")
    log("🎯 Using client-provided DAD prompts")
    log("🏷️ Hashtags powered by Ayrshare Auto Hashtags API")
    
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

    # Log what we're about to generate
    log(f"📋 Generating {len(pending_slots)} video(s):")
    for hour, minute, theme in pending_slots:
        log(f"   • Slot: {hour:02d}:{minute:02d} ET - Theme: {theme}")

    # Get next prompt and video concept
    prompt, video_concept = get_next_prompt()
    
    # Generate 8-second video
    video = generate_video(prompt)
    if not video:
        log("❌ Stopping: video not generated")
        return

    # Generate caption with auto hashtags
    caption = generate_dad_caption(video_concept)
    
    log(f"\n📝 Generated Caption with Auto Hashtags:")
    log("=" * 60)
    log(caption)
    log("=" * 60)

    # Upload to Google Drive
    log(f"\n📤 Uploading to Google Drive...")
    
    gdrive_result = upload_to_gdrive(
        file_path=video,
        folder_id=GDRIVE_FOLDER_ID,
        caption=caption
    )
    
    if gdrive_result:
        log(f"\n✅ Successfully uploaded to Google Drive!")
        log(f"   📄 Video: {gdrive_result['name']}")
        log(f"   🔗 Link: {gdrive_result['link']}")
        
        if gdrive_result.get('caption_file'):
            log(f"   📝 Caption file: {gdrive_result['caption_file']['name']}")
            log(f"   🔗 Caption link: {gdrive_result['caption_file']['link']}")
        
        # Mark slots as generated
        for hour, minute, theme in pending_slots:
            mark_generated(
                target_hour=hour,
                target_minute=minute,
                gdrive_file_id=gdrive_result['id'],
                gdrive_link=gdrive_result['link'],
                theme=theme
            )
            log(f"✅ Marked slot {hour:02d}:{minute:02d} as generated")
    else:
        log(f"\n⚠️ Failed to upload to Google Drive")
        log(f"   Video saved locally: {video}")

    # Cleanup local file
    if os.path.exists(video):
        os.remove(video)
        log("🗑 Cleaned up local video file")
    
    log("\n" + "="*60)
    log("✅ GENERATION COMPLETE - DAD Campaign")
    log("🎬 8-second video with Ayrshare auto-generated hashtags")
    log("="*60)

# ======================================================
if __name__ == "__main__":

    run()
