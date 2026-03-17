"""
post_with_qr_tracking.py - Social Media Posting with QR-Enhanced Badge Generation
ENHANCED VERSION: Replaces template QR codes with tracking URLs
Integrates QR replacement techniques from Diapers and Dominoes
"""

import requests
import json
import time
import re
import hashlib
from datetime import datetime
from caption_generater_db import generate_unique_caption, save_caption_to_history
from badge_qr import (
    generate_personalized_badge, 
    get_badge_info, 
    list_available_templates
)
from post_google_sheets_updater import *
from dotenv import load_dotenv
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build

load_dotenv()

# Ayrshare API Configuration
AYRSHARE_POST_URL = "https://app.ayrshare.com/api/post"
AYRSHARE_UPLOAD_URL = "https://app.ayrshare.com/api/media/upload"
API_KEY = os.getenv("AYESHARE_API_KEY")

# Click Tracking Server Configuration
#TRACKING_SERVER = "https://hustle-maestro-railway.onrender.com"
TRACKING_SERVER = "http://44.193.35.107:8000"
TRACKING_ENABLED = os.getenv("ENABLE_TRACKING", "true").lower() == "true"

# Google Sheets Configuration
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_SHEET_RANGE = os.getenv("GOOGLE_SHEET_RANGE", "Sheet1!A:E")
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json")
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# Badge Generation Configuration
OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER", "generated_badges")
os.makedirs(OUTPUT_FOLDER, exist_ok=True)


def get_google_sheets_service():
    """Create and return Google Sheets API service with service account credentials."""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            GOOGLE_SERVICE_ACCOUNT_FILE, 
            scopes=SCOPES
        )
        service = build('sheets', 'v4', credentials=credentials, cache_discovery=False)
        return service
    except FileNotFoundError:
        raise Exception(f"Service account file not found: {GOOGLE_SERVICE_ACCOUNT_FILE}")
    except Exception as e:
        raise Exception(f"Failed to authenticate with Google Sheets API: {str(e)}")


def upload_image_to_ayrshare(image_path):
    """Upload image to Ayrshare and get the media URL."""
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    
    try:
        print(f"   📤 Uploading image to Ayrshare...")
        print(f"   📁 File: {os.path.basename(image_path)}")
        print(f"   📊 Size: {os.path.getsize(image_path) / 1024:.2f} KB")
        
        filename = os.path.basename(image_path)
        
        with open(image_path, 'rb') as f:
            files = {
                'file': (filename, f, 'image/png')
            }
            
            data = {
                'fileName': filename,
                'description': 'QR-enhanced badge with tracking for social media posting'
            }
            
            response = requests.post(
                AYRSHARE_UPLOAD_URL,
                headers=headers,
                files=files,
                data=data,
                timeout=30
            )
        
        print(f"   📡 Response Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            
            if result.get('status') == 'success' or result.get('url'):
                media_url = result.get('url')
                file_id = result.get('id')
                
                print(f"   ✅ Image uploaded successfully")
                print(f"   🆔 File ID: {file_id}")
                print(f"   🔗 Media URL: {media_url[:80]}...")
                
                return media_url
            else:
                error_msg = result.get('message', result.get('error', 'Unknown error'))
                print(f"   ❌ Upload failed: {error_msg}")
                return None
        
        elif response.status_code == 403:
            print(f"   ❌ Upload failed: 403 Forbidden")
            print(f"   💡 Make sure you're on Ayrshare Premium plan")
            return None
        
        else:
            print(f"   ❌ Upload failed with status {response.status_code}")
            try:
                print(f"   Response: {response.json()}")
            except:
                print(f"   Response: {response.text[:200]}")
            return None
            
    except Exception as e:
        print(f"   ❌ Upload error: {e}")
        return None


def generate_tracking_url(username, badge_type, platform):
    """Generate tracking URL - Creates PENDING post."""
    if not TRACKING_ENABLED:
        return {
            "tracking_url": "https://nonai.life/",
            "tracking_id": "disabled_" + hashlib.md5(f"{datetime.now().timestamp()}".encode()).hexdigest()[:8],
            "is_public": True
        }
    
    try:
        print(f"   📡 Requesting tracking URL from Railway server...")
        response = requests.post(
            f"{TRACKING_SERVER}/api/generate-tracking-url",
            json={
                "username": username,
                "badge_type": badge_type,
                "platform": platform
            },
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            tracking_url = data.get("tracking_url", "")
            
            if 'localhost' in tracking_url or '127.0.0.1' in tracking_url:
                print(f"   ⚠️ WARNING: Got localhost URL, replacing with Railway domain")
                tracking_url = tracking_url.replace("http://localhost:5000", TRACKING_SERVER)
                tracking_url = tracking_url.replace("http://localhost:8080", TRACKING_SERVER)
                tracking_url = tracking_url.replace("http://127.0.0.1:5000", TRACKING_SERVER)
                tracking_url = tracking_url.replace("http://localhost:10000", TRACKING_SERVER)
                tracking_url = tracking_url.replace("http://localhost:8000", TRACKING_SERVER)
                data["tracking_url"] = tracking_url
            
            print(f"   ✅ Generated tracking URL (PENDING confirmation)")
            print(f"   📝 ID: {data.get('tracking_id', 'N/A')}")
            print(f"   🔗 URL: {tracking_url[:80]}...")
            
            return data
            
    except Exception as e:
        print(f"   ⚠️ Tracking error: {e}")
    
    print(f"   ⚠️ Using fallback (direct nonai.life link)")
    return {
        "tracking_url": "https://nonai.life/",
        "tracking_id": "fallback_" + hashlib.md5(f"{datetime.now().timestamp()}".encode()).hexdigest()[:8],
        "is_public": True
    }


def confirm_post_tracking(tracking_id, username, post_url, platform):
    """Confirm that a post was successfully published."""
    if not TRACKING_ENABLED:
        print(f"   ℹ️ Tracking disabled, skipping confirmation")
        return False
    
    if not tracking_id or tracking_id.startswith(('disabled_', 'fallback_', 'error_')):
        print(f"   ℹ️ Invalid tracking ID, skipping confirmation: {tracking_id}")
        return False
    
    try:
        print(f"   📊 Confirming post tracking...")
        response = requests.post(
            f"{TRACKING_SERVER}/api/confirm-post",
            json={
                "tracking_id": tracking_id,
                "post_url": post_url,
                "platform": platform,
                "username": username
            },
            timeout=10
        )
        
        if response.status_code == 200:
            print(f"   ✅ Tracking confirmed and activated!")
            return True
        else:
            print(f"   ⚠️ Failed to confirm tracking: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"   ⚠️ Error confirming tracking: {e}")
        return False


def load_badge_data():
    """Reads badge data from private Google Sheets using service account."""
    if not GOOGLE_SHEET_ID:
        raise ValueError("GOOGLE_SHEET_ID not set in environment variables")
    
    print(f"📥 Fetching badge data from private Google Sheets...")
    print(f"   Sheet ID: {GOOGLE_SHEET_ID}")
    print(f"   Range: {GOOGLE_SHEET_RANGE}")
    
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            print(f"   Attempt {attempt + 1}/{max_retries}...")
            
            service = get_google_sheets_service()
            sheet = service.spreadsheets()
            
            import socket
            socket.setdefaulttimeout(30)
            
            result = sheet.values().get(
                spreadsheetId=GOOGLE_SHEET_ID,
                range=GOOGLE_SHEET_RANGE
            ).execute()
            
            values = result.get('values', [])
            
            if not values:
                raise ValueError("No data found in Google Sheet")
            
            if len(values) < 2:
                raise ValueError("Google Sheet has no data rows (only header)")
            
            header = [col.strip().lower() for col in values[0]]
            print(f"   Found columns: {', '.join(header)}")
            
            badges = []
            for i, row in enumerate(values[1:], start=2):
                if not row or all(not cell.strip() for cell in row):
                    continue
                
                while len(row) < len(header):
                    row.append('')
                
                row_data = dict(zip(header, row))
                
                username = row_data.get('username', '').strip()
                name = row_data.get('name', '').strip()
                badge_type = row_data.get('badge_type', '').strip().lower()
                platforms_str = row_data.get('platforms', '').strip()
                posted_status = row_data.get('posted', '').strip()
                
                if not name and not username:
                    print(f"   ⚠️ Row {i}: Missing both username and name, skipping")
                    continue
                
                if not name:
                    name = username
                if not username:
                    username = name
                
                if not badge_type:
                    badge_type = 'gold'
                
                if platforms_str:
                    platforms = [p.strip() for p in platforms_str.split(',') if p.strip()]
                else:
                    platforms = ['facebook', 'linkedin']
                
                badge = {
                    'username': username,
                    'name': name,
                    'badge_type': badge_type,
                    'platforms': platforms,
                    'posted': posted_status,
                    'row_number': i
                }
                
                badges.append(badge)
                print(f"   ✅ Row {i}: @{username} → Badge for '{name}' ({badge_type}) [Posted: {posted_status or 'NO'}]")
            
            print(f"\n   ✅ Loaded {len(badges)} badges from Google Sheets\n")
            return badges
            
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"   ⚠️ Error: {e}")
                print(f"   Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                raise Exception(f"Error fetching Google Sheet data: {e}")


def add_user_mention(caption, username, platform):
    """Adds user mention to the caption."""
    if not username:
        return caption
    
    mention = username.strip()
    if not mention.startswith('@'):
        mention = f"@{mention}"
    
    congratulations_phrases = [
        f"Congratulations {mention}! 🎉",
        f"Kudos to {mention}! 👏",
        f"Well done, {mention}! 🌟",
        f"Celebrating {mention}! 🎊",
        f"Shoutout to {mention}! 🔥"
    ]
    
    import random
    prefix = random.choice(congratulations_phrases)
    return f"{prefix}\n\n{caption}"


def format_caption_with_tracking(caption, tracking_url, username, platform):
    """Format caption with tracking URL and user mention."""
    caption = add_user_mention(caption, username, platform)
    caption = re.sub(r'https?://\S+', '', caption)
    caption = re.sub(r'[👉➡️⬇️⏬💫✨🔗]+', '', caption)
    caption = re.sub(r'\s+', ' ', caption).strip()
    
    formatted_caption = f"{caption}\n\n{tracking_url}"
    formatted_caption = re.sub(r'\n{3,}', '\n\n', formatted_caption)
    
    return formatted_caption.strip()


def extract_post_data_from_response(result):
    """Extract all useful data from Ayrshare response."""
    post_data = {
        'success': False,
        'primary_url': None,
        'primary_id': None,
        'primary_platform': None,
        'all_posts': [],
        'ayrshare_id': result.get('id'),
        'ayrshare_refId': result.get('refId'),
        'platform_ids': {}
    }
    
    if result.get('status') != 'success':
        error = result.get('errors', result.get('error', 'Unknown error'))
        print(f"   ❌ Ayrshare error: {error}")
        return post_data
    
    if 'postIds' in result and isinstance(result['postIds'], list):
        print(f"   📦 Found {len(result['postIds'])} platform post(s)")
        
        for post_item in result['postIds']:
            platform = post_item.get('platform', 'unknown')
            post_url = post_item.get('postUrl')
            post_id = post_item.get('id')
            post_status = post_item.get('status')
            
            post_info = {
                'platform': platform,
                'url': post_url,
                'id': post_id,
                'status': post_status
            }
            post_data['all_posts'].append(post_info)
            
            if post_id:
                post_data['platform_ids'][platform] = post_id
            
            if post_status == 'success' and not post_data['primary_url']:
                post_data['success'] = True
                post_data['primary_url'] = post_url
                post_data['primary_id'] = post_id
                post_data['primary_platform'] = platform
                print(f"   ✅ {platform.upper()}: {post_url}")
            elif post_status == 'success':
                print(f"   ✅ {platform.upper()}: {post_url}")
            else:
                error_msg = post_item.get('message', 'Unknown error')
                print(f"   ❌ {platform.upper()}: {error_msg}")
    
    if not post_data['primary_url'] and 'postUrl' in result:
        post_data['success'] = True
        post_data['primary_url'] = result['postUrl']
        print(f"   📍 Got postUrl from root: {result['postUrl']}")
    
    return post_data


def post_to_social(caption, badge_url, platforms, username, badge_type, max_retries=3):
    """Posts to social media with generated badge image."""
    primary_platform = platforms[0] if platforms else "linkedin"
    
    print(f"   🔗 Generating tracking URL (PENDING)...")
    tracking_data = generate_tracking_url(username, badge_type, primary_platform)
    
    tracking_url = tracking_data.get('tracking_url', 'https://nonai.life/')
    tracking_id = tracking_data.get('tracking_id', '')
    
    print(f"   📝 Tracking ID: {tracking_id} (pending confirmation)")
    
    formatted_caption = format_caption_with_tracking(caption, tracking_url, username, primary_platform)
    
    if 'twitter' in platforms:
        if len(formatted_caption) > 280:
            print(f"   ⚠️ WARNING: Caption too long for Twitter ({len(formatted_caption)} chars)")
            max_caption_length = 280 - len(tracking_url) - 10
            truncated = formatted_caption[:max_caption_length].rsplit(' ', 1)[0]
            formatted_caption = f"{truncated}...\n\n{tracking_url}"
            print(f"   ✂️ Truncated to {len(formatted_caption)} characters")
    
    payload = {
        "post": formatted_caption,
        "mediaUrls": [badge_url],
        "platforms": platforms
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_KEY}"
    }

    print(f"   📋 Posting to {', '.join(platforms)}...")
    
    for attempt in range(max_retries):
        try:
            timeout = 60 if len(platforms) > 1 else 45
            
            if attempt > 0:
                print(f"   🔄 Retry attempt {attempt + 1}/{max_retries}")
            
            response = requests.post(
                AYRSHARE_POST_URL, 
                json=payload, 
                headers=headers, 
                timeout=timeout
            )
            
            print(f"   📤 API Response Status: {response.status_code}")
            
            result = response.json()
            post_data = extract_post_data_from_response(result)
            
            if not post_data['success']:
                error_msg = result.get('errors', result.get('error', 'Unknown error'))
                if isinstance(error_msg, list) and len(error_msg) > 0:
                    error_msg = error_msg[0]
                
                return {
                    "error": error_msg,
                    "status": "failed",
                    "tracking_id": tracking_id,
                    "tracking_url": tracking_url
                }
            
            print(f"   ✅ Posting successful!")
            
            if tracking_id and TRACKING_ENABLED and not tracking_id.startswith(('disabled_', 'fallback_')):
                primary_url = post_data['primary_url'] or tracking_url
                confirm_post_tracking(tracking_id, username, primary_url, primary_platform)
            
            return {
                'status': 'success',
                'tracking_id': tracking_id,
                'tracking_url': tracking_url,
                'post_url': post_data['primary_url'] or "N/A",
                'post_id': post_data['primary_id'],
                'primary_platform': post_data['primary_platform'],
                'all_posts': post_data['all_posts'],
                'retry_count': attempt
            }
            
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                wait_time = 5 * (2 ** attempt)
                print(f"   ⏳ Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
            else:
                return {
                    "error": f"Request timeout after {max_retries} attempts", 
                    "status": "failed",
                    "tracking_id": tracking_id,
                    "tracking_url": tracking_url
                }
        
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 5 * (2 ** attempt)
                time.sleep(wait_time)
            else:
                return {
                    "error": str(e), 
                    "status": "failed",
                    "tracking_id": tracking_id,
                    "tracking_url": tracking_url
                }


"""def process_badges():
   
    Process badges with QR code replacement for tracking.
    ENHANCED: Replaces template QR codes with tracking URLs
 
    print("\n" + "="*70)
    print("🚀 SOCIAL MEDIA POSTING WITH QR-ENHANCED BADGES")
    print("="*70)
    
    badge_info = get_badge_info()
    templates_by_platform = badge_info.get('templates_by_platform', {})
    
    print(f"   Data Source: Private Google Sheets")
    print(f"   Badge Generation: {'✅ ENABLED' if badge_info['gemini_api_configured'] else '❌ DISABLED'}")
    print(f"   QR Replacement: ✅ ENABLED")
    print(f"   Total Templates: {badge_info['total_templates']}")
    print(f"   Click Tracking: {'ENABLED ✅' if TRACKING_ENABLED else 'DISABLED ⚠️'}")
    
    if templates_by_platform:
        print("\n   📁 Available Templates:")
        for platform, templates in templates_by_platform.items():
            print(f"      {platform.upper()}: {len(templates)} template(s)")
    
    print("="*70 + "\n")
    
    if not badge_info['gemini_api_configured']:
        print("❌ GEMINI_API_KEY not configured")
        return
    
    if badge_info['total_templates'] == 0:
        print("❌ No templates found in badges/ folder")
        print("   Expected naming: facebook_1.png, facebook_2.png, linkedin_1.png, etc.")
        return
    
    try:
        users = load_badge_data()
    except Exception as e:
        print(f"❌ Failed to load badge data: {e}")
        return
    
    if not users:
        print("❌ No badge data found in Google Sheets")
        return
    
    total_posts = len(users)
    successful_posts = 0
    failed_posts = 0

    for idx, user in enumerate(users, 1):
        print(f"\n{'='*70}")
        print(f"📊 Processing Post {idx}/{total_posts}")
        print(f"{'='*70}")
        
        badge_type = user.get("badge_type", "gold")
        username = user.get("username", "User")
        name = user.get("name", username)
        platforms = user.get("platforms", ["facebook", "linkedin"])
        primary_platform = platforms[0] if platforms else "facebook"

        print(f"   👤 Username (for @mention): {username}")
        print(f"   📛 Display Name (for badge): {name}")
        print(f"   🏆 Badge Type: {badge_type.upper()}")
        print(f"   📱 Platforms: {', '.join(platforms)}")
        print()

        # ===== STEP 1: Generate tracking URL =====
        print(f"🔗 Step 1: Generating tracking URL...")
        tracking_data = generate_tracking_url(username, badge_type, primary_platform)
        tracking_url = tracking_data.get('tracking_url', 'https://nonai.life/')
        tracking_id = tracking_data.get('tracking_id', '')
        
        # ===== STEP 2: Generate badge with QR code replacement =====
        print(f"\n🎨 Step 2: Generating QR-enhanced badge for '{name}'...")
        print(f"   🎯 Target Platform: {primary_platform.upper()}")
        print(f"   📱 QR Code URL: {tracking_url[:60]}...")
        
        success, output_path, error = generate_personalized_badge(
            name=name,
            platform=primary_platform,
            tracking_url=tracking_url,
            qr_position="bottom-right",
            remove_existing_qr=True
        )
        
        if not success:
            print(f"   ❌ Badge generation failed: {error}")
            print("   Skipping this post\n")
            failed_posts += 1
            continue
        
        print(f"   ✅ QR-enhanced badge created: {os.path.basename(output_path)}")
        
        # ===== STEP 3: Upload to Ayrshare =====
        print("\n📤 Step 3: Uploading badge to Ayrshare...")
        badge_url = upload_image_to_ayrshare(output_path)
        
        if not badge_url:
            print("   ❌ Upload failed, skipping this post\n")
            failed_posts += 1
            continue

        # ===== STEP 4: Generate caption =====
        print(f"\n🤖 Step 4: Generating AI caption for {primary_platform}...")
        
        try:
            caption = generate_unique_caption(
                badge_type=badge_type,
                platform=primary_platform,
                username=username
            )
            
            print(f"   📝 Caption preview: {caption[:100]}...")
        
        except Exception as e:
            print(f"   ❌ Caption generation failed: {e}")
            failed_posts += 1
            continue

        # ===== STEP 5: Post to social media =====
        print(f"\n📤 Step 5: Posting to social media...")
        result = post_to_social(caption, badge_url, platforms, username, badge_type)

        # Display results
        if "error" in result or result.get("status") == "failed":
            print(f"\n❌ Posting failed!")
            if "error" in result:
                print(f"   Error: {str(result['error'])[:200]}")
            failed_posts += 1
        else:
            print(f"\n✅ Successfully posted!")
            print(f"   📊 Tracking ID: {tracking_id}")
            print(f"   🔗 Tracking URL embedded in QR code")
            successful_posts += 1
            
            for platform in platforms:
                save_caption_to_history(caption, platform)
        
        print(f"{'='*70}\n")
        
        if idx < total_posts:
            wait_time = 8
            print(f"⏳ Waiting {wait_time} seconds before next post...")
            time.sleep(wait_time)
    
    # Final summary
    print("\n" + "="*70)
    print("📊 POSTING SUMMARY")
    print("="*70)
    print(f"   Total Posts Attempted: {total_posts}")
    print(f"   ✅ Successful: {successful_posts}")
    print(f"   ❌ Failed: {failed_posts}")
    print(f"   🎯 Success Rate: {(successful_posts/total_posts*100):.1f}%")
    print("="*70 + "\n")
"""
def process_badges():
    """
    Process badges with QR code replacement for tracking.
    ENHANCED: Replaces template QR codes with tracking URLs and updates Google Sheets.
    """
    # Import the updater inside the function or at the top of the file
 
    print("\n" + "="*70)
    print("🚀 SOCIAL MEDIA POSTING WITH QR-ENHANCED BADGES")
    print("="*70)
    
    badge_info = get_badge_info()
    templates_by_platform = badge_info.get('templates_by_platform', {})
    
    print(f"   Data Source: Private Google Sheets")
    print(f"   Badge Generation: {'✅ ENABLED' if badge_info['gemini_api_configured'] else '❌ DISABLED'}")
    print(f"   QR Replacement: ✅ ENABLED")
    print(f"   Total Templates: {badge_info['total_templates']}")
    print(f"   Click Tracking: {'ENABLED ✅' if TRACKING_ENABLED else 'DISABLED ⚠️'}")
    
    if templates_by_platform:
        print("\n   📁 Available Templates:")
        for platform, templates in templates_by_platform.items():
            print(f"      {platform.upper()}: {len(templates)} template(s)")
    
    print("="*70 + "\n")
    
    if not badge_info['gemini_api_configured']:
        print("❌ GEMINI_API_KEY not configured")
        return
    
    if badge_info['total_templates'] == 0:
        print("❌ No templates found in badges/ folder")
        print("   Expected naming: facebook_1.png, facebook_2.png, linkedin_1.png, etc.")
        return
    
    try:
        users = load_badge_data()
    except Exception as e:
        print(f"❌ Failed to load badge data: {e}")
        return
    
    if not users:
        print("❌ No badge data found in Google Sheets")
        return
    
    total_posts = len(users)
    successful_posts = 0
    failed_posts = 0
    skipped_posts = 0

    for idx, user in enumerate(users, 1):
        # 1. CHECK IF ALREADY POSTED
        if user.get('posted') in ['✅', 'YES', 'TRUE', 'POSTED']:
            print(f"⏭️ Skipping Row {user['row_number']}: Already marked as posted ({user['username']})")
            skipped_posts += 1
            continue

        print(f"\n{'='*70}")
        print(f"📊 Processing Post {idx}/{total_posts}")
        print(f"{'='*70}")
        
        row_num = user.get('row_number')
        badge_type = user.get("badge_type", "gold")
        username = user.get("username", "User")
        name = user.get("name", username)
        platforms = user.get("platforms", ["facebook", "linkedin"])
        primary_platform = platforms[0] if platforms else "facebook"

        print(f"   👤 Username: {username} | Row: {row_num}")
        print(f"   📛 Display Name: {name}")
        print(f"   🏆 Badge Type: {badge_type.upper()}")
        print(f"   📱 Platforms: {', '.join(platforms)}")
        print()

        # ===== STEP 1: Generate tracking URL =====
        print(f"🔗 Step 1: Generating tracking URL...")
        tracking_data = generate_tracking_url(username, badge_type, primary_platform)
        tracking_url = tracking_data.get('tracking_url', 'https://nonai.life/')
        tracking_id = tracking_data.get('tracking_id', '')
        
        # ===== STEP 2: Generate badge with QR code replacement =====
        print(f"\n🎨 Step 2: Generating QR-enhanced badge for '{name}'...")
        success, output_path, error = generate_personalized_badge(
            name=name,
            platform=primary_platform,
            tracking_url=tracking_url,
            qr_position="bottom-right",
            remove_existing_qr=True
        )
        
        if not success:
            print(f"   ❌ Badge generation failed: {error}")
            update_sheet_status(row_num, posted=False, error=f"Badge Gen: {error}")
            failed_posts += 1
            continue
        
        print(f"   ✅ QR-enhanced badge created")
        
        # ===== STEP 3: Upload to Ayrshare =====
        print("\n📤 Step 3: Uploading badge to Ayrshare...")
        badge_url = upload_image_to_ayrshare(output_path)
        
        if not badge_url:
            print("   ❌ Upload failed")
            update_sheet_status(row_num, posted=False, error="Ayrshare Upload Failed")
            failed_posts += 1
            continue

        # ===== STEP 4: Generate caption =====
        print(f"\n🤖 Step 4: Generating AI caption...")
        try:
            caption = generate_unique_caption(
                badge_type=badge_type,
                platform=primary_platform,
                username=username
            )
        except Exception as e:
            print(f"   ❌ Caption generation failed: {e}")
            update_sheet_status(row_num, posted=False, error="AI Caption Failed")
            failed_posts += 1
            continue

        # ===== STEP 5: Post to social media =====
        print(f"\n📤 Step 5: Posting to social media...")
        result = post_to_social(caption, badge_url, platforms, username, badge_type)

        # ===== STEP 6: Update Google Sheet with Results =====
        if "error" in result or result.get("status") == "failed":
            error_msg = str(result.get('error', 'Unknown error'))
            print(f"\n❌ Posting failed: {error_msg}")
            update_sheet_status(row_num, posted=False, error=error_msg)
            failed_posts += 1
        else:
            print(f"\n✅ Successfully posted!")
            # This updates Column E, F, G, and H in your sheet
            update_sheet_status(
                row_number=row_num,
                posted=True,
                post_urls=result.get('post_url', 'N/A'),
                tracking_id=result.get('tracking_id', tracking_id)
            )
            
            successful_posts += 1
            for platform in platforms:
                save_caption_to_history(caption, platform)
        
        print(f"{'='*70}\n")
        
        if idx < total_posts:
            wait_time = 8
            print(f"⏳ Waiting {wait_time} seconds before next post...")
            time.sleep(wait_time)
    
    # Final summary
    print("\n" + "="*70)
    print("📊 POSTING SUMMARY")
    print("="*70)
    print(f"   Total Rows Found: {len(users)}")
    print(f"   ✅ Successful: {successful_posts}")
    print(f"   ⏭️ Skipped: {skipped_posts}")
    print(f"   ❌ Failed: {failed_posts}")
    if (successful_posts + failed_posts) > 0:
        rate = (successful_posts / (successful_posts + failed_posts) * 100)
        print(f"   🎯 Success Rate: {rate:.1f}%")
    print("="*70 + "\n")

if __name__ == "__main__":
    print("="*70)
    print("🎯 QR-ENHANCED BADGE GENERATOR + SOCIAL POSTER")
    print("="*70)
    print("   • QR code detection and replacement")
    print("   • Tracking URL embedded in badge QR codes")
    print("   • Gemini 2.5 Flash image generation")
    print("   • AI-generated captions")
    print("   • Private Google Sheets integration")
    print("   • Railway click tracking")
    print("   • Multi-platform posting")
    print("="*70)
    
    try:
        process_badges()
        print("✅ All posts processed successfully!")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()