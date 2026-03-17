"""
caption_generator_db.py - FIXED VERSION
Caption does NOT include "Congratulations" - added later during posting
"""

import google.generativeai as genai
from pinecone import Pinecone
import random
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
import boto3
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import re

load_dotenv()

# API Keys
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX_NAME = "knowledge-base"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
COHERE_MODEL_ID = "cohere.embed-v4:0"
DATABASE_URL = os.getenv("DATABASE_URL")

# Configure APIs
genai.configure(api_key=GEMINI_API_KEY)
pc = Pinecone(api_key=PINECONE_API_KEY)
model = genai.GenerativeModel("gemini-2.5-pro")
index = pc.Index(PINECONE_INDEX_NAME)

# Initialize Bedrock client
bedrock_runtime = boto3.client(
    service_name="bedrock-runtime",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# BRAND HASHTAGS
BRAND_HASHTAGS = {
    "required": ["#EssentiaScan", "#NONAI", "#BarimEnterprises"],
    "secondary": ["#AIFree", "#RealHuman", "#VerifiedHuman"],
    "business": ["#DeepfakeDefense", "#IdentityCrisis", "#ZeroTrust", "#TruthDecay", "#CISO", "#RiskManagement", "#RiskMandate"],
    "social_media": ["#HumanOnly", "#EndDeepFakes", "#IsThisPersonReal", "#DigitalPredators", "#CatfishOrNot", "#TeamHuman", "#NotMyChildAIidentityTheft"]
}


def get_db_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ DB connection error: {e}")
        return None


def init_caption_table():
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS caption_history (
                    id SERIAL PRIMARY KEY,
                    caption TEXT NOT NULL,
                    platform VARCHAR(50) NOT NULL,
                    timestamp TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            conn.commit()
        return True
    finally:
        conn.close()


def load_caption_history():
    conn = get_db_connection()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT caption, platform
                FROM caption_history
                WHERE timestamp >= NOW() - INTERVAL '3 days'
            """)
            return cur.fetchall()
    finally:
        conn.close()


def save_caption_to_history(caption, platform):
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO caption_history (caption, platform) VALUES (%s, %s)",
                (caption, platform)
            )
            conn.commit()
            return True
    finally:
        conn.close()


def get_embedding(text, input_type="search_query"):
    """Generate embedding using Cohere Embed v4 via AWS Bedrock"""
    text = text.replace("\n", " ").strip()
    
    body = json.dumps({
        "texts": [text],
        "input_type": input_type,
        "embedding_types": ["float"],
        "output_dimension": 1024
    })
    
    try:
        response = bedrock_runtime.invoke_model(
            modelId=COHERE_MODEL_ID,
            contentType='application/json',
            accept='application/json',
            body=body
        )
        
        response_body = json.loads(response['body'].read())
        embedding = response_body['embeddings']['float'][0]
        
        return embedding
        
    except Exception as e:
        print(f"❌ Error generating embedding: {str(e)}")
        return None


def parse_hashtag_string(hashtag_string):
    """Parse pipe-separated hashtag string into list of dicts"""
    if not hashtag_string or hashtag_string == '':
        return []
    
    parsed_tags = []
    for item in hashtag_string.split('|'):
        if ':' in item:
            tag, info = item.split(':', 1)
            
            posts_count = 0
            try:
                info_lower = info.lower().strip()
                if 'm' in info_lower:
                    posts_count = int(float(info_lower.replace('m', '')) * 1000000)
                elif 'k' in info_lower:
                    posts_count = int(float(info_lower.replace('k', '')) * 1000)
                elif info_lower.replace('.', '').isdigit():
                    posts_count = int(float(info_lower))
            except:
                posts_count = 0
            
            parsed_tags.append({
                'hashtag': f"#{tag}",
                'posts': posts_count,
                'info': info
            })
    
    return parsed_tags


def get_viral_hashtags(top_k=50):
    """Fetch viral hashtags from Pinecone"""
    try:
        query_text = "trending viral popular engagement social media content creator influencer"
        query_embedding = get_embedding(query_text, input_type="search_query")
        
        if not query_embedding:
            return []
        
        results = index.query(
            vector=query_embedding,
            top_k=top_k,
            namespace="hashtag",
            include_metadata=True
        )
        
        viral_hashtags = {}
        
        for match in results.matches:
            metadata = match.metadata
            main_hashtag = metadata.get('hashtag', '')
            posts_count = metadata.get('postsCount', 0)
            
            if main_hashtag and main_hashtag not in viral_hashtags:
                viral_hashtags[main_hashtag.lower()] = {
                    'hashtag': f"#{main_hashtag}",
                    'posts': posts_count,
                    'score': match.score
                }
            
            related_tags = parse_hashtag_string(metadata.get('related_tags', ''))
            frequent_tags = parse_hashtag_string(metadata.get('frequent_tags', ''))
            related_frequent_tags = parse_hashtag_string(metadata.get('related_frequent_tags', ''))
            
            all_related = related_tags + frequent_tags + related_frequent_tags
            
            for tag_data in all_related:
                tag = tag_data['hashtag'].lower()
                if tag not in viral_hashtags:
                    viral_hashtags[tag] = {
                        'hashtag': tag_data['hashtag'],
                        'posts': tag_data['posts'],
                        'score': match.score * 0.8
                    }
        
        sorted_hashtags = sorted(
            viral_hashtags.values(),
            key=lambda x: x['posts'],
            reverse=True
        )
        
        top_viral = sorted_hashtags[:30]
        return [item['hashtag'] for item in top_viral]
        
    except Exception as e:
        print(f"❌ Error fetching viral hashtags: {e}")
        return []


def build_hashtags(platform='linkedin'):
    """Build brand + viral hashtags following guidelines"""
    # Brand hashtags
    brand_tags = []
    brand_tags.append(random.choice(BRAND_HASHTAGS['required']))
    brand_tags.extend(random.sample(BRAND_HASHTAGS['secondary'], k=min(2, len(BRAND_HASHTAGS['secondary']))))
    
    # Platform-specific hashtag count
    if platform.lower() == 'twitter':
        # Twitter: Fewer hashtags (only 2-3 brand tags, 2 viral)
        brand_tags = brand_tags[:3]  # Keep only 3 brand tags max
        viral_tags = get_viral_hashtags()
        selected_viral = random.sample(viral_tags, min(2, len(viral_tags))) if viral_tags else []
    elif platform.lower() == 'linkedin':
        brand_tags.extend(random.sample(BRAND_HASHTAGS['business'], k=min(3, len(BRAND_HASHTAGS['business']))))
        viral_tags = get_viral_hashtags()
        selected_viral = random.sample(viral_tags, min(5, len(viral_tags))) if viral_tags else []
    else:
        brand_tags.extend(random.sample(BRAND_HASHTAGS['social_media'], k=min(4, len(BRAND_HASHTAGS['social_media']))))
        viral_tags = get_viral_hashtags()
        selected_viral = random.sample(viral_tags, min(5, len(viral_tags))) if viral_tags else []
    
    # Combine
    all_hashtags = f"{' '.join(brand_tags)} {' '.join(selected_viral)}"
    return all_hashtags


def generate_unique_caption(badge_type, platform, username, retry_limit=5):
    """
    Generate caption WITHOUT "Congratulations @username"
    That will be added automatically by add_user_mention() in post_with_dynamic_badges.py
    """
    history = load_caption_history()
    recent = [h["caption"] for h in history if h["platform"] == platform]

    hashtags = build_hashtags(platform)
    
    # Define platform-specific info
    platform_info = {
        'twitter': {'tone': 'punchy, urgent', 'emoji': 'minimal', 'char_limit': '40'},
        'instagram': {'tone': 'celebratory, aspirational', 'emoji': 'generous', 'char_limit': 'none'},
        'linkedin': {'tone': 'professional, inspiring', 'emoji': 'moderate', 'char_limit': 'none'},
        'facebook': {'tone': 'friendly, welcoming', 'emoji': 'moderate', 'char_limit': 'none'}
    }
    
    info = platform_info.get(platform.lower(), platform_info['instagram'])

    prompt = f"""
You are a social media caption generator. Generate ONLY the caption text, no preamble, no explanation, no formatting notes.

Create a VIRAL {platform.upper()} post for NonAI.life that creates MAXIMUM FOMO.

CONTEXT:
- NonAI.life verifies real humans and awards a verification badge
- Someone just got verified and joined the community
- They proved they're human — not AI
- We're celebrating their verification

IMPORTANT: 
- DO NOT start with "Congratulations @{username}" - this is added automatically later
- DO NOT mention the username anywhere in the caption
- Start directly with the celebration message (e.g., "You just got verified...", "Welcome to...", "It's official...")

MARKETING OBJECTIVES:
1. CELEBRATE their achievement - make it about joining the verified community
2. Create FOMO for others – "Everyone is getting verified!"
3. DRIVE CLICKS to https://nonai.life/
4. Make it VIRAL and shareable
5. Urgently push others to get verified ASAP

STRUCTURE:
1st sentence: Celebrate the verification (e.g., "You just got verified on NonAI.life! 🎉")
2nd-3rd sentences: Create FOMO and social proof for others
Last part: Strong CTA with URL

STYLE:
- Tone: {info['tone']}
- Emojis: {info['emoji']}
- {"Under 40 characters" if platform.lower() == 'twitter' else "Clear, punchy, engaging"}
- Start with a strong, celebratory hook
- Include social proof: "50,000+ already verified"

MUST INCLUDE:
✓ Start with celebration (WITHOUT username - e.g., "You just got verified!", "Welcome to the verified community!", "It's official!")
✓ For Twitter: Write 3-4 complete sentences with substance. Make it engaging and worth reading.
✓ DO NOT say whether they were flagged as AI or not
✓ Encourage others to get verified immediately
✓ Strong FOMO language
✓ Clear CTA
✓ The URL https://nonai.life/ on its own line with no other text

AVOID:
✗ DO NOT say "Congratulations" or mention @{username} (added automatically)
✗ Do NOT say "AI", "flagged", or imply failure
✗ Do NOT explain what badge type they received
✗ Do NOT mention specific badge types (gold/silver/bronze/etc.)
✗ Do NOT mention years (e.g., "2025", "2024", etc.)
✗ Do NOT add hashtags (we add separately)
✗ Keep it fresh and different every time
✗ NO introductory text like "Here is the caption" or "Caption:"
✗ NO markdown formatting like ### or **
✗ NO explanations or notes
✗ NO [Visual:] descriptions

IMPORTANT:
This caption must be COMPLETELY DIFFERENT from recent captions and optimized for maximum FOMO and urgency.

Recent captions to avoid duplicating:
{json.dumps(recent[-5:], indent=2)}

OUTPUT ONLY THE CAPTION TEXT (WITHOUT "Congratulations @username").
"""

    for attempt in range(retry_limit):
        response = model.generate_content(prompt)
        caption = response.text.strip()

        # Remove any [Visual:] tags
        caption = re.sub(r"\[.*?\]", "", caption)
        caption = caption.replace("###", "").replace("**", "").strip()
        
        # Remove any "Congratulations" that AI might add anyway
        caption = re.sub(r'^Congratulations\s+[@\w]+[!.]?\s*[🎉🥳✨]*\s*', '', caption, flags=re.IGNORECASE).strip()

        if all(caption[:50].lower() not in r.lower() for r in recent):
            return f"{caption}\n\n{hashtags}"

    # Fallback caption (WITHOUT "Congratulations" - added by add_user_mention)
    if platform.lower() == 'twitter':
        fallback = f"""You just got verified on NonAI.life! 🎉

Welcome to the community of 50,000+ verified humans proving their authenticity in a world full of bots and deepfakes. 

Your turn is waiting. Get verified now.

https://nonai.life/

{hashtags}"""
    else:
        fallback = f"""You're officially verified on NonAI.life! 🎉

Welcome to a community of 50,000+ verified humans. You've proven your authenticity.

Others are watching. Don't get left behind — claim your verification today.

https://nonai.life/

{hashtags}"""
    return fallback


# Init DB
init_caption_table()


if __name__ == "__main__":
    caption = generate_unique_caption(
        badge_type="bronze",
        platform="linkedin",
        username="Mia"
    )
    print("\n" + "="*70)
    print("GENERATED CAPTION (without 'Congratulations @Mia'):")
    print("="*70)
    print(caption)
    print("="*70)
    print("\nNote: 'Congratulations @Mia! 🎉' will be added automatically by add_user_mention()")
    print("="*70)