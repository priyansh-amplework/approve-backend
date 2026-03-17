"""
caption_generator_marketing.py - For NonAI Marketing Videos
Generates promotional captions (NOT winner congratulations)
Includes brand hashtags in caption, Ayrshare adds auto-hashtags
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

# BRAND HASHTAGS FOR MARKETING
# For Instagram: Use 3 brand hashtags in caption, Ayrshare adds 2 auto = 5 total
BRAND_HASHTAGS_MARKETING = {
    "core": ["#NonAI", "#EssentiaScan", "#VerifiedHuman"],  # Always use these 3
    "secondary": ["#HumanVerification", "#BiologicalFirewall", "#ProveYoureHuman"],
    "business": ["#DeepfakeDefense", "#IdentitySecurity", "#ZeroTrust"],
    "social": ["#RealHuman", "#HumanOnly", "#AIFree"]
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


def load_caption_history():
    conn = get_db_connection()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT caption, video_concept, platform
                FROM caption_history_marketing
                WHERE timestamp >= NOW() - INTERVAL '7 days'
            """)
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


def build_marketing_hashtags(platform='instagram'):
    """
    Build brand hashtags for marketing posts
    
    Instagram: 3 brand hashtags (Ayrshare adds 2 auto = 5 total)
    Other platforms: More flexible
    """
    if platform.lower() == 'instagram':
        # ALWAYS use the 3 core brand hashtags for Instagram
        return " ".join(BRAND_HASHTAGS_MARKETING['core'])
    
    elif platform.lower() == 'twitter':
        # Twitter: 3-4 hashtags total
        tags = BRAND_HASHTAGS_MARKETING['core'][:2]  # 2 core tags
        tags.append(random.choice(BRAND_HASHTAGS_MARKETING['social']))  # 1 social tag
        return " ".join(tags)
    
    elif platform.lower() == 'linkedin':
        # LinkedIn: Mix of core + business hashtags
        tags = BRAND_HASHTAGS_MARKETING['core']  # 3 core
        tags.extend(random.sample(BRAND_HASHTAGS_MARKETING['business'], k=2))  # 2 business
        return " ".join(tags)
    
    else:
        # Facebook, etc.: Core + mix
        tags = BRAND_HASHTAGS_MARKETING['core']  # 3 core
        tags.extend(random.sample(BRAND_HASHTAGS_MARKETING['social'], k=2))  # 2 social
        return " ".join(tags)


def generate_marketing_caption(video_concept, platform='instagram', retry_limit=5):
    """
    Generate MARKETING caption for NonAI promotional videos
    
    Args:
        video_concept: The video concept name (e.g., "human_verification_intro")
        platform: Social media platform (default: instagram)
        retry_limit: Max retries for unique caption (default: 5)
    
    Returns:
        Caption with brand hashtags (for Instagram: 3 hashtags, Ayrshare adds 2 more)
    """
    
    history = load_caption_history()
    recent = [h["caption"] for h in history if h["platform"] == platform]

    # Get appropriate hashtags for platform
    hashtags = build_marketing_hashtags(platform)
    
    # Platform-specific guidelines
    platform_info = {
        'twitter': {
            'tone': 'urgent, punchy, direct',
            'emoji': 'minimal (1-2 max)',
            'length': '3-4 sentences, engaging and substantial',
            'style': 'News-style with strong hook'
        },
        'instagram': {
            'tone': 'bold, empowering, aspirational',
            'emoji': 'strategic (3-4 relevant emojis)',
            'length': '4-6 sentences with clear structure',
            'style': 'Visual storytelling with strong CTA'
        },
        'linkedin': {
            'tone': 'professional, authoritative, insightful',
            'emoji': 'minimal (1-2 professional emojis)',
            'length': '3-5 sentences, thought leadership',
            'style': 'Business value proposition'
        },
        'facebook': {
            'tone': 'conversational, relatable, friendly',
            'emoji': 'moderate (2-3 emojis)',
            'length': '3-5 sentences, engaging',
            'style': 'Community-focused with clear benefits'
        }
    }
    
    info = platform_info.get(platform.lower(), platform_info['instagram'])
    
    # Video concept contexts
    concept_context = {
        'human_verification_intro': 'introducing EssentiaScan verification process',
        'deepfake_protection': 'protecting against AI deepfakes and fraud',
        'humanity_score': 'gamification of human uniqueness scoring',
        'creator_authenticity': 'certifying human-created content',
        'multi_factor_verification': 'explaining 5-layer verification technology',
        'ai_impersonation_fight': 'fighting AI identity theft and fraud',
        'human_essence_celebration': 'celebrating what makes us uniquely human',
        'social_media_trust': 'building trust in social media interactions',
        'professional_credibility': 'professional human verification for business',
        'future_of_identity': 'future vision of human identity verification'
    }
    
    context = concept_context.get(video_concept, 'NonAI human verification')

    prompt = f"""
You are a viral social media marketing expert for NonAI.life/EssentiaScan.

Generate a MARKETING {platform.upper()} caption for a promotional video about: {context}

VIDEO CONTEXT:
This is a MARKETING/PROMOTIONAL video (NOT a winner announcement).
The video showcases NonAI.life's human verification technology and benefits.

PRODUCT INFO:
- EssentiaScan: Multi-factor biological verification platform
- Verifies 100% human authenticity (blocks AI, bots, deepfakes)
- 5-layer verification: Face, Voice, Keystroke, Cursor, Sync
- Provides "Verified Human" badge and Non-AI Score
- URL: https://nonai.life/

MARKETING OBJECTIVES:
1. Generate AWARENESS of the problem (AI fraud, deepfakes, bot accounts)
2. Position EssentiaScan as THE SOLUTION
3. Create URGENCY - get verified before it's required
4. Drive TRAFFIC to https://nonai.life/
5. Build SOCIAL PROOF - "50,000+ verified humans"
6. Make it VIRAL and shareable

CAPTION STRUCTURE:
Opening: Strong hook (question, bold statement, or problem statement)
Body: Key benefits or features (2-3 sentences)
Closing: Clear CTA with URL

STYLE GUIDELINES:
- Tone: {info['tone']}
- Emojis: {info['emoji']}
- Length: {info['length']}
- Style: {info['style']}

PLATFORM-SPECIFIC RULES:
{"- Twitter: Write 3-4 complete, substantial sentences. Make every word count." if platform.lower() == 'twitter' else ""}
{"- Instagram: Bold opening, clear value prop, strong CTA. Use line breaks for readability." if platform.lower() == 'instagram' else ""}
{"- LinkedIn: Professional tone, business value, thought leadership angle." if platform.lower() == 'linkedin' else ""}

MUST INCLUDE:
✓ Problem awareness (AI fraud, deepfakes, identity theft, etc.)
✓ Solution positioning (EssentiaScan's unique value)
✓ Social proof ("50,000+ verified humans" or similar)
✓ Strong CTA (action-oriented)
✓ URL https://nonai.life/ on its own line
✓ Urgency/FOMO element

AVOID:
✗ DO NOT mention specific users or congratulations (this is marketing, not winner post)
✗ DO NOT add hashtags (we add separately)
✗ DO NOT mention years (2025, 2024, etc.)
✗ DO NOT use markdown (no ###, **, etc.)
✗ DO NOT include [Visual:] descriptions
✗ DO NOT start with "Here is the caption" or explanations
✗ DO NOT duplicate recent captions

RECENT CAPTIONS TO AVOID:
{json.dumps(recent[-5:], indent=2) if recent else "No recent captions"}

OUTPUT RULES:
- Write ONLY the caption text
- No preamble, no explanation, no formatting notes
- No markdown formatting
- Direct, engaging marketing copy
- Start immediately with the hook

Generate the caption now:
"""

    for attempt in range(retry_limit):
        try:
            response = model.generate_content(prompt)
            caption = response.text.strip()

            # Clean up any unwanted formatting
            caption = re.sub(r"\[.*?\]", "", caption)  # Remove [Visual:] tags
            caption = caption.replace("###", "").replace("**", "").strip()
            caption = re.sub(r'^(Here is.*?:|Caption:)\s*', '', caption, flags=re.IGNORECASE)
            
            # Check uniqueness
            if all(caption[:50].lower() not in r.lower() for r in recent):
                # Save to history
                save_caption_to_history(caption, video_concept, platform)
                
                # Add hashtags
                final_caption = f"{caption}\n\n{hashtags}"
                
                return final_caption
            else:
                print(f"⚠️ Caption too similar to recent, retrying... (attempt {attempt + 1}/{retry_limit})")
                
        except Exception as e:
            print(f"⚠️ Error generating caption (attempt {attempt + 1}/{retry_limit}): {e}")
            time.sleep(2)
    
    # Fallback caption if all retries fail
    fallback_captions = {
        'instagram': f"""In a world full of AI, prove you're 100% human. 🧬

EssentiaScan verifies YOUR biological authenticity with multi-factor verification that AI can't fake.

Join 50,000+ verified humans. Get your badge today.

https://nonai.life/

{hashtags}""",
        
        'twitter': f"""AI fraud is everywhere. EssentiaScan fights back with 5-layer biological verification. Prove you're human, not a bot. Join 50,000+ verified humans today.

https://nonai.life/

{hashtags}""",
        
        'linkedin': f"""The future of digital identity is biological verification.

EssentiaScan provides enterprise-grade human authentication through multi-factor biometric analysis. Protect your organization from AI fraud and deepfake attacks.

https://nonai.life/

{hashtags}""",
        
        'facebook': f"""How do you prove you're really human in 2025? 🤔

EssentiaScan uses advanced biometric verification to give you a "Verified Human" badge. No AI can pass our 5-layer authentication.

Get verified today!

https://nonai.life/

{hashtags}"""
    }
    
    fallback = fallback_captions.get(platform.lower(), fallback_captions['instagram'])
    save_caption_to_history(fallback, video_concept, platform)
    return fallback


# Init DB
init_caption_table()


if __name__ == "__main__":
    print("\n" + "="*70)
    print("NONAI MARKETING CAPTION GENERATOR")
    print("="*70)
    
    # Test different video concepts
    test_concepts = [
        "human_verification_intro",
        "deepfake_protection",
        "humanity_score"
    ]
    
    for concept in test_concepts:
        print(f"\n{'='*70}")
        print(f"VIDEO CONCEPT: {concept.upper().replace('_', ' ')}")
        print(f"{'='*70}")
        
        caption = generate_marketing_caption(
            video_concept=concept,
            platform="instagram"
        )
        
        print(caption)
        print(f"\n✅ Caption includes 3 brand hashtags")
        print(f"   Ayrshare will add 2 more auto-hashtags = 5 TOTAL on Instagram")
        time.sleep(2)  # Avoid rate limits
    
    print("\n" + "="*70)
    print("Testing complete!")
    print("="*70)