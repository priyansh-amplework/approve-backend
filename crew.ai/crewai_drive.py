import os
from pathlib import Path
from datetime import datetime
import google.generativeai as genai
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from crewai import Crew
from retriever import retrieve_context
from agents import pro_ai_agent, anti_ai_agent, moderator_agent
from debate_tasks import opening, rebuttal, closing, moderator_blog
from embeddings import embed_query

# ---------------- GEMINI CONFIG ----------------
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel("gemini-2.5-pro")

# ---------------- GOOGLE DRIVE CONFIG ----------------
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "service_account.json")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")  # Optional: specific folder ID
SCOPES = [
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive'
]


# ---------------- GOOGLE DRIVE UTILS ----------------


def check_drive_access(folder_id):
    service = get_gdrive_service()
    folder = service.files().get(
        fileId=folder_id,
        fields="id, name",
        supportsAllDrives=True
    ).execute()
    print(f"✅ Access confirmed: {folder['name']}")

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
        raise Exception(f"Service account file not found: {GOOGLE_SERVICE_ACCOUNT_FILE}")
    except Exception as e:
        raise Exception(f"Failed to authenticate with Google Drive: {str(e)}")
    
check_drive_access(GDRIVE_FOLDER_ID)


def upload_to_gdrive(file_path: str, folder_id: str = None):
    """
    Upload a file to Google Drive using service account
    
    Args:
        file_path: Path to the local file to upload
        folder_id: Google Drive folder ID (optional)
    
    Returns:
        Dict with file info (id, name, webViewLink)
    """
    try:
        service = get_gdrive_service()
        
        file_name = os.path.basename(file_path)
        
        file_metadata = {
            'name': file_name,
            'mimeType': 'text/markdown'
        }
        
        # If folder_id is provided, set it as parent
        if folder_id:
            file_metadata['parents'] = [folder_id]
        
        media = MediaFileUpload(file_path, mimetype='text/markdown', resumable=True)
        
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, webViewLink',
            supportsAllDrives=True
        ).execute()
        
        print(f"\n✅ File uploaded to Google Drive!")
        print(f"📄 File name: {file.get('name')}")
        print(f"🔗 File ID: {file.get('id')}")
        print(f"🌐 View link: {file.get('webViewLink')}\n")
        
        return {
            'id': file.get('id'),
            'name': file.get('name'),
            'link': file.get('webViewLink')
        }
        
    except Exception as e:
        print(f"\n❌ Error uploading to Google Drive: {e}\n")
        return None


def list_gdrive_folders(parent_folder_id=None):
    """
    List folders in Google Drive (helpful for finding folder IDs)
    
    Args:
        parent_folder_id: Optional parent folder to list subfolders from
    
    Returns:
        List of folder dicts with id and name
    """
    try:
        service = get_gdrive_service()
        
        query = "mimeType='application/vnd.google-apps.folder'"
        if parent_folder_id:
            query += f" and '{parent_folder_id}' in parents"
        
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)',
            pageSize=100
        ).execute()
        
        folders = results.get('files', [])
        return folders
        
    except Exception as e:
        print(f"Error listing folders: {e}")
        return []


# ---------------- UTILS ----------------
def generate_new_ai_topic():
    """Generate AI topics focused on real-world harms and human costs"""
    prompt = (
        "Generate a fresh, current AI-related topic focused on one of these themes:\n"
        "- Job displacement and labor market disruption\n"
        "- Creative industry collapse (music, art, writing, film)\n"
        "- Loss of apprenticeship and mentorship models\n"
        "- Skill atrophy and deskilling\n"
        "- Cultural homogenization and loss of authentic voice\n"
        "- Power consolidation in tech platforms\n"
        "- Erosion of human judgment in critical domains\n"
        "- Impact on education and learning\n"
        "- Weakening of institutional knowledge\n"
        "- Unintended societal consequences\n\n"
        "The topic should:\n"
        "- Be grounded in recent news or documented trends\n"
        "- Invite critical examination, not celebration\n"
        "- Focus on human costs and second-order effects\n"
        "- Be suitable for a 800-1200 word critical essay\n\n"
        "Return ONLY the topic title (no quotes, explanations, or instructions)."
    )
    response = gemini_model.generate_content(prompt)
    topic = response.text.strip().replace('"', '').replace("'", '')
    return topic


def clean_markdown_formatting(text: str) -> str:
    """Remove markdown formatting to make text look more natural and human-written"""
    
    # Remove "A Moderated AI Debate" subtitle
    text = text.replace(': A Moderated AI Debate', '')
    text = text.replace('A Moderated AI Debate', '')
    
    # Remove markdown code blocks
    text = text.replace('```markdown', '')
    text = text.replace('```', '')
    
    lines = text.split('\n')
    cleaned_lines = []
    
    for line in lines:
        line = line.strip()
        
        # Skip empty lines (we'll add proper spacing later)
        if not line:
            cleaned_lines.append('')
            continue
        
        # Remove markdown headers (###, ##, #) but keep the text
        if line.startswith('#'):
            line = line.lstrip('#').strip()
            if line:
                cleaned_lines.append(line)
                cleaned_lines.append('')  # Add spacing after headers
                continue
        
        # Remove bold markers (**text** or __text__)
        while '**' in line:
            line = line.replace('**', '')
        while '__' in line:
            line = line.replace('__', '')
        
        # Handle bullet points - convert to clean bullets
        if line.startswith('*') or line.startswith('-'):
            line = line.lstrip('*-').strip()
            if line:
                cleaned_lines.append(f"• {line}")
                continue
        
        # Add the cleaned line
        if line:
            cleaned_lines.append(line)
    
    # Join lines
    result = '\n'.join(cleaned_lines)
    
    # Replace multiple consecutive newlines with double newlines
    while '\n\n\n' in result:
        result = result.replace('\n\n\n', '\n\n')
    
    return result.strip()


def rewrite_blog_polished(raw_blog: str, topic: str, headline: str) -> str:
    """Rewrite debate as polished blog grounded in news reporting"""
    prompt = f"""
You are a professional cultural writer and authority voice.

Your task is to rewrite the following content into a polished,
publication-ready blog that reinforces authenticity, realness,
and human accountability as non-negotiable standards.

HEADLINE:
"{headline}"

CORE WRITING RULES:
- Write with conviction, not neutrality
- Use clear, short paragraphs suitable for online reading
- Do NOT reference debates, agents, process, YouTube, or podcasts
- Do NOT use sales language, hype, or soft persuasion
- Maintain a grounded, human, lived-experience tone
- Base ALL claims on news sources and documented reporting

EVIDENCE REQUIREMENTS (CRITICAL):
- Every claim about AI impact MUST reference recent news
- Use specific examples: company names, policy changes, industry disruptions
- Cite labor reports, cultural journalism, industry analysis
- Ground arguments in verifiable real-world consequences
- Do NOT reference YouTube videos, podcasts, or debate content
- Only use documented news sources and reporting

CONTENT REQUIREMENTS:
- Translate AI risks into real human stakes involving:
  work, creativity, hip-hop, fatherhood, mentorship, and legacy
- Make it clear that authenticity is something demonstrated, not claimed
- Remove any mention of "Pro AI" or "Anti AI"
- Include 2–3 concrete news-based examples with specifics
- Include a concise "Key Takeaways" section (3–5 bullets)

ENDING REQUIREMENTS:
- End with a thoughtful, reflective conclusion that emphasizes:
  • The importance of human presence, judgment, and accountability
  • How legacy and wisdom are built through lived experience
  • The tension between technological efficiency and human values
- Keep the ending grounded and authentic
- No promotional content or calls to action

IMPORTANT:
- Output ONLY the final blog content
- No preamble, no explanations
- Base ALL arguments on news sources, NOT YouTube or podcasts

CONTENT TO REWRITE:
{raw_blog}
"""

    response = gemini_model.generate_content(prompt)
    final_article = response.text.strip()

    # ---------------- CLEAN UP ----------------
    # Remove common preamble phrases
    preamble_phrases = [
        "Of course!", 
        "Here is the rewritten debate content", 
        "As requested,", 
        "***",
        "Here is the blog",
        "as a polished, social-media-ready blog post.",
        "as a polished, social-media-ready blog",
        "Here's the blog post:",
        "Here's the rewritten content:"
    ]
    
    for phrase in preamble_phrases:
        # Case-insensitive replacement
        if phrase.lower() in final_article.lower():
            idx = final_article.lower().find(phrase.lower())
            final_article = final_article[:idx] + final_article[idx + len(phrase):]
    
    # Remove leading/trailing whitespace
    final_article = final_article.strip()
    
    # Remove excessive empty lines
    lines = final_article.split("\n")
    cleaned_lines = []
    for line in lines:
        if line.strip() or (cleaned_lines and cleaned_lines[-1].strip()):
            cleaned_lines.append(line)
    
    final_article = "\n".join(cleaned_lines).strip()
    
    # Apply markdown cleanup to make it more human-readable
    final_article = clean_markdown_formatting(final_article)

    return final_article


def save_blog_output(blog_text: str, topic: str, upload_to_drive: bool = True):
    """
    Save final polished blog to Google Drive (and optionally keep local copy)
    
    Args:
        blog_text: The blog content to save
        topic: The blog topic (used for filename)
        upload_to_drive: Whether to upload to Google Drive (default: True)
    
    Returns:
        Dictionary with local filepath and Google Drive file info
    """
    # Create temp directory for local storage
    Path("temp_outputs").mkdir(exist_ok=True)
    
    # Create a safe filename
    safe_topic = topic.lower().replace(" ", "_").replace("?", "").replace(":", "").replace("/", "_")
    # Limit filename length
    safe_topic = safe_topic[:50]
    filename = f"temp_outputs/blog_{safe_topic}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    
    result = {
        'local_file': None,
        'gdrive_file_id': None,
        'gdrive_link': None,
        'gdrive_name': None
    }
    
    # Write to local file first
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(blog_text)
        print(f"\n💾 Blog saved locally to: {filename}\n")
        
        # Verify the file was created and has content
        file_size = os.path.getsize(filename)
        print(f"✅ File size: {file_size} bytes\n")
        
        result['local_file'] = filename
        
    except Exception as e:
        print(f"\n❌ Error saving blog locally: {e}\n")
        return result
    
    # Upload to Google Drive if requested
    if upload_to_drive:
        try:
            folder_id = GDRIVE_FOLDER_ID if GDRIVE_FOLDER_ID else None
            gdrive_file = upload_to_gdrive(filename, folder_id)
            
            if gdrive_file:
                result['gdrive_file_id'] = gdrive_file['id']
                result['gdrive_link'] = gdrive_file['link']
                result['gdrive_name'] = gdrive_file['name']
                
                # Optionally delete local file after successful upload
                # Uncomment the lines below if you want to auto-delete local files
                # os.remove(filename)
                # print(f"🗑️  Local temp file deleted after successful upload\n")
            
        except Exception as e:
            print(f"\n⚠️  Warning: Could not upload to Google Drive: {e}\n")
            print(f"📁 File is still available locally at: {filename}\n")
    
    return result


# ---------------- MAIN PIPELINE ----------------
def run():
    # 1️⃣ Generate a new AI topic
    topic = generate_new_ai_topic()
    print(f"\n🚀 New AI Topic: {topic}\n")

    # 2️⃣ Embed topic and retrieve context from NEWS sources only
    query_embedding = embed_query(topic)
    contexts = retrieve_context(query_embedding)
    
    # Get news context (filter out any YouTube/podcast content if present)
    news_context = contexts.get("news", [])
    
    # Filter to ensure only news sources
    filtered_context = []
    for ctx in news_context:
        # Skip if context mentions YouTube, podcasts, or video content
        if isinstance(ctx, str):
            lower_ctx = ctx.lower()
            if not any(term in lower_ctx for term in ['youtube', 'podcast', 'video', 'episode']):
                filtered_context.append(ctx)
        else:
            filtered_context.append(ctx)
    
    pro_context = filtered_context
    anti_context = filtered_context
    mod_context = filtered_context

    print(f"\n📰 Retrieved {len(filtered_context)} news-based context items\n")

    # 3️⃣ Create debate tasks

    # Openings
    pro_open_task = opening(pro_ai_agent, pro_context)
    anti_open_task = opening(anti_ai_agent, anti_context)

    # Rebuttal Round 1
    pro_rebut_1 = rebuttal(pro_ai_agent, anti_open_task, pro_context)
    anti_rebut_1 = rebuttal(anti_ai_agent, pro_open_task, anti_context)

    # Rebuttal Round 2
    pro_rebut_2 = rebuttal(pro_ai_agent, anti_rebut_1, pro_context)
    anti_rebut_2 = rebuttal(anti_ai_agent, pro_rebut_1, anti_context)

    # Closings
    pro_close_task = closing(pro_ai_agent, pro_context)
    anti_close_task = closing(anti_ai_agent, anti_context)

    # Moderator synthesis
    moderator_task = moderator_blog(
        moderator_agent,
        debate_reference=[
            pro_open_task,
            anti_open_task,
            pro_rebut_1,
            anti_rebut_1,
            pro_rebut_2,
            anti_rebut_2,
            pro_close_task,
            anti_close_task
        ],
        context=mod_context
    )

    # 4️⃣ Run Crew
    crew = Crew(
        agents=[pro_ai_agent, anti_ai_agent, moderator_agent],
        tasks=[
            pro_open_task,
            anti_open_task,
            pro_rebut_1,
            anti_rebut_1,
            pro_rebut_2,
            anti_rebut_2,
            pro_close_task,
            anti_close_task,
            moderator_task
        ],
        verbose=True,
    )

    raw_debate_output = crew.kickoff()
    print("\n📝 Debate complete!\n")

    # 5️⃣ Rewrite as polished blog
    headline = f"{topic}"
    polished_blog = rewrite_blog_polished(str(raw_debate_output), topic, headline)

    # 6️⃣ Save final blog to Google Drive
    save_result = save_blog_output(polished_blog, topic, upload_to_drive=True)

    if save_result['local_file']:
        print(f"✅ Blog successfully saved locally!\n")
    
    if save_result['gdrive_file_id']:
        print(f"✅ Blog successfully uploaded to Google Drive!")
        print(f"🔗 View at: {save_result['gdrive_link']}\n")
    else:
        print(f"⚠️  Warning: Blog may not have been uploaded to Google Drive\n")

    return polished_blog, save_result


# 🔥 ENTRY POINT
if __name__ == "__main__":
    final_blog, save_info = run()
    print("\n📝 FINAL BLOG OUTPUT:\n")
    print(final_blog)
    
    if save_info.get('gdrive_link'):
        print(f"\n🌐 View on Google Drive: {save_info['gdrive_link']}")