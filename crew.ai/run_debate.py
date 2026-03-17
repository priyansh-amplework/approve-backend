import os
from pathlib import Path
from datetime import datetime
import google.generativeai as genai

from crewai import Crew
from retriever import retrieve_context
from agents import pro_ai_agent, anti_ai_agent, moderator_agent
from debate_tasks import opening, rebuttal, closing, moderator_blog
from embeddings import embed_query

# ---------------- GEMINI CONFIG ----------------
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel("gemini-2.5-pro")


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


def save_blog_output(blog_text: str, topic: str):
    """Save final polished blog to outputs folder as markdown"""
    Path("outputs").mkdir(exist_ok=True)
    
    # Create a safe filename
    safe_topic = topic.lower().replace(" ", "_").replace("?", "").replace(":", "").replace("/", "_")
    # Limit filename length
    safe_topic = safe_topic[:50]
    filename = f"outputs/blog_{safe_topic}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    
    # Actually write the content to the file
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(blog_text)
        print(f"\n💾 Blog saved to: {filename}\n")
        
        # Verify the file was created and has content
        file_size = os.path.getsize(filename)
        print(f"✅ File size: {file_size} bytes\n")
        
    except Exception as e:
        print(f"\n❌ Error saving blog: {e}\n")
        return None
    
    return filename


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

    # 6️⃣ Save final blog
    saved_file = save_blog_output(polished_blog, topic)

    if saved_file:
        print(f"✅ Blog successfully saved and verified!\n")
    else:
        print(f"⚠️  Warning: Blog may not have been saved properly\n")

    return polished_blog


# 🔥 ENTRY POINT
if __name__ == "__main__":
    final_blog = run()
    print("\n📝 FINAL BLOG OUTPUT:\n")
    print(final_blog)