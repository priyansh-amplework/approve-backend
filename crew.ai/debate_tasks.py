from crewai import Task

def opening(agent, context):
    return Task(
        description=f"""
        Write a strong OPENING ARGUMENT for your position.

        Rules:
        - Use ONLY the provided news context and documented reporting
        - Base arguments on specific events, studies, or reported consequences
        - Be persuasive, structured, and confident
        - No mention of being an AI
        - No references to YouTube, podcasts, or debates
        - Cite specific news sources when making claims

        CONTEXT (News sources and reporting):
        {context}
        """,
        agent=agent,
        expected_output="A persuasive opening argument grounded in news reporting (2–3 paragraphs)"
    )


def rebuttal(agent, opponent_task, context):
    return Task(
        description=f"""
        Write a REBUTTAL responding directly to your opponent's argument.

        Opponent argument:
        {{task_output:{opponent_task.id}}}

        Rules:
        - Identify the single strongest claim made by your opponent
        - Respond ONLY to that claim using evidence from news sources
        - Expose assumptions, tradeoffs, or second-order effects
        - Do NOT repeat arguments from earlier rounds
        - Use specific examples from the provided news context
        - Be sharp but respectful
        - No references to YouTube, podcasts, or debates
        - Ground rebuttals in documented reporting and real-world cases

        CONTEXT (News sources and reporting):
        {context}
        """,
        agent=agent,
        expected_output="A clear rebuttal countering the opponent's claims with news-based evidence (1–2 paragraphs)"
    )


def closing(agent, context):
    return Task(
        description=f"""
        Write a CLOSING STATEMENT.

        Rules:
        - Reinforce your strongest arguments from news-based evidence
        - Leave a memorable final impression
        - Do not introduce new arguments
        - Reference specific reported consequences or documented trends
        - No references to YouTube, podcasts, or debates

        CONTEXT (News sources and reporting):
        {context}
        """,
        agent=agent,
        expected_output="A concise and impactful closing statement grounded in reporting"
    )


def moderator_blog(agent, debate_reference, context):
    return Task(
        description=f"""
You are a MODERATOR and cultural authority.

Your task is to transform the full debate into a polished,
publication-ready blog that establishes clear standards
around authenticity, human presence, and real-world accountability.

CORE RULES:
- Use ONLY the provided debate content and NEWS context
- Base ALL claims on documented news reporting, studies, or real-world events
- Do NOT mention agents, roles, or debate structure
- Do NOT label positions as "pro-AI" or "anti-AI"
- Do NOT reference YouTube, podcasts, or debate videos
- Write as a standalone published essay
- Tone: confident, grounded, human, uncompromising

EVIDENCE REQUIREMENTS:
- Every claim about AI impact must reference news sources
- Use specific examples: company layoffs, policy changes, 
  industry disruptions, documented harms
- Cite labor reports, cultural journalism, industry analysis
- Ground arguments in verifiable real-world consequences

CONTENT GUIDANCE:
- Translate AI debates into real human stakes involving:
  work, creativity, hip-hop culture, fatherhood, mentorship,
  and legacy
- Emphasize power, ownership, and second-order consequences
- Make it clear that authenticity is a standard, not a preference
- Avoid hype, futurism, or corporate optimism
- Use concrete examples from recent news

STRUCTURE:
- Strong opening hook grounded in recent news or documented event
- Clear thematic sections
- Thoughtful synthesis based on evidence
- "Key Takeaways" (3–5 bullets)
- Reflective conclusion emphasizing human values and accountability

ENDING REQUIREMENTS:
- Close with a thoughtful reflection on:
  • The balance between technological progress and human values
  • The importance of presence, mentorship, and lived experience
  • How wisdom and legacy are transmitted through human relationships
- Keep it authentic and grounded
- No promotional content or product mentions

Debate content:
{{task_output:{debate_reference[0].id}}}
{{task_output:{debate_reference[1].id}}}
{{task_output:{debate_reference[2].id}}}
{{task_output:{debate_reference[3].id}}}
{{task_output:{debate_reference[4].id}}}
{{task_output:{debate_reference[5].id}}}
{{task_output:{debate_reference[6].id}}}
{{task_output:{debate_reference[7].id}}}

CONTEXT (News sources and reporting):
{context}

IMPORTANT:
Output ONLY the final blog.
No meta commentary.
No explanations.
Base ALL arguments on news sources, not YouTube or podcasts.
""",
        agent=agent,
        expected_output=(
            "A publication-ready essay grounded in news reporting that establishes the author as a cultural authority "
            "and ends with a direct, values-based call to the podcast, book, and NonAI.life"
        )
    )