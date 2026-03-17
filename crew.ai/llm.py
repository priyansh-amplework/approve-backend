import os
from crewai import LLM
from dotenv import load_dotenv

load_dotenv()

def get_gemini_llm():
    """
    Central Gemini LLM initializer for CrewAI
    """
    return LLM(
        model="gemini-2.5-pro",
        provider="google",
        api_key=os.getenv("GEMINI_API_KEY"),
        temperature=0.7
    )
