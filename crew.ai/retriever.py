import os
from pinecone import Pinecone
from dotenv import load_dotenv

load_dotenv()

pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
index = pc.Index("knowledge-base")

def retrieve(namespace, embedding, top_k=5):
    res = index.query(
        namespace=namespace,
        vector=embedding,
        top_k=top_k,
        include_metadata=True
    )
    return [m["metadata"] for m in res["matches"]]


def retrieve_context(embedding):
    """
    Centralized retrieval.
    Book namespace intentionally excluded for now.
    """
    return {
        "news": retrieve("news", embedding),
        "youtube": retrieve("youtube", embedding)
    }
