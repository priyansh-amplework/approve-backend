# embedding.py

import json
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

COHERE_MODEL_ID = "cohere.embed-v4:0"
EMBEDDING_DIM = 1024

bedrock_runtime = boto3.client(
    service_name="bedrock-runtime",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

def embed_texts(texts, input_type):
    """
    Low-level embedding function.
    input_type:
      - search_document (indexing)
      - search_query (retrieval)
    """
    body = json.dumps({
        "texts": [t.replace("\n", " ").strip() for t in texts],
        "input_type": input_type,
        "embedding_types": ["float"],
        "output_dimension": EMBEDDING_DIM
    })

    response = bedrock_runtime.invoke_model(
        modelId=COHERE_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=body
    )

    result = json.loads(response["body"].read())
    return result["embeddings"]["float"]


# -------------------------------
# Public functions you IMPORT
# -------------------------------

def embed_documents(texts):
    """Used for Pinecone upserts"""
    return embed_texts(texts, input_type="search_document")


def embed_query(query):
    """Used for Pinecone search / retrieval"""
    return embed_texts([query], input_type="search_query")[0]
