import os
import json
import httpx
import psycopg2
from typing import List, Optional
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# ── Load env ──────────────────────────────────────────────────────────────────
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
AYRSHARE_API_KEY = os.getenv("AYESHARE_API_KEY") # Check: Your code had AYESHARE_API_KEY (typo?)
TRACKING_API_URL = os.getenv("TRACKING_API_URL", "http://44.193.35.107:8000")

app = FastAPI(title="Post Approval Queue API")

# ── CORS ──────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Pydantic Models ───────────────────────────────────────────────────────────
class PostAddRequest(BaseModel):
    content_type: str = "image"
    concept_key: Optional[str] = None
    concept_title: Optional[str] = None
    caption: Optional[str] = None
    hashtags: Optional[str] = None
    media_url: Optional[str] = None
    media_local_path: Optional[str] = None
    available_platforms: Optional[str] = None
    tracking_id: Optional[str] = None
    tracking_url: Optional[str] = None
    source_scheduler: str = "unknown"

class PostApproveRequest(BaseModel):
    id: int
    platforms: List[str]

class PostRejectRequest(BaseModel):
    id: int

class BulkRejectRequest(BaseModel):
    ids: List[int]

# ── DB Helper ─────────────────────────────────────────────────────────────────
def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()

@app.on_event("startup")
def init_db():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS post_queue (
                    id                  SERIAL PRIMARY KEY,
                    content_type        VARCHAR(10)  NOT NULL DEFAULT 'image',
                    concept_key         VARCHAR(100),
                    concept_title       VARCHAR(200),
                    caption             TEXT,
                    hashtags            TEXT,
                    media_url           TEXT,
                    media_local_path    TEXT,
                    available_platforms TEXT,
                    status              VARCHAR(20)  NOT NULL DEFAULT 'pending',
                    posted_platforms    TEXT,
                    created_at          TIMESTAMP    NOT NULL DEFAULT NOW(),
                    updated_at          TIMESTAMP    NOT NULL DEFAULT NOW(),
                    ayrshare_response   JSONB,
                    tracking_id         VARCHAR(100),
                    tracking_url        TEXT,
                    source_scheduler    VARCHAR(50)
                )
            """)
            conn.commit()
        print("✅ post_queue table ready")
    except Exception as e:
        print(f"❌ init_db error: {e}")
        conn.rollback()
    finally:
        conn.close()

# ── Ayrshare Helpers (Async) ──────────────────────────────────────────────────
async def ayrshare_post_async(media_url: str, caption: str, platforms: list, is_video: bool = False) -> dict:
    headers = {
        "Authorization": f"Bearer {AYRSHARE_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "post": caption,
        "platforms": platforms,
        "mediaUrls": [media_url] if media_url else [],
    }
    if is_video:
        payload["isVideo"] = True

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post("https://api.ayrshare.com/api/post", json=payload, headers=headers, timeout=60.0)
            return resp.json()
        except Exception as e:
            return {"error": str(e), "status": "error"}

async def ayrshare_upload_async(local_path: str, is_video: bool = False) -> Optional[str]:
    headers = {"Authorization": f"Bearer {AYRSHARE_API_KEY}"}
    mime = "video/mp4" if is_video else "image/jpeg"
    try:
        async with httpx.AsyncClient() as client:
            with open(local_path, "rb") as f:
                files = {"file": (os.path.basename(local_path), f, mime)}
                resp = await client.post(
                    "https://app.ayrshare.com/api/media/upload",
                    headers=headers, files=files, timeout=120.0
                )
            data = resp.json()
            return data.get("url")
    except Exception as e:
        print(f"❌ Upload error: {e}")
        return None

# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "post-queue-api", "ts": datetime.utcnow().isoformat()}

@app.get("/api/post-queue")
def list_queue(status: str = Query("all"), db=Depends(get_db)):
    """List queued posts. ?status=pending|approved|rejected|all."""
    try:
        with db.cursor(cursor_factory=RealDictCursor) as cur:
            if status == "all":
                cur.execute("SELECT * FROM post_queue ORDER BY created_at DESC LIMIT 200")
            else:
                cur.execute("SELECT * FROM post_queue WHERE status = %s ORDER BY created_at DESC LIMIT 200", (status,))
            rows = cur.fetchall()
        
        # FastAPI handles datetime conversion to ISO automatically if returned as dict
        return {"posts": rows, "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/post-queue/add", status_code=201)
def add_to_queue(body: PostAddRequest, db=Depends(get_db)):
    try:
        with db.cursor() as cur:
            cur.execute("""
                INSERT INTO post_queue
                (content_type, concept_key, concept_title, caption, hashtags,
                 media_url, media_local_path, available_platforms,
                 tracking_id, tracking_url, source_scheduler, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending')
                RETURNING id
            """, (
                body.content_type, body.concept_key, body.concept_title,
                body.caption, body.hashtags, body.media_url,
                body.media_local_path, body.available_platforms,
                body.tracking_id, body.tracking_url, body.source_scheduler
            ))
            new_id = cur.fetchone()[0]
            db.commit()
        return {"success": True, "id": new_id}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/post-queue/approve")
async def approve_post(body: PostApproveRequest, db=Depends(get_db)):
    # 1. Fetch row
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM post_queue WHERE id = %s", (body.id,))
        row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Post not found")
    if row["status"] != "pending":
        raise HTTPException(status_code=400, detail=f"Post is already {row['status']}")

    media_url = row.get("media_url") or ""
    is_video = row.get("content_type") == "reel"

    # 2. Handle Local Upload if needed
    if not media_url and row.get("media_local_path"):
        local = row["media_local_path"]
        if os.path.exists(local):
            media_url = await ayrshare_upload_async(local, is_video=is_video)
            if not media_url:
                raise HTTPException(status_code=500, detail="Media upload to Ayrshare failed")
        else:
            raise HTTPException(status_code=400, detail=f"Local media file not found: {local}")

    # 3. Build text & Post
    caption = row.get("caption") or ""
    hashtags = row.get("hashtags") or ""
    full_text = f"{caption}\n\n{hashtags}".strip() if hashtags else caption

    ayr_resp = await ayrshare_post_async(media_url, full_text, body.platforms, is_video=is_video)
    success = ayr_resp.get("status") == "success"

    # 4. Update DB
    try:
        with db.cursor() as cur:
            cur.execute("""
                UPDATE post_queue
                SET status            = %s,
                    posted_platforms  = %s,
                    ayrshare_response = %s,
                    updated_at        = NOW()
                WHERE id = %s
            """, (
                "approved" if success else "failed",
                ",".join(body.platforms),
                json.dumps(ayr_resp),
                body.id,
            ))
            db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    if success:
        return {"success": True, "ayrshare": ayr_resp}
    else:
        return {"success": False, "error": "Ayrshare post failed", "ayrshare": ayr_resp}

@app.post("/api/post-queue/reject")
def reject_post(body: PostRejectRequest, db=Depends(get_db)):
    try:
        with db.cursor() as cur:
            cur.execute("UPDATE post_queue SET status = 'rejected', updated_at = NOW() WHERE id = %s", (body.id,))
            db.commit()
        return {"success": True, "id": body.id}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/post-queue/bulk-reject")
def bulk_reject(body: BulkRejectRequest, db=Depends(get_db)):
    try:
        with db.cursor() as cur:
            cur.execute("UPDATE post_queue SET status = 'rejected', updated_at = NOW() WHERE id = ANY(%s)", (body.ids,))
            db.commit()
        return {"success": True, "rejected": len(body.ids)}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/post-queue/stats")
def queue_stats(db=Depends(get_db)):
    try:
        with db.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT status, content_type, COUNT(*) as count
                FROM post_queue
                GROUP BY status, content_type
            """)
            rows = cur.fetchall()
        return {"stats": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("QUEUE_API_PORT", 8001))
    uvicorn.run(app, host="0.0.0.0", port=port)
