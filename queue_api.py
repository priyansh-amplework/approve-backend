"""
queue_api.py
Post Approval Queue — lightweight Flask API (port 8001)

Endpoints:
  GET  /health
  GET  /api/post-queue?status=pending|approved|rejected|all
  POST /api/post-queue/add         — used by schedulers
  POST /api/post-queue/approve     — posts to Ayrshare then marks approved
  POST /api/post-queue/reject      — marks rejected
"""

import os
import json
import time
import requests
import psycopg2
from flask import Flask, request, jsonify
from flask_cors import CORS
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from datetime import datetime

# ── Load env ──────────────────────────────────────────────────────────────────
load_dotenv()

DATABASE_URL     = os.getenv("DATABASE_URL")
AYRSHARE_API_KEY = os.getenv("AYESHARE_API_KEY")
TRACKING_API_URL = os.getenv("TRACKING_API_URL", "http://44.193.35.107:8000")

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# ── DB helper ─────────────────────────────────────────────────────────────────
def get_db():
    return psycopg2.connect(DATABASE_URL)

# ── Schema init ───────────────────────────────────────────────────────────────
def init_db():
    conn = get_db()
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
                    ayrshare_response   JSONB
                )
            """)
            # Safe migrations
            for col, dtype in [
                ("tracking_id",      "VARCHAR(100)"),
                ("tracking_url",     "TEXT"),
                ("source_scheduler", "VARCHAR(50)"),
            ]:
                cur.execute(f"""
                    ALTER TABLE post_queue
                    ADD COLUMN IF NOT EXISTS {col} {dtype}
                """)
            conn.commit()
        print("✅ post_queue table ready")
    except Exception as e:
        print(f"❌ init_db error: {e}")
        conn.rollback()
    finally:
        conn.close()


# ── Ayrshare helpers ──────────────────────────────────────────────────────────
def ayrshare_post(media_url: str, caption: str, platforms: list,
                  is_video: bool = False) -> dict:
    """Call Ayrshare /api/post and return the raw response JSON."""
    headers = {
        "Authorization": f"Bearer {AYRSHARE_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "post":       caption,
        "platforms":  platforms,
        "mediaUrls":  [media_url] if media_url else [],
    }
    if is_video:
        payload["isVideo"] = True

    try:
        resp = requests.post(
            "https://api.ayrshare.com/api/post",
            json=payload, headers=headers, timeout=60,
        )
        return resp.json()
    except Exception as e:
        return {"error": str(e), "status": "error"}


def ayrshare_upload(local_path: str, is_video: bool = False) -> str | None:
    """Upload a local file to Ayrshare media CDN. Returns URL or None."""
    headers = {"Authorization": f"Bearer {AYRSHARE_API_KEY}"}
    mime = "video/mp4" if is_video else "image/jpeg"
    try:
        with open(local_path, "rb") as f:
            files = {"file": (os.path.basename(local_path), f, mime)}
            resp = requests.post(
                "https://app.ayrshare.com/api/media/upload",
                headers=headers, files=files, timeout=120,
            )
        data = resp.json()
        return data.get("url")
    except Exception as e:
        print(f"❌ Upload error: {e}")
        return None


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "post-queue-api", "ts": datetime.utcnow().isoformat()})


@app.route("/api/post-queue", methods=["GET"])
def list_queue():
    """List queued posts. ?status=pending|approved|rejected|all (default: all)."""
    status_filter = request.args.get("status", "all")
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if status_filter == "all":
                cur.execute("""
                    SELECT * FROM post_queue
                    ORDER BY created_at DESC
                    LIMIT 200
                """)
            else:
                cur.execute("""
                    SELECT * FROM post_queue
                    WHERE status = %s
                    ORDER BY created_at DESC
                    LIMIT 200
                """, (status_filter,))
            rows = cur.fetchall()
        # Convert datetimes to ISO strings
        result = []
        for row in rows:
            r = dict(row)
            for k in ("created_at", "updated_at"):
                if r.get(k):
                    r[k] = r[k].isoformat()
            result.append(r)
        return jsonify({"posts": result, "count": len(result)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/post-queue/add", methods=["POST"])
def add_to_queue():
    """
    Body (JSON):
      content_type, concept_key, concept_title, caption, hashtags,
      media_url, media_local_path, available_platforms,
      tracking_id, tracking_url, source_scheduler
    """
    body = request.get_json(force=True) or {}
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO post_queue
                (content_type, concept_key, concept_title, caption, hashtags,
                 media_url, media_local_path, available_platforms,
                 tracking_id, tracking_url, source_scheduler, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending')
                RETURNING id
            """, (
                body.get("content_type", "image"),
                body.get("concept_key"),
                body.get("concept_title"),
                body.get("caption"),
                body.get("hashtags"),
                body.get("media_url"),
                body.get("media_local_path"),
                body.get("available_platforms"),
                body.get("tracking_id"),
                body.get("tracking_url"),
                body.get("source_scheduler", "unknown"),
            ))
            new_id = cur.fetchone()[0]
            conn.commit()
        return jsonify({"success": True, "id": new_id}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/post-queue/approve", methods=["POST"])
def approve_post():
    """
    Body (JSON):
      id       : int   — row id in post_queue
      platforms: list  — e.g. ["instagram","facebook"]

    Fetches the row, calls Ayrshare, marks approved.
    If media_url is empty but media_local_path exists, uploads first.
    """
    body = request.get_json(force=True) or {}
    row_id    = body.get("id")
    platforms = body.get("platforms", [])

    if not row_id or not platforms:
        return jsonify({"error": "id and platforms are required"}), 400

    # Fetch row
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM post_queue WHERE id = %s", (row_id,))
            row = cur.fetchone()
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

    if not row:
        return jsonify({"error": "Post not found"}), 404
    if row["status"] != "pending":
        return jsonify({"error": f"Post is already {row['status']}"}), 400

    media_url = row.get("media_url") or ""
    is_video  = row.get("content_type", "image") == "reel"

    # If no CDN URL but have a local file, upload now
    if not media_url and row.get("media_local_path"):
        local = row["media_local_path"]
        if os.path.exists(local):
            print(f"📤 Uploading local file: {local}")
            media_url = ayrshare_upload(local, is_video=is_video)
            if not media_url:
                return jsonify({"error": "Media upload to Ayrshare failed"}), 500
        else:
            return jsonify({"error": f"Local media file not found: {local}"}), 400

    # Build full caption (caption + hashtags)
    caption   = row.get("caption") or ""
    hashtags  = row.get("hashtags") or ""
    full_text = f"{caption}\n\n{hashtags}".strip() if hashtags else caption

    # Call Ayrshare
    print(f"📲 Posting queue item #{row_id} to {platforms}")
    ayr_resp = ayrshare_post(media_url, full_text, platforms, is_video=is_video)
    print(f"📋 Ayrshare response: {json.dumps(ayr_resp, indent=2)}")

    success = ayr_resp.get("status") == "success"

    # Update DB
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE post_queue
                SET status            = %s,
                    posted_platforms  = %s,
                    ayrshare_response = %s,
                    updated_at        = NOW()
                WHERE id = %s
            """, (
                "approved" if success else "failed",
                ",".join(platforms),
                json.dumps(ayr_resp),
                row_id,
            ))
            conn.commit()
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

    if success:
        return jsonify({"success": True, "ayrshare": ayr_resp})
    else:
        return jsonify({"success": False, "error": "Ayrshare post failed", "ayrshare": ayr_resp}), 502


@app.route("/api/post-queue/reject", methods=["POST"])
def reject_post():
    """Body: { id: int }"""
    body   = request.get_json(force=True) or {}
    row_id = body.get("id")
    if not row_id:
        return jsonify({"error": "id is required"}), 400

    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE post_queue
                SET status = 'rejected', updated_at = NOW()
                WHERE id = %s
            """, (row_id,))
            conn.commit()
        return jsonify({"success": True, "id": row_id})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/post-queue/bulk-reject", methods=["POST"])
def bulk_reject():
    """Body: { ids: [int, ...] }"""
    body = request.get_json(force=True) or {}
    ids  = body.get("ids", [])
    if not ids:
        return jsonify({"error": "ids list is required"}), 400
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE post_queue
                SET status = 'rejected', updated_at = NOW()
                WHERE id = ANY(%s)
            """, (ids,))
            conn.commit()
        return jsonify({"success": True, "rejected": len(ids)})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/post-queue/stats", methods=["GET"])
def queue_stats():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT status, content_type, COUNT(*) as count
                FROM post_queue
                GROUP BY status, content_type
            """)
            rows = [dict(r) for r in cur.fetchall()]
        return jsonify({"stats": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🚀 Post Queue API starting …")
    init_db()
    port = int(os.getenv("QUEUE_API_PORT", 8001))
    app.run(host="0.0.0.0", port=port, debug=False)
