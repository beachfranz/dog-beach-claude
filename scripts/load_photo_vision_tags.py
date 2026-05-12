"""Two-pass Claude Haiku vision extractor for beach_photos.

Writes structured visual tags into beach_photos.source_meta.vision so
the photo-quality classifier (Tier 2) can train on pixel signal, not
just metadata. Fixes the "dog photo with non-dog title gets rated low"
failure mode surfaced at fid 9838 (id 8943 — collie at 0.23 prob).

Pass 1 → free-form description (image + beach name as context).
Pass 2 → structured JSON extraction (same image + pass-1 description).

Schema:
  has_dog, has_birds, has_human_face_closeup       (bool)
  scene                                            (enum)
  subjects, landscape_features                     (multi-label)
  quality_issue, atmosphere                        (enum)
  confidence                                       (0..1)
  description                                      (pass-1 text)

Run shape — chunked so a single subprocess can't run for hours:
  python scripts/load_photo_vision_tags.py --audit 9838     # 5 photos, no write
  python scripts/load_photo_vision_tags.py --chunk-size 50  # one chunk
  while running; orchestrator can loop chunks with idempotent re-entry.

Idempotency: skips any photo where source_meta.vision.model == current
model id. Re-running after a partial chunk is safe.
"""
from __future__ import annotations
import argparse
import base64
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
_p = urllib.parse.urlparse(POOLER)
PG = dict(host=_p.hostname, port=_p.port or 5432, user=_p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(_p.path or "/postgres").lstrip("/"), sslmode="require")

# Haiku 4.5 vision — cheapest capable vision model per env description.
MODEL = "claude-haiku-4-5-20251001"

# Anthropic per-1M-tokens (Haiku 4.5): input $1, output $5, image ~1500 tok.
# Two-pass per photo ≈ 3000 in + 350 out ≈ $0.005. Budget guard below.
COST_PER_PHOTO_USD = 0.005
DEFAULT_BUDGET_USD = 50.0

LANDSCAPE_FEATURES = [
    "driftwood", "sea_stack", "rock_formation", "cliffs",
    "tide_pools", "dunes", "cave_or_arch", "lagoon", "sandbar",
]
SCENES = [
    "beach_with_sand", "water_only", "coast_no_sand", "wildlife_only",
    "interior", "urban", "screenshot_or_map", "food", "other",
]
SUBJECTS = [
    "dog", "people", "water", "sand", "sunset", "pier", "boats", "structure",
]
QUALITY_ISSUES = ["none", "blurry", "low_light", "distressing", "screenshot"]
ATMOSPHERES = ["sunny", "cloudy", "fog", "stormy", "night", "sunset", "unclear"]


# ─── Anthropic API ────────────────────────────────────────────────────────

import anthropic
_client = anthropic.Anthropic()


def _strip_utm(url: str) -> str:
    """Drop only utm_* params, preserve everything else (e.g. width=1024)."""
    parts = urllib.parse.urlsplit(url)
    qs = [(k, v) for k, v in urllib.parse.parse_qsl(parts.query)
          if not k.startswith("utm_")]
    return urllib.parse.urlunsplit(parts._replace(
        query=urllib.parse.urlencode(qs)))


def _image_source(image_url: str, force_b64: bool = False) -> dict:
    """Return image-source dict. Wikimedia (any subdomain) → always base64
    because Anthropic's URL fetcher fails on both Special:FilePath
    redirects and upload.wikimedia.org URLs in this corpus.
    Strip only utm_* params so width=N hints survive."""
    url = _strip_utm(image_url)
    if force_b64 or "wikimedia.org" in url or "wikipedia.org" in url:
        req = urllib.request.Request(url, headers={"User-Agent": "DogBeachScout/1.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = r.read()
            ct = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
        if len(data) > 5 * 1024 * 1024:
            # >5 MB → Anthropic rejects. Fetch a smaller thumbnail variant
            # via Special:FilePath?width=1024.
            if "Special:FilePath" in url:
                small = url + ("&" if "?" in url else "?") + "width=1024"
                req = urllib.request.Request(small, headers={"User-Agent": "DogBeachScout/1.0"})
                with urllib.request.urlopen(req, timeout=30) as r:
                    data = r.read()
                    ct = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
            else:
                raise RuntimeError(f"image too large ({len(data)} bytes) and no Special:FilePath fallback")
        return {"type": "base64", "media_type": ct,
                "data": base64.b64encode(data).decode()}
    return {"type": "url", "url": url}


def _describe(image_url: str, beach_name: str) -> tuple[str, int, int]:
    """Pass 1: free-form description. Returns (text, in_tok, out_tok)."""
    msg = _client.messages.create(
        model=MODEL,
        max_tokens=120,
        messages=[{
            "role": "user",
            "content": [
                {"type": "image", "source": _image_source(image_url)},
                {"type": "text",
                 "text": (
                    f"This photo is supposedly of {beach_name}. "
                    "Describe what you see in 1-2 plain sentences. Focus on "
                    "the main subject, what's happening, and the setting. "
                    "Do not use markdown headers, do not speculate about "
                    "whether it's actually that beach."
                 )},
            ],
        }],
    )
    text = "".join(b.text for b in msg.content if b.type == "text").strip()
    return text, msg.usage.input_tokens, msg.usage.output_tokens


_EXTRACT_PROMPT = f"""You are tagging a beach photo for a search/sort system.
Given the photo and an initial description, return a JSON object with these fields:

  has_dog: true|false  (dogs, puppies — any breed)
  has_birds: true|false  (shorebirds, gulls, raptors, any bird — but not when they are a tiny dot in the distance)
  has_human_face_closeup: true|false  (a person's face dominates the frame — selfie or portrait)
  scene: one of {SCENES}
  subjects: subset of {SUBJECTS}  (multi-label; pick any that are clearly and prominently present)
  landscape_features: subset of {LANDSCAPE_FEATURES}  (multi-label; ONLY include a feature if it is a PROMINENT, IDENTIFIABLE element of the photo — not background detail.
       Specifically: "driftwood" = an actual log or pile of driftwood that's a recognizable element. Stray twigs or branches do NOT count.
       "tide_pools" = visible pools of water trapped in rocks. Wet sand does NOT count.
       "cliffs" = tall coastal walls, not gentle slopes.
       Empty list is correct and common — most photos have no notable landscape feature.)
  quality_issue: one of {QUALITY_ISSUES}  ("distressing" = dead/injured animals, blood, graphic content)
  atmosphere: one of {ATMOSPHERES}
  confidence: 0.0-1.0  (your self-rated certainty across all fields)

Return ONLY the JSON object, no prose, no markdown fences."""


def _extract(image_url: str, beach_name: str, description: str
             ) -> tuple[dict, int, int]:
    """Pass 2: structured extraction. Returns (parsed, in_tok, out_tok)."""
    msg = _client.messages.create(
        model=MODEL,
        max_tokens=400,
        messages=[{
            "role": "user",
            "content": [
                {"type": "image", "source": _image_source(image_url)},
                {"type": "text",
                 "text": (
                    f"Beach context: {beach_name}\n"
                    f"Initial description: {description}\n\n"
                    + _EXTRACT_PROMPT
                 )},
            ],
        }],
    )
    raw = "".join(b.text for b in msg.content if b.type == "text").strip()
    # Strip possible markdown fences defensively.
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Pass-2 returned non-JSON: {raw[:200]}") from e
    return parsed, msg.usage.input_tokens, msg.usage.output_tokens


# ─── Per-photo driver ─────────────────────────────────────────────────────

def tag_photo(image_url: str, beach_name: str, retries: int = 2
              ) -> tuple[dict, dict]:
    """Run two passes; return (tags, usage). Retries on transient errors."""
    last_err = None
    for attempt in range(retries + 1):
        try:
            desc, in1, out1 = _describe(image_url, beach_name)
            tags, in2, out2 = _extract(image_url, beach_name, desc)
            tags["description"] = desc
            tags["model"] = MODEL
            tags["tagged_at"] = datetime.now(timezone.utc).isoformat()
            usage = {"input_tokens": in1 + in2, "output_tokens": out1 + out2}
            return tags, usage
        except (anthropic.APIError, RuntimeError) as e:
            last_err = e
            if attempt < retries:
                wait = 2 ** attempt * 3   # 3s, 6s, 12s
                print(f"    retry {attempt+1}/{retries} after {wait}s: {e}",
                      flush=True)
                time.sleep(wait)
    raise RuntimeError(f"giving up after {retries} retries: {last_err}")


# ─── Main loop ────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--audit", help="Comma-separated fids — print tags, no write")
    ap.add_argument("--chunk-size", type=int, default=50,
                    help="Photos per run (default 50)")
    ap.add_argument("--no-skip-existing", action="store_true",
                    help="Reprocess photos that already have vision tags")
    ap.add_argument("--budget-usd", type=float, default=DEFAULT_BUDGET_USD,
                    help=f"Hard stop if est cost exceeds (default ${DEFAULT_BUDGET_USD})")
    args = ap.parse_args()

    conn = psycopg2.connect(**PG)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("set statement_timeout = '120s'")

    # Build target set.
    if args.audit:
        fids = [int(x) for x in args.audit.split(",")]
        cur.execute("""
            select bp.id, bp.image_url, bp.thumb_url, bp.source_meta,
                   coalesce(g.display_name_override, g.name) as beach_name
              from public.beach_photos bp
              join public.beaches_gold g on g.fid = bp.arena_group_id
             where bp.arena_group_id = any(%s)
             order by bp.arena_group_id, bp.sort_order
        """, (fids,))
    else:
        skip_clause = ""
        if not args.no_skip_existing:
            skip_clause = f"""and not (source_meta -> 'vision' ->> 'model' = '{MODEL}')"""
        cur.execute(f"""
            select bp.id, bp.image_url, bp.thumb_url, bp.source_meta,
                   coalesce(g.display_name_override, g.name) as beach_name
              from public.beach_photos bp
              join public.beaches_gold g on g.fid = bp.arena_group_id
             where bp.image_url is not null
               and (source_meta -> 'vision' ->> 'model' is null
                    or source_meta -> 'vision' ->> 'model' != '{MODEL}')
             order by bp.id
             limit %s
        """, (args.chunk_size,))
    targets = cur.fetchall()
    print(f"Targets: {len(targets)} photos", flush=True)

    est = len(targets) * COST_PER_PHOTO_USD
    print(f"Est cost: ${est:.2f}  (budget ${args.budget_usd})", flush=True)
    if est > args.budget_usd:
        print("  -> would exceed budget. Lower --chunk-size or raise --budget-usd.")
        sys.exit(1)

    upd_cur = conn.cursor()
    ok = err = 0
    in_tok = out_tok = 0
    t0 = time.time()
    for i, p in enumerate(targets, 1):
        url = p["thumb_url"] or p["image_url"]   # thumb is plenty for vision
        try:
            tags, usage = tag_photo(url, p["beach_name"] or "a beach")
            in_tok += usage["input_tokens"]
            out_tok += usage["output_tokens"]
            ok += 1
        except Exception as e:
            err += 1
            print(f"  [{i}/{len(targets)}] id={p['id']} ERR: {e}", flush=True)
            time.sleep(1.0)
            continue

        if args.audit:
            print(f"  [{i}/{len(targets)}] id={p['id']}  "
                  f"dog={tags.get('has_dog')}  birds={tags.get('has_birds')}  "
                  f"scene={tags.get('scene')}  q={tags.get('quality_issue')}  "
                  f"features={tags.get('landscape_features')}",
                  flush=True)
            print(f"      desc: {tags.get('description', '')[:160]}", flush=True)
        else:
            upd_cur.execute("""
                update public.beach_photos
                   set source_meta = source_meta || jsonb_build_object('vision', %s::jsonb)
                 where id = %s
            """, (json.dumps(tags), p["id"]))

        if i % 20 == 0:
            conn.commit()
            elapsed = time.time() - t0
            rate = i / elapsed
            eta = (len(targets) - i) / rate if rate > 0 else 0
            print(f"  [{i}/{len(targets)}]  ok={ok}  err={err}  "
                  f"{rate:.1f}/s  eta={eta:.0f}s  "
                  f"tok in={in_tok:,} out={out_tok:,}", flush=True)

        time.sleep(0.15)   # gentle pacing — Haiku has plenty of headroom

    if not args.audit:
        conn.commit()

    actual = in_tok * 1e-6 + out_tok * 5e-6   # Haiku 4.5 pricing
    print(f"\nDone in {time.time()-t0:.0f}s. ok={ok} err={err}", flush=True)
    print(f"Tokens: in={in_tok:,} out={out_tok:,}  est cost=${actual:.2f}", flush=True)


if __name__ == "__main__":
    main()
