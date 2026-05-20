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
import sys
sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
import truststore                          # Win Python 3.14 OS-cert store
truststore.inject_into_ssl()

import argparse
import base64
import json
import os
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
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
# Schema version — bump when the prompt/output shape changes so old rows
# get re-tagged automatically without manual clearing.
#   v1: initial schema
#   v2: added has_surfing + has_active_people, dropped "people" from subjects
#   v3: added has_path + has_vehicle (Franz 2026-05-19 photo curation v3)
SCHEMA_VERSION = "v3"

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
    "dog", "water", "sand", "sunset", "pier", "boats", "structure",
    # "people" intentionally dropped — has_active_people / has_human_face_closeup cover it.
]
QUALITY_ISSUES = ["none", "blurry", "low_light", "distressing", "screenshot"]
ATMOSPHERES = ["sunny", "cloudy", "fog", "stormy", "night", "sunset", "unclear"]


# ─── Anthropic API ────────────────────────────────────────────────────────

import anthropic
_client = anthropic.Anthropic()

# Wikimedia robot policy (w.wiki/4wJS) requires:
#  - identifying User-Agent with contact info
#  - serial, paced access (concurrent fetches trigger "robot policy" 429s
#    that block IPs, distinct from normal rate-limit throttling)
# Use a global lock + post-fetch sleep instead of just a 2-slot semaphore.
WIKIMEDIA_UA = ("DogBeachScout/1.0 (https://dogbeachscout.com; "
                "franz@franzfunk.com) photo-curation vision-backfill")
_wm_lock = threading.Lock()
WM_PACING_S = 1.5


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
    Strip only utm_* params so width=N hints survive.

    Raises RuntimeError('bad_url') early for malformed URLs (no protocol,
    HTTP-only, or non-URL strings) so we don't burn 3 Anthropic retries
    on data-quality issues."""
    if not image_url or not isinstance(image_url, str):
        raise RuntimeError("bad_url: empty")
    if not image_url.startswith("https://"):
        raise RuntimeError(f"bad_url: not https ({image_url[:60]})")
    url = _strip_utm(image_url)
    # parks.wa.gov 403s Anthropic's URL fetcher UA — download server-side
    # with browser UA, base64 to Anthropic. No serial lock needed (small
    # site but no per-IP rate limit observed). Per WSPRC loader 2026-05-19.
    if "parks.wa.gov" in url:
        browser_ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/130.0.0.0 Safari/537.36")
        req = urllib.request.Request(url, headers={
            "User-Agent": browser_ua,
            "Accept": "image/avif,image/webp,image/jpeg,image/png,*/*",
            "Accept-Language": "en-US,en;q=0.9",
        })
        with urllib.request.urlopen(req, timeout=30) as r:
            data = r.read()
            ct = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
        # Anthropic vision 5MB cap is on the BASE64-ENCODED size, not
        # raw — so cap raw at 3.5 MB (base64 inflates ~33%, plus margin).
        # WSPRC originals routinely 4-6 MB raw → 5-8 MB base64. Resize via
        # Pillow, stepping the longest edge down 2000→1400→1000 until under.
        RAW_CAP = 3 * 1024 * 1024 + 512 * 1024   # 3.5 MB raw → ~4.67 MB b64
        if len(data) > RAW_CAP:
            from PIL import Image  # local import — Pillow is available
            import io
            for max_edge in (2000, 1400, 1000):
                img = Image.open(io.BytesIO(data))
                img.thumbnail((max_edge, max_edge))
                buf = io.BytesIO()
                if img.mode in ("RGBA", "P"):
                    img = img.convert("RGB")
                img.save(buf, format="JPEG", quality=85, optimize=True)
                data = buf.getvalue()
                ct = "image/jpeg"
                if len(data) <= RAW_CAP:
                    break
            if len(data) > RAW_CAP:
                raise RuntimeError(f"wsprc image too large after resize: {len(data)} bytes raw")
        return {"type": "base64", "media_type": ct,
                "data": base64.b64encode(data).decode()}
    if force_b64 or "wikimedia.org" in url or "wikipedia.org" in url:
        with _wm_lock:                         # serial wikimedia access
            req = urllib.request.Request(url, headers={"User-Agent": WIKIMEDIA_UA})
            with urllib.request.urlopen(req, timeout=30) as r:
                data = r.read()
                ct = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
            # Width cascade for big images. TIFs at width=1024 can still
            # exceed 5MB; step down to 800, then 500. Special:FilePath also
            # serves JPEG-converted output for TIF sources at smaller widths.
            for cascade_width in (1024, 800, 500):
                if len(data) <= 5 * 1024 * 1024:
                    break
                if "Special:FilePath" not in url:
                    # upload.wikimedia.org direct URLs don't have a width
                    # knob — derive Special:FilePath from filename.
                    fname = url.rsplit("/", 1)[-1].split("?")[0]
                    url = f"https://commons.wikimedia.org/wiki/Special:FilePath/{fname}"
                # Re-build URL with explicit width (overwriting prior width=).
                base = url.split("?")[0]
                small = f"{base}?width={cascade_width}"
                req = urllib.request.Request(small, headers={"User-Agent": WIKIMEDIA_UA})
                with urllib.request.urlopen(req, timeout=30) as r:
                    data = r.read()
                    ct = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
                time.sleep(WM_PACING_S)        # pace each retry too
            if len(data) > 5 * 1024 * 1024:
                raise RuntimeError(f"image too large even at width=500: {len(data)} bytes")
            time.sleep(WM_PACING_S)            # pace AFTER successful fetch, before releasing lock
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
  has_surfing: true|false  (visible surfers riding waves, OR surfboards/paddleboards/wetsuits being carried or in use. Just calm flat water with no surf activity = false.)
  has_active_people: true|false  (people doing things on the beach — playing, walking, swimming, picnicking, kids, families. Not just one tiny figure in the distance, but a recognizable beachgoer present and engaged. A close-up portrait counts as active_people=false; use has_human_face_closeup for that.)
  has_human_face_closeup: true|false  (a person's face dominates the frame — selfie or portrait shot)
  has_path: true|false  (paths, trails, boardwalks, walkways, or stairs leading to or along the beach. Includes wooden boardwalks, dirt trails, paved walking paths, beach-access stairs. NOT roads, parking-lot driveways, or vehicle paths.)
  has_vehicle: true|false  (cars, trucks, RVs, parking lots, vehicles on the beach or visible in the frame. NOT boats, kayaks, or surfboards. NOT distant background traffic; the vehicle must be prominent enough to be a real element of the photo.)
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

def tag_photo(image_url: str, beach_name: str, retries: int = 3
              ) -> tuple[dict, dict]:
    """Run two passes; return (tags, usage). Retries on transient errors
    including Wikimedia 429s during the base64 fetch."""
    last_err = None
    for attempt in range(retries + 1):
        try:
            desc, in1, out1 = _describe(image_url, beach_name)
            tags, in2, out2 = _extract(image_url, beach_name, desc)
            tags["description"] = desc
            tags["model"] = MODEL
            tags["schema_version"] = SCHEMA_VERSION
            tags["tagged_at"] = datetime.now(timezone.utc).isoformat()
            usage = {"input_tokens": in1 + in2, "output_tokens": out1 + out2}
            return tags, usage
        except Exception as e:
            last_err = e
            # Fast-fail on deterministic non-recoverable errors — no point
            # burning 3×exponential-backoff retries on a malformed URL or
            # an image we cannot resize.
            msg = str(e)
            if msg.startswith("bad_url:") or "image too large after resize" in msg:
                raise RuntimeError(f"non-recoverable: {msg}") from e
            # 429s (wikimedia or anthropic) need longer backoff
            is_429 = "429" in msg or "rate" in msg.lower()
            if attempt < retries:
                wait = (15 if is_429 else 3) * (2 ** attempt)  # 15/30/60 or 3/6/12
                print(f"    retry {attempt+1}/{retries} after {wait}s: {msg[:120]}",
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
    ap.add_argument("--workers", type=int, default=1,
                    help="Concurrent vision-tag workers (default 1; backfill should use 5-10)")
    ap.add_argument("--source", default=None,
                    help="Comma-separated list of bp.source values to limit to "
                         "(e.g. 'flickr' or 'flickr,ccc'). Lets two processes run "
                         "concurrently on disjoint sources — one fast (flickr URL-source) "
                         "and one slow (wikimedia base64 + serial lock).")
    ap.add_argument("--state", default=None,
                    help="Comma-separated state codes to limit beaches (e.g. CA,OR,WA). "
                         "Useful for MVP+-targeted backfill runs.")
    ap.add_argument("--fids", default=None,
                    help="Comma-separated beach fids — only tag photos for these "
                         "beaches. Used by rescue_reactivated_beaches.py so the "
                         "scoreboard accurately reflects this batch only.")
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
        # Idempotency: skip rows already tagged with the current model+schema.
        # Bumping SCHEMA_VERSION auto-invalidates v1 rows without manual ops.
        source_filter = ""
        source_params: tuple = ()
        if args.source:
            sources = [s.strip() for s in args.source.split(",") if s.strip()]
            source_filter = "and bp.source = any(%s)"
            source_params = (sources,)
        state_filter = ""
        if args.state:
            states = [s.strip().upper() for s in args.state.split(",") if s.strip()]
            state_filter = "and g.state = any(%s)"
            source_params = source_params + (states,)
        fid_filter = ""
        if args.fids:
            fid_list = [int(s) for s in args.fids.split(",") if s.strip()]
            fid_filter = "and bp.arena_group_id = any(%s)"
            source_params = source_params + (fid_list,)
        # Per Franz 2026-05-19 collapsed-architecture: every photo loaded
        # via the refactored Flickr/Wikimedia loaders has already passed
        # the unified v3 ingest filter. Pre-existing photos (ingested
        # under old per-source filters) get retroactively evaluated by
        # scripts/filter_non_curated_for_retag.py — rejects are marked
        # source_meta.v3_skipped=true. The WHERE clause excludes them.
        cur.execute(f"""
            select bp.id, bp.image_url, bp.thumb_url, bp.source_meta,
                   coalesce(g.display_name_override, g.name) as beach_name
              from public.beach_photos bp
              join public.beaches_gold g on g.fid = bp.arena_group_id
             where bp.image_url is not null
               {source_filter}
               {state_filter}
               {fid_filter}
               and (source_meta -> 'vision' ->> 'model' is null
                    or source_meta -> 'vision' ->> 'model' != '{MODEL}'
                    or coalesce(source_meta -> 'vision' ->> 'schema_version', 'v1') != '{SCHEMA_VERSION}')
               and coalesce(source_meta ->> 'v3_skipped', 'false') != 'true'
             order by bp.id
             limit %s
        """, source_params + (args.chunk_size,))
    targets = cur.fetchall()
    print(f"Targets: {len(targets)} photos", flush=True)

    est = len(targets) * COST_PER_PHOTO_USD
    print(f"Est cost: ${est:.2f}  (budget ${args.budget_usd})", flush=True)
    if est > args.budget_usd:
        print("  -> would exceed budget. Lower --chunk-size or raise --budget-usd.")
        sys.exit(1)

    upd_cur = conn.cursor()
    state = {"ok": 0, "err": 0, "in": 0, "out": 0, "done": 0}
    lock = threading.Lock()
    t0 = time.time()

    def process(p):
        url = p["thumb_url"] or p["image_url"]
        try:
            tags, usage = tag_photo(url, p["beach_name"] or "a beach")
        except Exception as e:
            with lock:
                state["err"] += 1
                state["done"] += 1
                pid = p["id"]
                d = state["done"]
            print(f"  [{d}/{len(targets)}] id={pid} ERR: {e}", flush=True)
            return None
        with lock:
            state["ok"] += 1
            state["in"] += usage["input_tokens"]
            state["out"] += usage["output_tokens"]
            state["done"] += 1
            d = state["done"]
        if args.audit:
            print(f"  [{d}/{len(targets)}] id={p['id']}  "
                  f"dog={tags.get('has_dog')}  birds={tags.get('has_birds')}  "
                  f"surf={tags.get('has_surfing')}  "
                  f"active={tags.get('has_active_people')}  "
                  f"face={tags.get('has_human_face_closeup')}  "
                  f"scene={tags.get('scene')}  q={tags.get('quality_issue')}  "
                  f"feat={tags.get('landscape_features')}", flush=True)
            print(f"      desc: {tags.get('description', '')[:160]}", flush=True)
            return None
        return (p["id"], tags, d)

    if args.workers <= 1:
        # Sequential path
        for p in targets:
            r = process(p)
            if r is None: continue
            pid, tags, d = r
            upd_cur.execute("""
                update public.beach_photos
                   set source_meta = source_meta || jsonb_build_object('vision', %s::jsonb)
                 where id = %s
            """, (json.dumps(tags), pid))
            if d % 20 == 0:
                conn.commit()
                _progress(state, len(targets), t0)
            time.sleep(0.15)
    else:
        # Parallel: workers pool, single DB writer in main thread.
        # Tag → result queue (via Future); main commits as they land.
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(process, p): p for p in targets}
            for fut in as_completed(futures):
                r = fut.result()
                if r is None: continue
                pid, tags, d = r
                upd_cur.execute("""
                    update public.beach_photos
                       set source_meta = source_meta || jsonb_build_object('vision', %s::jsonb)
                     where id = %s
                """, (json.dumps(tags), pid))
                if d % 20 == 0:
                    conn.commit()
                    _progress(state, len(targets), t0)

    if not args.audit:
        conn.commit()

    actual = state["in"] * 1e-6 + state["out"] * 5e-6   # Haiku 4.5 pricing
    print(f"\nDone in {time.time()-t0:.0f}s. ok={state['ok']} err={state['err']}",
          flush=True)
    print(f"Tokens: in={state['in']:,} out={state['out']:,}  est cost=${actual:.2f}",
          flush=True)


def _progress(state, total, t0):
    elapsed = time.time() - t0
    d = state["done"]
    rate = d / elapsed if elapsed > 0 else 0
    eta = (total - d) / rate if rate > 0 else 0
    print(f"  [{d}/{total}]  ok={state['ok']}  err={state['err']}  "
          f"{rate:.1f}/s  eta={eta:.0f}s  "
          f"tok in={state['in']:,} out={state['out']:,}", flush=True)


if __name__ == "__main__":
    main()
