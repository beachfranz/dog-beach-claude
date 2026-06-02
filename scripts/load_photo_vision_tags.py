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

import psycopg2
import psycopg2.extras

# Bootstrap repo root into sys.path so `from scripts.common.X import Y` works
# both when imported (`import scripts.X`) and when invoked as a script
# (`python scripts/X.py` — what `run_state_pipeline.py` does via subprocess).
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.common.db import connect
from scripts.common.llm import HAIKU
from scripts._photo_filters import ENTITIES

# Haiku 4.5 vision — cheapest capable vision model per env description.
MODEL = HAIKU
# Schema version — bump when the prompt/output shape changes so old rows
# get re-tagged automatically without manual clearing.
#   v1: initial schema
#   v2: added has_surfing + has_active_people, dropped "people" from subjects
#   v3: added has_path + has_vehicle (Franz 2026-05-19 photo curation v3)
#   v4: DP-only — added is_dog_park_relevant (2026-06-01); beach stays v3.
SCHEMA_VERSION_BY_ENTITY = {"beach": "v3", "dog_park": "v4"}
# Back-compat default (call sites that don't know the entity).
SCHEMA_VERSION = "v3"

def schema_version_for(entity: str) -> str:
    return SCHEMA_VERSION_BY_ENTITY.get(entity, SCHEMA_VERSION)

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
    """Return image-source dict. Two paths:

    - URL pointer: for known-Anthropic-friendly hosts (Flickr/Unsplash
      static CDNs). Anthropic fetches the image; cheaper bandwidth on
      our side.
    - Bytes-on-our-side base64: default for every other host. Bypasses
      Cloudflare-blocked origins (Yelp/FB/YT/Reddit/bringfido etc) that
      Anthropic's URL fetcher returns 400 'Unable to download' on. We
      already have AVG + certs working in our Python; Anthropic accepts
      base64 of any host's content. Wikimedia + parks.wa.gov keep their
      tuned special-case branches below.

    Raises RuntimeError('bad_url') early for malformed URLs (no protocol,
    HTTP-only, or non-URL strings) so we don't burn 3 Anthropic retries
    on data-quality issues."""
    if not image_url or not isinstance(image_url, str):
        raise RuntimeError("bad_url: empty")
    if not image_url.startswith("https://"):
        raise RuntimeError(f"bad_url: not https ({image_url[:60]})")
    url = _strip_utm(image_url)
    # URL-pointer allowlist (Franz 2026-05-26 path 2). Hosts proven to
    # work with Anthropic's URL fetcher. Everything else falls through
    # to the bytes-on-our-side default at the end of this function.
    URL_POINTER_HOSTS = {"live.staticflickr.com", "images.unsplash.com"}
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

    # URL-pointer fast path for proven-OK hosts.
    try:
        host = url.split("/", 3)[2].lower()
    except Exception:
        host = ""
    if host in URL_POINTER_HOSTS:
        return {"type": "url", "url": url}

    # Default path (Franz 2026-05-26 path 2): bytes-on-our-side base64.
    # Bypasses Cloudflare-blocked origins that Anthropic's URL fetcher
    # rejects. Uses browser UA + the same PIL resize cascade as the
    # parks.wa.gov branch so we stay under the 5MB base64 cap.
    browser_ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/130.0.0.0 Safari/537.36")
    req = urllib.request.Request(url, headers={
        "User-Agent": browser_ua,
        "Accept": "image/avif,image/webp,image/jpeg,image/png,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            data = r.read()
            ct = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
    except urllib.error.HTTPError as e:
        # Origin blocked us too — fail fast as non-recoverable so the
        # retry loop in tag_photo doesn't burn 3×backoff on a dead URL.
        raise RuntimeError(f"bad_url: HTTP {e.code} from {host}") from e
    except Exception as e:
        raise RuntimeError(f"bad_url: fetch failed ({type(e).__name__}: {e})") from e

    # Same RAW_CAP + PIL resize cascade as parks.wa.gov branch.
    RAW_CAP = 3 * 1024 * 1024 + 512 * 1024   # 3.5 MB raw → ~4.67 MB b64
    if len(data) > RAW_CAP:
        from PIL import Image
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
            raise RuntimeError(f"image too large after resize: {len(data)} bytes raw")
    return {"type": "base64", "media_type": ct,
            "data": base64.b64encode(data).decode()}


def _describe(image_url: str, entity_name: str, entity_label: str = "beach") -> tuple[str, int, int]:
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
                    f"This photo is supposedly of {entity_name}. "
                    "Describe what you see in 1-2 plain sentences. Focus on "
                    "the main subject, what's happening, and the setting. "
                    "Do not use markdown headers, do not speculate about "
                    f"whether it's actually that {entity_label}."
                 )},
            ],
        }],
    )
    text = "".join(b.text for b in msg.content if b.type == "text").strip()
    return text, msg.usage.input_tokens, msg.usage.output_tokens


def _extract_prompt_for(entity: str) -> str:
    label = "dog park" if entity == "dog_park" else "beach"
    # DP-only fields (added v4, 2026-06-01). Cleaner signal than the
    # scene-gymnastics that read fenced enclosures as "interior".
    dp_extra = (
        "  is_dog_park_relevant: true|false  (does this photo show a recognizable "
        "off-leash dog area — fenced enclosure, dog-friendly grass field, agility "
        "equipment, paw-themed signage, dogs playing in a park setting, etc. "
        "A close-up portrait of just a dog with no surrounding context = false. "
        "A street view of a building, sign, or map = false. A photo from a "
        "non-park location like a dog show or pet store = false.)\n"
        if entity == "dog_park" else ""
    )
    return f"""You are tagging a {label} photo for a search/sort system.
Given the photo and an initial description, return a JSON object with these fields:

  has_dog: true|false  (dogs, puppies — any breed)
{dp_extra}  has_birds: true|false  (shorebirds, gulls, raptors, any bird — but not when they are a tiny dot in the distance)
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


# Back-compat: keep the module-level constant for any direct importers.
_EXTRACT_PROMPT = _extract_prompt_for("beach")


def _extract(image_url: str, entity_name: str, description: str,
             entity_label: str = "beach") -> tuple[dict, int, int]:
    """Pass 2: structured extraction. Returns (parsed, in_tok, out_tok)."""
    label_titlecase = entity_label.title()   # "Beach" / "Dog Park"
    entity_key = "dog_park" if entity_label == "dog park" else "beach"
    msg = _client.messages.create(
        model=MODEL,
        max_tokens=400,
        messages=[{
            "role": "user",
            "content": [
                {"type": "image", "source": _image_source(image_url)},
                {"type": "text",
                 "text": (
                    f"{label_titlecase} context: {entity_name}\n"
                    f"Initial description: {description}\n\n"
                    + _extract_prompt_for(entity_key)
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


# ─── Batched (multi-image per Haiku call) ─────────────────────────────────

def _parse_json_array(raw: str) -> list:
    if raw.startswith("```"):
        raw = raw.strip("`").lstrip("json").strip()
    return json.loads(raw)


def _describe_batch(specs: list[dict]) -> tuple[list[str], int, int]:
    """Pass 1 batched: one Haiku call describes N images.
    specs = [{'url':..., 'name':..., 'label':...}, ...]
    Returns (descriptions_in_order, in_tok, out_tok)."""
    content: list[dict] = []
    for i, s in enumerate(specs):
        content.append({"type": "text",
                        "text": f"[Image {i}] supposedly of {s['name']}:"})
        content.append({"type": "image", "source": _image_source(s["url"])})
    content.append({"type": "text", "text": (
        f"Above are {len(specs)} images, indexed 0..{len(specs)-1}. "
        "For each image, write a 1-2 sentence plain description focused on "
        "the main subject and setting. Do not speculate about whether each is "
        "actually the location it's supposedly of. "
        f"Return ONLY a JSON array of {len(specs)} objects in order, "
        '[{"i":0,"description":"..."}, {"i":1,"description":"..."}, ...]. '
        "No prose, no markdown fences.")})
    msg = _client.messages.create(
        model=MODEL,
        max_tokens=140 * len(specs),
        messages=[{"role": "user", "content": content}],
    )
    raw = "".join(b.text for b in msg.content if b.type == "text").strip()
    arr = _parse_json_array(raw)
    if not isinstance(arr, list) or len(arr) != len(specs):
        raise RuntimeError(f"describe_batch returned {len(arr) if isinstance(arr,list) else type(arr).__name__}, expected {len(specs)}")
    descs = [str(o.get("description", "")).strip() for o in arr]
    return descs, msg.usage.input_tokens, msg.usage.output_tokens


def _extract_batch(specs: list[dict], descriptions: list[str]
                   ) -> tuple[list[dict], int, int]:
    """Pass 2 batched: one Haiku call returns structured tags for N images."""
    content: list[dict] = []
    for i, s in enumerate(specs):
        label_tc = s["label"].title()
        content.append({"type": "text", "text": (
            f"[Image {i}] {label_tc} context: {s['name']}. "
            f"Initial description: {descriptions[i]}")})
        content.append({"type": "image", "source": _image_source(s["url"])})
    # All specs in a batch share the same entity (batches are built per-run).
    batch_entity = ("dog_park" if specs and specs[0].get("label") == "dog park"
                    else "beach")
    content.append({"type": "text", "text": (
        f"Above are {len(specs)} images, each with context + initial description. "
        + _extract_prompt_for(batch_entity) + "\n\n"
        f"Return ONLY a JSON array of {len(specs)} objects in image order, "
        '[{"i":0, ...tag fields...}, {"i":1, ...}, ...]. No prose, no markdown fences.')})
    msg = _client.messages.create(
        model=MODEL,
        max_tokens=420 * len(specs),
        messages=[{"role": "user", "content": content}],
    )
    raw = "".join(b.text for b in msg.content if b.type == "text").strip()
    arr = _parse_json_array(raw)
    if not isinstance(arr, list) or len(arr) != len(specs):
        raise RuntimeError(f"extract_batch returned {len(arr) if isinstance(arr,list) else type(arr).__name__}, expected {len(specs)}")
    return arr, msg.usage.input_tokens, msg.usage.output_tokens


def tag_batch(specs: list[dict], retries: int = 2
              ) -> list[tuple[dict | None, dict | None, str | None]]:
    """Tag a batch of N photos in 2 Haiku calls (vs 2N).
    Returns list aligned to specs: (tags, usage, err_msg).
    On batch failure, falls back to per-photo tag_photo() for each spec."""
    last_err = None
    for attempt in range(retries + 1):
        try:
            descs, d_in, d_out = _describe_batch(specs)
            tags_list, e_in, e_out = _extract_batch(specs, descs)
            out: list[tuple[dict | None, dict | None, str | None]] = []
            # Split token usage evenly across N — approximation.
            per_in = (d_in + e_in) // max(1, len(specs))
            per_out = (d_out + e_out) // max(1, len(specs))
            for i, s in enumerate(specs):
                tags = tags_list[i]
                tags["description"] = descs[i]
                tags["model"] = MODEL
                tags["schema_version"] = schema_version_for(
                    "dog_park" if s.get("label") == "dog park" else "beach"
                )
                tags["tagged_at"] = datetime.now(timezone.utc).isoformat()
                out.append((tags,
                            {"input_tokens": per_in, "output_tokens": per_out},
                            None))
            return out
        except Exception as e:
            last_err = e
            msg_s = str(e)
            # Non-recoverable on any image → can't retry batch; fall back to per-photo.
            if (msg_s.startswith("bad_url:")
                    or "image too large" in msg_s
                    or "exceeds 5 MB" in msg_s):
                break
            if attempt < retries:
                wait = 3 * (2 ** attempt)
                print(f"    batch retry {attempt+1}/{retries} after {wait}s: {msg_s[:120]}",
                      flush=True)
                time.sleep(wait)
    # Fall back to per-photo so one bad image doesn't sink the rest.
    print(f"    batch failed ({last_err}) — falling back to per-photo",
          flush=True)
    out = []
    for s in specs:
        try:
            tags, usage = tag_photo(s["url"], s["name"], entity_label=s["label"])
            out.append((tags, usage, None))
        except Exception as e:
            out.append((None, None, str(e)))
    return out


# ─── Per-photo driver ─────────────────────────────────────────────────────

def tag_photo(image_url: str, entity_name: str, retries: int = 3,
              entity_label: str = "beach") -> tuple[dict, dict]:
    """Run two passes; return (tags, usage). Retries on transient errors
    including Wikimedia 429s during the base64 fetch."""
    last_err = None
    for attempt in range(retries + 1):
        try:
            desc, in1, out1 = _describe(image_url, entity_name, entity_label)
            tags, in2, out2 = _extract(image_url, entity_name, desc, entity_label)
            tags["description"] = desc
            tags["model"] = MODEL
            tags["schema_version"] = schema_version_for(
                "dog_park" if entity_label == "dog park" else "beach"
            )
            tags["tagged_at"] = datetime.now(timezone.utc).isoformat()
            usage = {"input_tokens": in1 + in2, "output_tokens": out1 + out2}
            return tags, usage
        except Exception as e:
            last_err = e
            # Fast-fail on deterministic non-recoverable errors — no point
            # burning 3×exponential-backoff retries on a malformed URL or
            # an image we cannot resize.
            msg = str(e)
            # Non-recoverable: malformed URL, oversized image (local or
            # upstream API rejection at 5MB cap). All deterministic — no
            # point burning 3x retries. Wikimedia originals up to ~10MB
            # come back oversized even at width=500.
            if (msg.startswith("bad_url:")
                    or "image too large after resize" in msg
                    or "too large even at width=" in msg
                    or "exceeds 5 MB maximum" in msg
                    or "exceeds 5 MB" in msg):
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
    ap.add_argument("--entity", default="beach", choices=["beach", "dog_park"],
                    help="Entity type. Default: beach (back-compat).")
    ap.add_argument("--audit", help="Comma-separated fids — print tags, no write")
    ap.add_argument("--chunk-size", type=int, default=50,
                    help="Photos per run (default 50)")
    ap.add_argument("--no-skip-existing", action="store_true",
                    help="Reprocess photos that already have vision tags")
    ap.add_argument("--require-lat", dest="require_lat", action="store_true",
                    default=True,
                    help="Skip photos where lat IS NULL (default: on). For non-geo "
                         "sources (websearch, unsplash), lat=NULL means the photo "
                         "failed tight_name_match at load time and won't reach the "
                         "consumer surface — tagging it wastes Haiku spend. Geo "
                         "sources (Flickr, Wikimedia) always have lat, so this is "
                         "a no-op for them.")
    ap.add_argument("--no-require-lat", dest="require_lat", action="store_false",
                    help="Tag ALL photos including name-match orphans. Use when "
                         "building a complete tag dataset for inspection / analytics.")
    ap.add_argument("--budget-usd", type=float, default=DEFAULT_BUDGET_USD,
                    help=f"Hard stop if est cost exceeds (default ${DEFAULT_BUDGET_USD})")
    ap.add_argument("--workers", type=int, default=1,
                    help="Concurrent vision-tag workers (default 1; backfill should use 5-10)")
    ap.add_argument("--batch-size", type=int, default=4,
                    help="Photos per Haiku call (default 4). 1=legacy per-photo. "
                         "Batching turns 2N calls into 2 calls per N photos = ~Nx wall-clock "
                         "speedup. Falls back to per-photo on batch failure.")
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
    ap.add_argument("--include-v3-skipped", action="store_true",
                    help="Override the v3_skipped sentinel filter. Default is to "
                         "skip photos marked v3_skipped=true (cost-saving for "
                         "non-curated MVP+ per task #76). Pass this to force "
                         "tagging — e.g. NPS backfill 2026-05-20.")
    args = ap.parse_args()

    ent = ENTITIES[args.entity]
    photo_table = ent["photo_table"]
    fk_col      = ent["fk_col"]
    gold_table  = ent["table"]
    entity_label = "dog park" if args.entity == "dog_park" else "beach"
    print(f"[vision] entity={args.entity}  photo_table={photo_table}  gold={gold_table}", flush=True)

    conn = connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("set statement_timeout = '120s'")

    # Build target set.
    if args.audit:
        fids = [int(x) for x in args.audit.split(",")]
        cur.execute(f"""
            select bp.id, bp.image_url, bp.thumb_url, bp.source_meta,
                   coalesce(g.display_name_override, g.name) as entity_name
              from public.{photo_table} bp
              join public.{gold_table} g on g.fid = bp.{fk_col}
             where bp.{fk_col} = any(%s)
             order by bp.{fk_col}, bp.sort_order
        """, (fids,))
    else:
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
            fid_filter = f"and bp.{fk_col} = any(%s)"
            source_params = source_params + (fid_list,)
        v3_skipped_filter = ("" if args.include_v3_skipped
                              else "and coalesce(source_meta ->> 'v3_skipped', 'false') != 'true'")
        # Per-entity SCHEMA_VERSION so bumping the DP version doesn't
        # invalidate beach photos (and vice-versa).
        current_schema = schema_version_for(args.entity)
        # Per Franz 2026-06-02: by default skip photos with lat IS NULL.
        # For non-geo sources (websearch/unsplash) these are name-match
        # orphans that can't reach the consumer surface; tagging them
        # is wasted Haiku spend. Geo sources (Flickr/Wikimedia) always
        # have lat so this is a no-op for them.
        lat_filter = "and bp.lat is not null" if args.require_lat else ""
        cur.execute(f"""
            select bp.id, bp.image_url, bp.thumb_url, bp.source_meta,
                   coalesce(g.display_name_override, g.name) as entity_name
              from public.{photo_table} bp
              join public.{gold_table} g on g.fid = bp.{fk_col}
             where bp.image_url is not null
               {source_filter}
               {state_filter}
               {fid_filter}
               {lat_filter}
               and (source_meta -> 'vision' ->> 'model' is null
                    or source_meta -> 'vision' ->> 'model' != '{MODEL}'
                    or coalesce(source_meta -> 'vision' ->> 'schema_version', 'v1') != '{current_schema}')
               {v3_skipped_filter}
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

    # Workers do ~1-3s API calls; while they're out, the DB conn idles
    # behind pgbouncer's timeout and gets cut. _safe_update reconnects
    # silently on OperationalError + retries the write. Diagnosed 2026-05-19.
    nonlocal_state = {"conn": conn, "cur": upd_cur}
    def _safe_update(tags, pid):
        sql = (f"update public.{photo_table} "
               "   set source_meta = source_meta || jsonb_build_object('vision', %s::jsonb) "
               " where id = %s")
        params = (json.dumps(tags), pid)
        try:
            nonlocal_state["cur"].execute(sql, params)
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            try: nonlocal_state["conn"].close()
            except Exception: pass
            nonlocal_state["conn"] = connect()
            nonlocal_state["cur"] = nonlocal_state["conn"].cursor()
            nonlocal_state["cur"].execute(sql, params)

    def process_one(p):
        """Single-photo path (used for batch_size=1 + audit mode)."""
        url = p["thumb_url"] or p["image_url"]
        try:
            tags, usage = tag_photo(url, p["entity_name"] or f"a {entity_label}", entity_label=entity_label)
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
        return [(p["id"], tags, d)]

    def process_batch(batch):
        """Batched path: N photos → 2 Haiku calls. Returns list of (pid, tags, d_idx)
        for successful tags, plus side-effects to state for errors."""
        specs = [{"url": (p["thumb_url"] or p["image_url"]),
                  "name": p["entity_name"] or f"a {entity_label}",
                  "label": entity_label} for p in batch]
        results = tag_batch(specs)
        out: list[tuple] = []
        for p, (tags, usage, err) in zip(batch, results):
            with lock:
                if err is not None:
                    state["err"] += 1
                    state["done"] += 1
                    d = state["done"]
                    print(f"  [{d}/{len(targets)}] id={p['id']} ERR: {err}", flush=True)
                else:
                    state["ok"] += 1
                    state["in"] += usage["input_tokens"]
                    state["out"] += usage["output_tokens"]
                    state["done"] += 1
                    d = state["done"]
            if tags is None:
                continue
            if args.audit:
                print(f"  [{d}/{len(targets)}] id={p['id']}  "
                      f"dog={tags.get('has_dog')}  scene={tags.get('scene')}  "
                      f"q={tags.get('quality_issue')}", flush=True)
                continue
            out.append((p["id"], tags, d))
        return out

    bs = max(1, args.batch_size)
    if bs == 1:
        # Legacy per-photo path
        if args.workers <= 1:
            for p in targets:
                r = process_one(p)
                if not r: continue
                pid, tags, d = r[0]
                _safe_update(tags, pid)
                if d % 20 == 0:
                    nonlocal_state["conn"].commit()
                    _progress(state, len(targets), t0)
                time.sleep(0.15)
        else:
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                futures = {pool.submit(process_one, p): p for p in targets}
                for fut in as_completed(futures):
                    r = fut.result()
                    if not r: continue
                    pid, tags, d = r[0]
                    _safe_update(tags, pid)
                    if d % 20 == 0:
                        nonlocal_state["conn"].commit()
                        _progress(state, len(targets), t0)
    else:
        # Batched path: group targets into chunks of bs, dispatch.
        print(f"[vision] batch_size={bs} → ~{(len(targets)+bs-1)//bs} batches", flush=True)
        batches = [targets[i:i+bs] for i in range(0, len(targets), bs)]
        if args.workers <= 1:
            for b in batches:
                results = process_batch(b)
                for pid, tags, d in results:
                    _safe_update(tags, pid)
                nonlocal_state["conn"].commit()
                _progress(state, len(targets), t0)
        else:
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                futures = {pool.submit(process_batch, b): b for b in batches}
                for fut in as_completed(futures):
                    results = fut.result()
                    for pid, tags, d in results:
                        _safe_update(tags, pid)
                    nonlocal_state["conn"].commit()
                    _progress(state, len(targets), t0)

    if not args.audit:
        nonlocal_state["conn"].commit()

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
