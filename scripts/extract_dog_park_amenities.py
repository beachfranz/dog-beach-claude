"""
extract_dog_park_amenities.py — codify-pattern port for dog park per-park amenities.

Phase DP-B per [[codify-patterns-beyond-statutes]]. For each active dog park
with an OSM `website=` tag (~394 in CA today), fetch the URL, gate the page
on park-name match, and LLM-extract amenities + leash + hours per-field with
cite-required verbatim quotes.

Writes to `dog_park_enrichment_provenance` (one row per field_group), then
fires consensus + promote per park to land canonical claims into
`dog_park_dog_policy`. Sentinel row on no-match / fetch-fail.

Reuses smart_fetch + AUTH_DOMAINS + DEEPLINK_MARKERS + sentinel pattern from
extract_per_beach_offleash_v2.py (the codify substrate).

Usage:
  python scripts/extract_dog_park_amenities.py --dry-run         # default
  python scripts/extract_dog_park_amenities.py --apply --limit 5
  python scripts/extract_dog_park_amenities.py --apply --fids 2034,1592
  python scripts/extract_dog_park_amenities.py --apply           # all 394 OSM-website parks
"""
from __future__ import annotations
import argparse, json, os, re, sys, time, threading
import urllib.parse, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import psycopg2.extras
from anthropic import Anthropic

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.common.db import connect
from scripts.common.llm import SONNET
from scripts.extract_research_v2 import strip_html, name_match
from scripts.extract_per_beach_offleash_v2 import (
    smart_fetch, is_thin_or_blocked, is_url_deep_enough, authority_score,
)

ANTHROPIC = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]


# ── LLM prompt — single flat JSON envelope, 15 fields, one cite ──────────
# Refined 2026-05-25 LATE per Franz's working-backward pass: dog parks are
# off-leash by definition (leash fields not extracted); descriptive prose +
# surface added per consumer surface needs.

PROMPT_SYSTEM = (
    "You are extracting dog-park information from a single operator-posted "
    "source page. Use ONLY the provided source content. For each field: if "
    "the page does NOT clearly state it, return null. Do NOT infer; do NOT "
    "guess. If the page does not mention the park by name (or its commonly-"
    "known abbreviation), return all-nulls with name_match=false.\n\n"
    "Honest-prose rule for description: use specific nouns. Name what IS "
    "there AND what isn't. Banned filler: 'amenities', 'facilities', 'the "
    "data', 'source page'. 1-3 sentences max."
)

PROMPT_USER_TEMPLATE = """SOURCE PAGE CONTENT:
{content}

PARK: {name} ({city}, CA)

Extract the following 13 fields about THIS specific dog park. Return null if not stated. For booleans, only true/false when the page explicitly says so.

Return JSON exactly in this flat shape (no nested blocks):
{{
  "name_match":         <true|false: does the page actually mention this park by name?>,
  "hours_text":         <text like "6am-10pm daily" or "Dawn to dusk" or null>,
  "hours_open_time":    <"HH:MM" 24h or null>,
  "hours_close_time":   <"HH:MM" 24h or null>,
  "additional_rules":   <1-2 sentences of park-specific rules (e.g. "max 3 dogs/person; no female dogs in heat"), else null>,
  "has_fence":          <true|false|null>,
  "has_drinking_water": <true|false|null>,
  "double_gate":        <true|false|null>,
  "small_dog_area":     <true|false|null>,
  "large_dog_area":     <true|false|null>,
  "lighting":           <true|false|null>,
  "surface":            <"grass"|"dirt"|"wood_chips"|"sand"|"decomposed_granite"|"mixed"|"artificial_turf"|null>,
  "description":        <1-3 sentences of honest descriptive prose per the rule above; null if nothing specific to say>,
  "cite_quote":         <ONE verbatim quote 50-300 chars supporting the above; MUST contain park name; null if no evidence>,
  "confidence":         "high" | "medium" | "low"
}}"""


def call_llm_amenities(content: str, name: str, city: str | None) -> dict:
    user = PROMPT_USER_TEMPLATE.format(
        content=content[:30000],
        name=name,
        city=city or "California",
    )
    body = {
        "model": SONNET,
        "max_tokens": 1500,
        "system": PROMPT_SYSTEM,
        "messages": [{"role": "user", "content": user}],
    }
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode("utf-8"), method="POST",
    )
    req.add_header("x-api-key", ANTHROPIC_KEY)
    req.add_header("anthropic-version", "2023-06-01")
    req.add_header("content-type", "application/json")
    with urllib.request.urlopen(req, timeout=120) as r:
        resp = json.loads(r.read().decode("utf-8"))
    text_blocks = [b.get("text", "") for b in resp.get("content", []) if b.get("type") == "text"]
    raw = (text_blocks[-1] if text_blocks else "").strip()
    if raw.startswith("```"):
        parts = raw.split("```", 2)
        if len(parts) >= 2:
            inner = parts[1]
            if inner.lower().startswith("json"):
                inner = inner[4:].lstrip()
            raw = inner.rsplit("```", 1)[0].strip()
    if not raw.startswith("{"):
        i, j = raw.find("{"), raw.rfind("}")
        if i >= 0 and j > i:
            raw = raw[i:j+1]
    try:
        return json.loads(raw)
    except Exception:
        return {"name_match": False, "_parse_error": raw[:300]}


# ── DPEP writes ─────────────────────────────────────────────────────────

def write_park_v1(cur, park_fid: int, source_url: str, confidence: float,
                  claimed: dict, cite: str | None, extraction_method: str) -> None:
    """Single dpep row per park — field_group='park_v1', 13 fields in one
    jsonb. Refined v2 shape (no per-field_group split for v1 — one source
    per park, no consensus needed)."""
    cur.execute("""
        INSERT INTO public.dog_park_enrichment_provenance
            (dog_park_fid, field_group, source, source_url, confidence,
             is_canonical, relevance_verified, claimed_values, cite_quote,
             extraction_method, notes, updated_at)
        VALUES (%s, 'park_v1', 'per_park_amenities_v1', %s, %s,
                true, true, %s::jsonb, %s, %s,
                'extract_dog_park_amenities.py — OSM-website-tag fast pass', now())
        ON CONFLICT (dog_park_fid, field_group, source) DO UPDATE
          SET source_url        = EXCLUDED.source_url,
              confidence        = EXCLUDED.confidence,
              claimed_values    = EXCLUDED.claimed_values,
              cite_quote        = EXCLUDED.cite_quote,
              extraction_method = EXCLUDED.extraction_method,
              is_canonical      = true,
              relevance_verified= true,
              updated_at        = now()
    """, (park_fid, source_url, confidence,
          json.dumps(claimed), cite, extraction_method))


def write_sentinel(cur, park_fid: int, url: str, why: str) -> None:
    claimed = {"status": "no_url_found", "why": why,
               "extracted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    cur.execute("""
        INSERT INTO public.dog_park_enrichment_provenance
            (dog_park_fid, field_group, source, source_url, confidence,
             is_canonical, relevance_verified, claimed_values, notes,
             extraction_method, updated_at)
        VALUES (%s, 'park_v1', 'per_park_amenities_v1_no_result', %s, 0.10,
                false, false, %s::jsonb,
                'extract_dog_park_amenities.py sentinel; OSM website did not pan out',
                'smart_fetch', now())
        ON CONFLICT (dog_park_fid, field_group, source) DO UPDATE
          SET claimed_values = EXCLUDED.claimed_values,
              source_url     = EXCLUDED.source_url,
              updated_at     = now()
    """, (park_fid, url, json.dumps(claimed)))


# ── Per-park worker (thread-safe; opens its own conn) ──────────────────

PRINT_LOCK = threading.Lock()


def process_park(p: dict, apply: bool) -> str:
    """Process one park end-to-end. Returns outcome key for summary tally.
    Opens + closes its own DB connection (psycopg2 conns are not thread-safe)."""
    fid, name = p['fid'], p['name']
    city = p.get('address_city')
    url  = p['osm_website']

    def say(msg: str) -> None:
        with PRINT_LOCK:
            print(msg, flush=True)

    say(f"\n[{fid}] {name}  city={city or '-'}  url={url[:70]}")

    if not is_url_deep_enough(url):
        say(f"    [-] [{fid}] not deep enough")
        if apply:
            conn = connect(); conn.set_client_encoding("UTF8")
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                write_sentinel(cur, fid, url, "url_not_deep_enough"); conn.commit()
            finally:
                conn.close()
        return "skipped_shallow"

    text_raw, why = smart_fetch(url)
    if text_raw is None:
        say(f"    [-] [{fid}] fetch fail: {why}")
        if apply:
            conn = connect(); conn.set_client_encoding("UTF8")
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                write_sentinel(cur, fid, url, f"fetch_fail:{why}"); conn.commit()
            finally:
                conn.close()
        return "fetch_fail"

    if is_thin_or_blocked(text_raw):
        say(f"    [-] [{fid}] thin/blocked ({len(text_raw)} chars)")
        if apply:
            conn = connect(); conn.set_client_encoding("UTF8")
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                write_sentinel(cur, fid, url, f"thin_or_blocked:{len(text_raw)}"); conn.commit()
            finally:
                conn.close()
        return "thin"

    text = strip_html(text_raw) if "<" in text_raw else text_raw[:60000]

    if not name_match(text, name):
        say(f"    [-] [{fid}] name not on page")
        if apply:
            conn = connect(); conn.set_client_encoding("UTF8")
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                write_sentinel(cur, fid, url, "name_not_on_page"); conn.commit()
            finally:
                conn.close()
        return "no_match"

    try:
        extracted = call_llm_amenities(text, name, city)
    except Exception as e:
        say(f"    [!] [{fid}] LLM error: {e}")
        return "llm_error"

    if not extracted.get('name_match'):
        say(f"    [-] [{fid}] LLM says name_match=false")
        if apply:
            conn = connect(); conn.set_client_encoding("UTF8")
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                write_sentinel(cur, fid, url, "llm_says_name_not_on_page"); conn.commit()
            finally:
                conn.close()
        return "no_match"

    # Success path — extract + write + cascade
    conf = {'high': 0.92, 'medium': 0.78, 'low': 0.60}.get(
               extracted.get('confidence', 'medium'), 0.78)
    cite = extracted.pop('cite_quote', None)
    extracted.pop('name_match', None)
    extracted.pop('confidence', None)
    claimed = {k: v for k, v in extracted.items() if v is not None}
    say(f"    [+] [{fid}] {len(claimed)}/13 fields: {sorted(claimed.keys())}")

    if apply and claimed:
        conn = connect(); conn.set_client_encoding("UTF8")
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            write_park_v1(cur, fid, url, conf, claimed, cite, 'smart_fetch')
            conn.commit()
            cur.execute("SELECT public.promote_canonical_dog_park_policy(%s)", (fid,))
            conn.commit()
        finally:
            conn.close()
    return "extracted" if claimed else "empty"


# ── Main ────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--fids", type=str, default=None,
                    help="Comma-separated dog_park fids to target (overrides default work-set)")
    ap.add_argument("--all-states", action="store_true",
                    help="Default is CA-only; pass this to include all states")
    ap.add_argument("--workers", type=int, default=6,
                    help="Parallel worker threads. Cap at <15 per [[supabase-pool-cap-vs-dagster-concurrency]]; default 6.")
    args = ap.parse_args()

    if args.workers > 12:
        print(f"WARN: --workers={args.workers} risks Supabase pooler exhaustion (cap 15)")

    conn = connect()
    conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if args.fids:
        target_fids = [int(x.strip()) for x in args.fids.split(",")]
        cur.execute("""
            SELECT dpg.fid, dpg.name, dpg.address_city, dpg.website AS osm_website
              FROM public.dog_parks_gold dpg
             WHERE dpg.fid = ANY(%s) AND dpg.website IS NOT NULL
        """, (target_fids,))
    else:
        state_filter = " AND state = 'CA' " if not args.all_states else ""
        cur.execute(f"""
            SELECT fid, name, address_city, osm_website
              FROM public.dog_parks_active_unsourced
             WHERE osm_website IS NOT NULL {state_filter}
             ORDER BY fid
        """ + (f" LIMIT {args.limit}" if args.limit else ""))
    parks = [dict(r) for r in cur.fetchall()]
    conn.close()

    print(f"OSM-website parks targeted: {len(parks)}; apply={args.apply}; workers={args.workers}")

    summary = {"extracted": 0, "no_match": 0, "fetch_fail": 0,
               "thin": 0, "parse_error": 0, "skipped_shallow": 0,
               "llm_error": 0, "empty": 0}

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(process_park, p, args.apply) for p in parks]
        for fut in as_completed(futures):
            try:
                outcome = fut.result()
            except Exception as e:
                with PRINT_LOCK:
                    print(f"[!] worker raised: {e}", flush=True)
                outcome = "llm_error"
            summary[outcome] = summary.get(outcome, 0) + 1

    print(f"\n{'='*60}\nSUMMARY:")
    for k, v in summary.items():
        print(f"  {k:18} = {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
