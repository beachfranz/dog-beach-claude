"""
retry_dog_park_no_match.py — Path D query-variant retry for web_search no_match parks.

Previous web_search pass returned name_match=false for ~65 CA parks. Many
are specific named parks (Rohr Dog Park, Memorial Community Park Dog Park,
Fiddler's Cove Dog Run) where Sonnet just needs a stronger nudge to try
multiple query angles. Some are too-generic ("Small Dog Park", "Green Belt")
where retry won't help — sentinel stays.

This script uses a more elaborate prompt that tells Sonnet to try at least
2 query variants before giving up.

Usage:
  python scripts/retry_dog_park_no_match.py --apply
  python scripts/retry_dog_park_no_match.py --apply --limit 5
"""
from __future__ import annotations
import argparse, json, os, sys, threading
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2.extras

# Force UTF-8 stdout on Windows cp1252
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.common.db import connect
from scripts.common.llm import SONNET
from scripts.extract_dog_park_amenities import (
    PROMPT_SYSTEM, write_park_v1, write_sentinel,
)

ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
PRINT_LOCK = threading.Lock()


RETRY_PROMPT_USER = """No authoritative source was found in the prior search for this park. Try AGAIN with multiple web_search queries (≥2 distinct phrasings) before giving up.

PARK: {name}
CITY: {city}
STATE: California

Suggested query angles (use at least 2):
  1. "{name}" {city} dog park rules hours
  2. {city} parks recreation "{name}" off-leash
  3. site:.gov "{name}" {city} dog
  4. "{name}" {city} CA fence water hours

If after multiple searches you still can't find an AUTHORITATIVE source (operator's .gov / .org site, NOT BringFido/Yelp/aggregators), return name_match=false. If the park name is too generic ("Dog Park", "Small Dog Park", "Green Belt"), name_match=false is the right call.

Return JSON exactly in this flat shape:
{{
  "name_match":         <true|false>,
  "hours_text":         <text or null>,
  "hours_open_time":    <"HH:MM" 24h or null>,
  "hours_close_time":   <"HH:MM" 24h or null>,
  "additional_rules":   <1-2 sentences or null>,
  "has_fence":          <true|false|null>,
  "has_drinking_water": <true|false|null>,
  "double_gate":        <true|false|null>,
  "small_dog_area":     <true|false|null>,
  "large_dog_area":     <true|false|null>,
  "lighting":           <true|false|null>,
  "surface":            <enum or null>,
  "description":        <1-3 sentences honest prose, or null>,
  "cite_quote":         <verbatim quote 50-300 chars containing the park name, or null>,
  "confidence":         "high" | "medium" | "low"
}}"""


def call_retry_llm(name: str, city: str | None) -> dict:
    user = RETRY_PROMPT_USER.format(name=name, city=city or "California")
    body = {
        "model": SONNET,
        "max_tokens": 1500,
        "system": PROMPT_SYSTEM,
        "messages": [{"role": "user", "content": user}],
        "tools": [{"type": "web_search_20250305",
                   "name": "web_search", "max_uses": 5}],  # 5 vs default 3
    }
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode("utf-8"), method="POST",
    )
    req.add_header("x-api-key", ANTHROPIC_KEY)
    req.add_header("anthropic-version", "2023-06-01")
    req.add_header("content-type", "application/json")
    with urllib.request.urlopen(req, timeout=240) as r:
        resp = json.loads(r.read().decode("utf-8"))
    text_blocks = [b.get("text", "") for b in resp.get("content", []) if b.get("type") == "text"]
    raw = (text_blocks[-1] if text_blocks else "").strip()
    import re
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


def process(park: dict, apply: bool) -> str:
    fid, name, city = park['fid'], park['name'], park.get('address_city')

    def say(msg: str) -> None:
        with PRINT_LOCK:
            print(msg, flush=True)

    say(f"\n[{fid}] retry: {name} ({city or '?'})")
    try:
        extracted = call_retry_llm(name, city)
    except Exception as e:
        say(f"    [!] [{fid}] LLM error: {e}")
        return "llm_error"

    if not extracted.get('name_match'):
        say(f"    [-] [{fid}] retry still no_match — giving up")
        if apply:
            conn = connect(); conn.set_client_encoding("UTF8")
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                write_sentinel(cur, fid, None, "retry_d_still_no_match")
                conn.commit()
            finally: conn.close()
        return "no_match"

    conf = {'high': 0.88, 'medium': 0.74, 'low': 0.56}.get(
                extracted.get('confidence', 'medium'), 0.74)
    cite = extracted.pop('cite_quote', None)
    extracted.pop('name_match', None)
    extracted.pop('confidence', None)
    claimed = {k: v for k, v in extracted.items() if v is not None}
    say(f"    [+] [{fid}] {len(claimed)}/13 fields: {sorted(claimed.keys())}")

    if apply and claimed:
        conn = connect(); conn.set_client_encoding("UTF8")
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("""
                INSERT INTO public.dog_park_enrichment_provenance
                    (dog_park_fid, field_group, source, source_url, confidence,
                     is_canonical, relevance_verified, claimed_values, cite_quote,
                     extraction_method, notes, updated_at)
                VALUES (%s, 'park_v1', 'per_park_amenities_v1', %s, %s,
                        true, true, %s::jsonb, %s, 'retry_d_web_search',
                        'retry_dog_park_no_match.py — query-variant retry', now())
                ON CONFLICT (dog_park_fid, field_group, source) DO UPDATE
                  SET source_url=EXCLUDED.source_url, confidence=EXCLUDED.confidence,
                      claimed_values=EXCLUDED.claimed_values, cite_quote=EXCLUDED.cite_quote,
                      extraction_method=EXCLUDED.extraction_method,
                      is_canonical=true, relevance_verified=true, updated_at=now()
            """, (fid, None, conf, json.dumps(claimed), cite))   # 5 params now match 5 placeholders
            conn.commit()
            cur.execute("SELECT public.promote_canonical_dog_park_policy(%s)", (fid,))
            conn.commit()
        finally:
            conn.close()
    return "extracted" if claimed else "empty"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--workers", type=int, default=6)
    args = ap.parse_args()

    conn = connect(); conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    # Use display_name_override (city-prefixed) when available — better web_search query.
    cur.execute("""
        SELECT DISTINCT dpg.fid,
               COALESCE(dpg.display_name_override, dpg.name) AS name,
               dpg.address_city
          FROM public.dog_park_enrichment_provenance dpep
          JOIN public.dog_parks_gold dpg ON dpg.fid = dpep.dog_park_fid
          JOIN public.dog_park_dog_policy ddp ON ddp.dog_park_fid = dpg.fid
         WHERE dpg.state='CA' AND dpg.is_active AND dpg.is_scoreable
           AND ddp.source IN ('osm_default','default','state_default')
           AND dpep.source='per_park_amenities_v1_no_result'
           AND dpep.claimed_values->>'why' IN ('web_search_no_match','llm_says_name_not_on_page','retry_d_still_no_match')
         ORDER BY dpg.fid
    """ + (f" LIMIT {int(args.limit)}" if args.limit else ""))
    parks = [dict(r) for r in cur.fetchall()]
    conn.close()

    print(f"Retry-D pool: {len(parks)} CA parks; apply={args.apply}; workers={args.workers}")

    summary = {"extracted": 0, "no_match": 0, "llm_error": 0, "empty": 0}
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(process, p, args.apply) for p in parks]
        for fut in as_completed(futures):
            try:
                summary[fut.result()] = summary.get(fut.result(), 0) + 1
            except Exception as e:
                with PRINT_LOCK:
                    print(f"[!] worker raised: {e}", flush=True)
                summary['llm_error'] = summary.get('llm_error', 0) + 1

    print(f"\n{'='*60}\nSUMMARY:")
    for k, v in summary.items():
        print(f"  {k:18} = {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
