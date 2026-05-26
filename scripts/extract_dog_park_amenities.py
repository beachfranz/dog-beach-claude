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


# ── URL-slug-based name match (fallback for SPA pages where body is empty) ─

def url_slug_matches_name(url: str, park_name: str) -> bool:
    """Tier-0 name match: if the URL's path slug contains all distinctive
    tokens from the park name (after stripping common suffixes), trust the URL.
    Handles SF Rec & Parks SPA pattern where body text is empty until JS
    hydrates but the URL slug is authoritative.

    Returns True if URL slug encodes the park name. Caller still requires
    the page body to mention SOMETHING about dogs (handled in main flow)."""
    if not url or not park_name:
        return False
    path = urllib.parse.urlparse(url).path.lower()
    # Normalize park name: drop stopwords + common suffixes, keep distinctive tokens
    name_low = park_name.lower()
    for suffix in (" dog park", " dog play area", " dog play areas",
                   " off-leash area", " dog beach", " dog run", " park"):
        if name_low.endswith(suffix):
            name_low = name_low[:-len(suffix)]
            break
    stopwords = {'the', 'a', 'an', 'of', 'and', 'at', 'in', 'on'}
    distinctive = [w for w in re.findall(r"[a-z0-9]+", name_low)
                   if w not in stopwords and len(w) > 2]
    if not distinctive:
        return False
    # All distinctive tokens must appear in URL path
    return all(tok in path for tok in distinctive)


def page_mentions_dog_terms(text: str) -> bool:
    """Light-weight guard for the URL-slug-match fallback: even if the URL slug
    matches the park name, ensure the page body has SOME dog-related content
    so we're not extracting from a redirect-to-404 or wrong-page accident."""
    if not text:
        return False
    low = text.lower()[:20000]
    dog_terms = ['dog', 'leash', 'off-leash', 'off leash', 'dogs',
                 'pet', 'k-9', 'k9', 'canine']
    return any(t in low for t in dog_terms)

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
    "guess.\n\n"
    "NAME MATCH RULE: name_match=true if EITHER (a) the body text mentions "
    "the park by name OR a clear abbreviation, OR (b) the SOURCE URL slug "
    "encodes the park name (e.g., '/Alamo-Square-Dog-Play-Area' for 'Alamo "
    "Square Dog Play Area'). The URL slug is authoritative — operator-posted "
    "URLs always identify the park. Some operator sites are JS SPAs where "
    "body text is generic nav chrome; trust the URL.\n\n"
    "Honest-prose rule for description: use specific nouns. Name what IS "
    "there AND what isn't. Banned filler: 'amenities', 'facilities', 'the "
    "data', 'source page'. 1-3 sentences max."
)

PROMPT_USER_TEMPLATE = """SOURCE URL: {source_url}

SOURCE PAGE CONTENT:
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


def call_llm_amenities(content: str, name: str, city: str | None,
                       source_url: str = "",
                       enable_web_search: bool = False) -> dict:
    user = PROMPT_USER_TEMPLATE.format(
        source_url=source_url,
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
    if enable_web_search:
        body["tools"] = [{"type": "web_search_20250305",
                          "name": "web_search", "max_uses": 4}]
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


# ── Hosts where direct fetch doesn't work; route via web_search ──
# Two failure shapes both treated as "use web_search":
#   1. SPA listings with no real per-park URLs (sfrecpark.org)
#   2. Cloudflare/bot-blocked sites where Playwright still gets 403
# Expanded 2026-05-25 LATE after diagnostic showed these hosts dominate
# the remaining fetch_fail + thin buckets.
NO_PER_PARK_URL_HOSTS = {
    # SPA listings — no per-park URLs
    "sfrecpark.org",
    "laparks.org",                     # LA RAP — search-based SPA
    "cityofsacramento.gov",
    "cityofdavis.org",
    "sanjoseca.gov",
    "alamedaca.gov",
    "santaclaraca.gov",
    "sanramon.ca.gov",
    # Civic-plus / facility-directory clones that 403 + thin under Playwright
    "civicplus.com",                   # matches *.civicplus.com (ca-imperialbeach etc.)
    "downeyca.org",
    "arcadiaca.gov",
    "lagunabeachcity.net",
    "elsegundorecparks.org",
    "redwoodcity.org",
    "whittierprcs.org",
    "lincolnca.gov",
    "southpasadenaca.gov",
}


def host_lacks_per_park_pages(url: str) -> bool:
    host = (urllib.parse.urlparse(url).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return any(host.endswith(h) for h in NO_PER_PARK_URL_HOSTS)


# ── Per-park worker (thread-safe; opens its own conn) ──────────────────

PRINT_LOCK = threading.Lock()


def extract_via_web_search(park_fid: int, name: str, city: str | None,
                           original_url: str, apply: bool) -> str:
    """Web_search route. Two callsites:
      - SPA listings where per-park URLs don't exist (sfrecpark.org etc.)
      - Parks with no OSM website tag — use park name + city for discovery
    Asks Sonnet to find operator-posted amenity info via web_search with
    cite-required contract."""
    def say(msg: str) -> None:
        with PRINT_LOCK:
            print(msg, flush=True)

    if original_url:
        instruction = (
            f"The page at {original_url} is a generic facility-search SPA — no "
            f"per-park data. Use web_search to find AUTHORITATIVE info about "
            f"'{name}' dog park/play area in {city or 'California'}. "
            f"Prefer the operator's own .gov / .org pages over aggregators. "
            f"Extract the 13 fields per the schema, with a verbatim cite quote."
        )
    else:
        # No website tag at all — start fresh from name + city
        instruction = (
            f"No source URL on file for this park. Use web_search to find "
            f"AUTHORITATIVE info about '{name}' dog park in "
            f"{city or 'California'}. Prefer the operator's official .gov "
            f"/ .org pages (city parks-rec department, county parks site) "
            f"over aggregators (BringFido, Yelp, BarkPark). Extract the 13 "
            f"fields per the schema with a verbatim cite quote from the "
            f"source you found. If web_search can't find an authoritative "
            f"source, return name_match=false."
        )
    try:
        extracted = call_llm_amenities(
            instruction, name, city,
            source_url=original_url,
            enable_web_search=True,
        )
    except Exception as e:
        say(f"      [!] [{park_fid}] web_search LLM error: {e}")
        return "llm_error"

    if not extracted.get('name_match'):
        say(f"    [-] [{park_fid}] web_search: name_match=false")
        if apply:
            conn = connect(); conn.set_client_encoding("UTF8")
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                write_sentinel(cur, park_fid, original_url,
                               "web_search_no_match")
                conn.commit()
            finally:
                conn.close()
        return "no_match"

    conf = {'high': 0.88, 'medium': 0.74, 'low': 0.56}.get(  # slight haircut vs direct
                extracted.get('confidence', 'medium'), 0.74)
    cite = extracted.pop('cite_quote', None)
    extracted.pop('name_match', None)
    extracted.pop('confidence', None)
    claimed = {k: v for k, v in extracted.items() if v is not None}
    say(f"    [+] [{park_fid}] (web_search) {len(claimed)}/13 fields: {sorted(claimed.keys())}")

    if apply and claimed:
        conn = connect(); conn.set_client_encoding("UTF8")
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Mark as web_search-sourced; promotes the same as direct extraction
            cur.execute("""
                INSERT INTO public.dog_park_enrichment_provenance
                    (dog_park_fid, field_group, source, source_url, confidence,
                     is_canonical, relevance_verified, claimed_values, cite_quote,
                     extraction_method, notes, updated_at)
                VALUES (%s, 'park_v1', 'per_park_amenities_v1', %s, %s,
                        true, true, %s::jsonb, %s, 'web_search',
                        'extract_dog_park_amenities.py — web_search route (host lacks per-park URLs)', now())
                ON CONFLICT (dog_park_fid, field_group, source) DO UPDATE
                  SET source_url        = EXCLUDED.source_url,
                      confidence        = EXCLUDED.confidence,
                      claimed_values    = EXCLUDED.claimed_values,
                      cite_quote        = EXCLUDED.cite_quote,
                      extraction_method = EXCLUDED.extraction_method,
                      is_canonical      = true,
                      relevance_verified= true,
                      updated_at        = now()
            """, (park_fid, original_url, conf,
                  json.dumps(claimed), cite))
            conn.commit()
            cur.execute("SELECT public.promote_canonical_dog_park_policy(%s)", (park_fid,))
            conn.commit()
        finally:
            conn.close()
    return "extracted" if claimed else "empty"


def process_park(p: dict, apply: bool) -> str:
    """Process one park end-to-end. Returns outcome key for summary tally.
    Opens + closes its own DB connection (psycopg2 conns are not thread-safe)."""
    fid, name = p['fid'], p['name']
    city = p.get('address_city')
    url  = p.get('osm_website')

    def say(msg: str) -> None:
        with PRINT_LOCK:
            print(msg, flush=True)

    say(f"\n[{fid}] {name}  city={city or '-'}  url={(url or '-')[:70]}")

    # No-website route: park has no OSM website tag → use city + name via web_search
    if not url:
        if not city:
            say(f"    [-] [{fid}] no website + no city — cannot route")
            return "no_url_no_city"
        say(f"    [route] {fid} → web_search (no OSM website)")
        return extract_via_web_search(fid, name, city, "", apply)

    # Host-specific route: for SPA listings with no per-park URLs (e.g.
    # sfrecpark.org), skip the fetch and go straight to web_search.
    if host_lacks_per_park_pages(url):
        say(f"    [route] {fid} → web_search (host lacks per-park URLs)")
        return extract_via_web_search(fid, name, city, url, apply)

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

    # Tier-0: URL slug matches → trust the URL even if body name match fails
    # (Required for SF Rec & Parks Facilities SPAs where body is sometimes
    # still hydrating. We still require dog-related body content as a guard.)
    name_ok = name_match(text, name)
    if not name_ok and url_slug_matches_name(url, name) and page_mentions_dog_terms(text):
        say(f"    [~] [{fid}] body name_match failed, URL slug + dog terms → trust")
        name_ok = True

    if not name_ok:
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
        extracted = call_llm_amenities(text, name, city, source_url=url)
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
    ap.add_argument("--state", type=str, default="CA",
                    help="State filter (default CA). Use --all-states to bypass.")
    ap.add_argument("--all-states", action="store_true",
                    help="Bypass state filter — process all active+scoreable parks")
    ap.add_argument("--workers", type=int, default=6,
                    help="Parallel worker threads. Cap at <15 per [[supabase-pool-cap-vs-dagster-concurrency]]; default 6.")
    ap.add_argument("--include-no-website", action="store_true",
                    help="Also process parks without OSM website tag (uses web_search with city + name).")
    args = ap.parse_args()

    if args.workers > 12:
        print(f"WARN: --workers={args.workers} risks Supabase pooler exhaustion (cap 15)")

    conn = connect()
    conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if args.fids:
        target_fids = [int(x.strip()) for x in args.fids.split(",")]
        # If --include-no-website, drop the website-required filter
        website_clause = "" if args.include_no_website else " AND dpg.website IS NOT NULL"
        cur.execute(f"""
            SELECT dpg.fid, dpg.name, dpg.address_city, dpg.website AS osm_website
              FROM public.dog_parks_gold dpg
             WHERE dpg.fid = ANY(%s){website_clause}
        """, (target_fids,))
    else:
        state_filter = "" if args.all_states else f" AND state = %s "
        state_params = [] if args.all_states else [args.state]
        # --include-no-website expands the work-set to parks lacking OSM website
        # but with address_city populated (web_search route works on those).
        website_filter = (
            " (osm_website IS NOT NULL OR address_city IS NOT NULL)"
            if args.include_no_website else
            " osm_website IS NOT NULL"
        )
        cur.execute(f"""
            SELECT fid, name, address_city, osm_website
              FROM public.dog_parks_active_unsourced
             WHERE {website_filter} {state_filter}
             ORDER BY (osm_website IS NULL), fid
        """ + (f" LIMIT {args.limit}" if args.limit else ""), state_params)
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
