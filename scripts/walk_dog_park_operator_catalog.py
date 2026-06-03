"""
walk_dog_park_operator_catalog.py — operator-catalog-walker (codify-pattern, level 2).

UNIFIED 2026-05-27: walks each operator catalog ONCE, extracts BOTH dog parks
AND beaches from the same fetched content. Dog-park extraction unchanged
(existing 60-79% coverage preserved). Beach extraction is additive — adds
a second LLM call on the same content with a beach-focused prompt, writes
results to public.beach_discovery_queue with source_handler='operator_catalog_v1'.
Per Franz "unify, single operator walker, dual extraction" 2026-05-27.

Phase DP-W (after the OSM-shortcut DP-B caps out at ~13% of CA parks). For
each city/county operator that runs multiple dog parks, fetch the operator's
catalog page (dog-park-list OR beach-list — operator may publish either or
both), enumerate the per-entry detail URLs, fuzzy-match each entry to a
dog_parks_gold row (for dog parks) or write to beach_discovery_queue (for
beaches). Then run the 13-field per-park extraction the OSM-shortcut uses
(dog parks only; beaches stay in queue for ingest_beach_discovery_queue).

Two-level codify-pattern:
  - Level 1 (OPERATOR): discover the catalog URL. Hand-coded per op + web_search
    fallback when missing.
  - Level 2 (PARK): catalog page → LLM enumerates (park_name, per_park_url) pairs
    → fuzzy-match to gold → per-park extraction (reuses extract_dog_park_amenities
    substrate: smart_fetch + name_match + cite-required LLM + dpep write + cascade).

Output:
  - dog_park_enrichment_provenance rows with source='catalog_walker_v1'
  - Sentinel rows for unmatched catalog entries + per-park failures
  - dog_park_dog_policy auto-promoted via promote_canonical_dog_park_policy

Usage:
  python scripts/walk_dog_park_operator_catalog.py --dry-run    # default
  python scripts/walk_dog_park_operator_catalog.py --apply
  python scripts/walk_dog_park_operator_catalog.py --apply --op-ids 78,77,175
"""
from __future__ import annotations
import argparse, json, os, re, sys, time, threading
import urllib.parse, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from difflib import SequenceMatcher

import psycopg2.extras
from anthropic import Anthropic

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.common.db import connect
from scripts.common.llm import SONNET
from scripts.extract_research_v2 import strip_html, name_match
from scripts.extract_per_beach_offleash_v2 import smart_fetch, is_thin_or_blocked, is_url_deep_enough
from scripts.extract_dog_park_amenities import (
    call_llm_amenities, write_park_v1, write_sentinel,
)

ANTHROPIC = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]

PRINT_LOCK = threading.Lock()


# ── Operator targets — auto-discovered per state ─────────────────────────
# Hand-coded URLs were dropped 2026-05-25 LATE per analysis — all 14 CA
# targets had stale URLs; web_search escalation found the real catalog every
# time. Walker now auto-discovers operators per state (top N by park count)
# AND always uses web_search to find each operator's catalog.

CA_OPERATOR_TARGETS = [
    77, 175, 78, 53, 74, 52, 180, 21, 176, 83, 177, 48, 61, 68,
]  # kept for backward compat / known-good set


def auto_operator_targets(state: str, min_parks: int = 3) -> list[int]:
    """Query top operators by park count for a state.
    Returns operator (singular) IDs to walk."""
    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT op.id
              FROM public.dog_parks_gold dpg
              JOIN public.operator op ON op.id = dpg.inferred_operator_id
             WHERE dpg.state = %s AND dpg.is_active
               AND op.type IN ('city','county')
             GROUP BY op.id
             HAVING count(*) >= %s
             ORDER BY count(*) DESC
        """, (state, min_parks))
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


# ── T1: Retry-with-backoff for transient Anthropic API errors ──────────

ANTHROPIC_RETRY_EXCEPTIONS = (
    urllib.error.URLError,         # wraps DNS failures (getaddrinfo)
    ConnectionError,                # Remote end closed connection
    TimeoutError,
    urllib.error.HTTPError,         # 503, 429 etc (raised as HTTPError)
)
ANTHROPIC_RETRY_BACKOFF_SECS = (5, 15, 30)  # 3 retries total


def anthropic_post_with_retry(body: dict, timeout: int = 180) -> dict:
    """POST to Anthropic API with retry on transient network errors. Returns
    parsed JSON response. Raises on persistent failure."""
    last_err = None
    for attempt, sleep_after in enumerate([0] + list(ANTHROPIC_RETRY_BACKOFF_SECS)):
        if sleep_after > 0:
            with PRINT_LOCK:
                print(f"      [retry] anthropic call attempt {attempt}, sleeping {sleep_after}s after error: {last_err}", flush=True)
            time.sleep(sleep_after)
        try:
            req = urllib.request.Request(
                "https://api.anthropic.com/v1/messages",
                data=json.dumps(body).encode("utf-8"), method="POST",
            )
            req.add_header("x-api-key", ANTHROPIC_KEY)
            req.add_header("anthropic-version", "2023-06-01")
            req.add_header("content-type", "application/json")
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except ANTHROPIC_RETRY_EXCEPTIONS as e:
            last_err = e
            # HTTPError with 4xx (other than 429) is fatal — don't retry
            if isinstance(e, urllib.error.HTTPError) and 400 <= e.code < 500 and e.code != 429:
                raise
            continue
    raise RuntimeError(f"anthropic_post_with_retry exhausted retries; last error: {last_err}")


# ── LLM call — catalog enumeration ──────────────────────────────────────

CATALOG_PROMPT_SYSTEM = (
    "You are extracting a LIST of dog parks from a single operator-posted "
    "catalog/index page. The page lists multiple dog parks run by ONE operator "
    "(a city or county parks department). For each park, return its name and "
    "the URL where you can read more about it (typically a deep link on the "
    "same domain). Do NOT extract dog parks that are NOT in this operator's "
    "system. Use ONLY the provided source page."
)

CATALOG_PROMPT_USER_TEMPLATE = """SOURCE CATALOG PAGE CONTENT:
{content}

OPERATOR: {op_name}

Extract every dog park listed on this catalog page. Return JSON:
{{
  "operator_match": <true|false: is this the catalog page for {op_name}'s dog parks?>,
  "parks": [
    {{
      "name": "<park name as listed on catalog>",
      "url":  "<absolute URL to the park detail page; null if no link visible>",
      "blurb": "<1-sentence blurb if available, else null>"
    }},
    ...
  ]
}}

If the page is an aggregator / not the operator's own catalog, set operator_match=false and return empty parks."""


# ── Beach catalog prompts (additive — 2026-05-27) ──────────────────────
# Beach extraction is a parallel pass on the SAME fetched catalog content.
# Two LLM calls instead of one keeps the dog-park prompt's specificity intact
# and avoids regressing the existing 60-79% per-state dog-park coverage.
# Cost: ~$0.01 extra per operator catalog walked.

BEACH_CATALOG_PROMPT_SYSTEM = (
    "You are extracting a LIST of NAMED BEACHES from a single operator-posted "
    "catalog/index page. The page may be a city/county parks department site. "
    "Many such operators publish a BEACHES catalog alongside their parks catalog. "
    "Some city operators publish ONLY parks (no beaches) — that is normal and "
    "you should return an empty list in that case. Use ONLY the provided page."
)

BEACH_CATALOG_PROMPT_USER_TEMPLATE = """SOURCE CATALOG PAGE CONTENT:
{content}

OPERATOR: {op_name}

Extract every named beach feature listed on this catalog page. Return JSON:
{{
  "operator_match": <true|false: is this a catalog page operated by {op_name}?>,
  "beaches": [
    {{
      "name": "<beach name as listed; MUST be a proper noun containing
               Beach/Cove/Bay/Lagoon/Swim/Sand stem>",
      "url":  "<absolute URL to the beach detail page; null if no link>",
      "blurb": "<1-sentence blurb if available, else null>"
    }},
    ...
  ]
}}

REJECT:
- Marinas/boat launches/boat ramps unless 'Beach' is literally in the name
- Generic phrases like 'sandy beaches' or 'the beach' (no proper noun)
- Sand dunes / sand mountains (off-road features, not water beaches)
- Historical / closed beaches
- Beaches in OTHER operators' jurisdictions

Return operator_match=false and empty beaches if the page is not the operator's
own catalog OR if the operator publishes no beaches."""


def call_llm_enumerate_beaches(content: str, op_name: str,
                               enable_web_search: bool = False) -> dict:
    """Beach analog of call_llm_enumerate_catalog. Same fetched content,
    different prompt. Returns dict with 'operator_match' + 'beaches' list."""
    user = BEACH_CATALOG_PROMPT_USER_TEMPLATE.format(
        content=content[:50000], op_name=op_name)
    body = {
        "model": SONNET,
        "max_tokens": 2500,
        "system": BEACH_CATALOG_PROMPT_SYSTEM,
        "messages": [{"role": "user", "content": user}],
    }
    if enable_web_search:
        body["tools"] = [{"type": "web_search_20250305",
                          "name": "web_search", "max_uses": 2}]
    resp = anthropic_post_with_retry(body)
    text_blocks = [b.get("text", "") for b in resp.get("content", [])
                    if b.get("type") == "text"]
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
        return {"operator_match": False, "beaches": [], "_parse_error": raw[:300]}


def insert_beach_queue_rows(op_id: int, op_name: str, op_state: str,
                             op_city: str | None,
                             catalog_url: str | None,
                             beaches: list[dict],
                             apply: bool = False) -> int:
    """Insert extracted beaches into public.beach_discovery_queue.
    Returns count of rows inserted. UNIQUE constraint dedupes within state."""
    if not beaches:
        return 0
    if not apply:
        return 0  # dry-run: count externally via len(beaches)
    n_inserted = 0
    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor()
        for b in beaches:
            name = (b.get("name") or "").strip()
            if not name:
                continue
            # Same post-LLM guard as walk_state_park_beaches.py: require a
            # water/beach stem in the name to filter out generic catalog
            # text that snuck through the prompt.
            if not re.search(
                r"\b(beach|cove|bay|lagoon|swim|swimming|sand[ay]?)\b",
                name, re.I):
                continue
            blurb = (b.get("blurb") or "").strip()
            beach_url = b.get("url") or catalog_url
            # parent_park_name for operator-catalog-sourced beaches is the
            # operator's city (e.g., "Long Beach", "San Diego") since these
            # are city/county beaches, not state-park beaches.
            parent = op_city or op_name
            try:
                cur.execute("""
                  INSERT INTO public.beach_discovery_queue
                    (state, parent_park_name, parent_park_url,
                     beach_name, verbatim_quote, source_url,
                     source_handler, status, queue_meta)
                  VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending', %s)
                  ON CONFLICT
                    (state, lower(parent_park_name),
                     lower(beach_name), source_handler)
                  DO UPDATE SET
                    verbatim_quote = excluded.verbatim_quote,
                    source_url = excluded.source_url,
                    updated_at = now()
                """, (op_state, parent, catalog_url, name, blurb,
                      beach_url, "operator_catalog_v1",
                      json.dumps({"operator_id": op_id,
                                  "operator_name": op_name})))
                n_inserted += 1
            except Exception as e:
                print(f"    [beach queue insert error] {name}: {e}")
                continue
        conn.commit()
    finally:
        conn.close()
    return n_inserted


def call_llm_enumerate_catalog(content: str, op_name: str,
                               enable_web_search: bool = False) -> dict:
    """Enumerate dog parks from a catalog page. enable_web_search=True lets
    Sonnet use web_search to find the catalog when hand-coded URL is dead/wrong.
    Uses anthropic_post_with_retry for resilience against transient network errors."""
    user = CATALOG_PROMPT_USER_TEMPLATE.format(content=content[:50000], op_name=op_name)
    body = {
        "model": SONNET,
        "max_tokens": 3000,
        "system": CATALOG_PROMPT_SYSTEM,
        "messages": [{"role": "user", "content": user}],
    }
    if enable_web_search:
        body["tools"] = [{"type": "web_search_20250305", "name": "web_search", "max_uses": 4}]
    resp = anthropic_post_with_retry(body)
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
        return {"operator_match": False, "parks": [], "_parse_error": raw[:300]}


# ── Fuzzy-match catalog entries to dog_parks_gold rows ──────────────────

def normalize_name(name: str) -> str:
    """Lowercase, drop common suffixes, strip non-alnum."""
    s = name.lower()
    for suffix in (" dog park", " dog play area", " off-leash area", " dog beach",
                   " dog run", " park", " play area"):
        if s.endswith(suffix):
            s = s[:-len(suffix)]
            break
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def fuzzy_match(catalog_name: str, candidates: list[dict],
                threshold: float = 0.6) -> tuple[dict | None, float]:
    """Return (best_candidate, similarity) or (None, 0.0)."""
    if not candidates or not catalog_name:
        return (None, 0.0)
    norm_cat = normalize_name(catalog_name)
    best = None
    best_score = 0.0
    for c in candidates:
        norm_c = normalize_name(c['name'] or '')
        if not norm_c:
            continue
        score = SequenceMatcher(None, norm_cat, norm_c).ratio()
        # Bonus for token overlap (catches "Walter Haas" vs "Walter Haas Dog Play Areas")
        cat_tokens = set(norm_cat.split())
        c_tokens = set(norm_c.split())
        if cat_tokens and c_tokens:
            jaccard = len(cat_tokens & c_tokens) / len(cat_tokens | c_tokens)
            score = max(score, jaccard)
        if score > best_score:
            best_score = score
            best = c
    return (best, best_score) if best_score >= threshold else (None, best_score)


# ── Spatial dedup helper (arena pattern) ─────────────────────────────────
# Called when fuzzy_match is ambiguous (0.40-0.79). Google Places geocodes
# the catalog name + operator city, then ST_Distance to ALL operator's gold
# candidates. <150m → strong match; >500m → real new park; 150-500m →
# genuinely ambiguous, keep needs_review.
# Per pin [[dog-park-coverage-playbook]] arena-style spatial > string fuzzy.

def spatial_match(catalog_name: str, operator_city: str | None,
                  operator_state: str, candidates: list[dict]) -> tuple[dict | None, float, str]:
    """Return (best_candidate_by_distance, distance_m, geocode_status).
    Returns (None, inf, why) if geocoding fails or no candidates within 500m."""
    try:
        # Lazy import — avoid hard dependency at module load time
        sys_path_save = sys.path[:]
        sys.path.insert(0, str(ROOT))
        from scripts.harvest.geocode import google_places_searchtext
        sys.path = sys_path_save
    except Exception as e:
        return (None, float('inf'), f"import_error:{e}")

    query_parts = [catalog_name]
    if operator_city:
        query_parts.append(operator_city)
    query_parts.append(operator_state or 'CA')
    query = ", ".join(p for p in query_parts if p)

    try:
        place = google_places_searchtext(query)
    except Exception as e:
        return (None, float('inf'), f"places_error:{type(e).__name__}")
    if not place or not place.get('location'):
        return (None, float('inf'), "no_geocode")

    lat = float(place['location']['latitude'])
    lng = float(place['location']['longitude'])

    # Compute distance to each candidate via DB (PostGIS)
    if not candidates:
        return (None, float('inf'), "no_candidates")
    fids = [c['fid'] for c in candidates]
    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT fid, name,
                   ST_Distance(geom::geography,
                               ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) AS dist_m
              FROM public.dog_parks_gold
             WHERE fid = ANY(%s) AND geom IS NOT NULL
             ORDER BY dist_m ASC LIMIT 1
        """, (lng, lat, fids))
        r = cur.fetchone()
    finally:
        conn.close()
    if not r:
        return (None, float('inf'), "no_geom_in_candidates")
    # Find the matching candidate dict
    best_cand = next((c for c in candidates if c['fid'] == r['fid']), None)
    return (best_cand, float(r['dist_m']), f"geocoded:{lat:.4f},{lng:.4f}")


# ── Per-park extraction (reuses extract_dog_park_amenities substrate) ───

def extract_one_park(park_fid: int, park_name: str, city: str | None,
                     per_park_url: str, apply: bool) -> str:
    """Adapted from extract_dog_park_amenities.process_park — different URL source."""
    def say(msg: str) -> None:
        with PRINT_LOCK:
            print(msg, flush=True)

    if not is_url_deep_enough(per_park_url):
        say(f"      [-] [{park_fid}] not deep enough: {per_park_url[:60]}")
        if apply:
            conn = connect(); conn.set_client_encoding("UTF8")
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                write_sentinel(cur, park_fid, per_park_url, "catalog_url_not_deep_enough")
                conn.commit()
            finally: conn.close()
        return "skipped_shallow"

    text_raw, why = smart_fetch(per_park_url)
    if text_raw is None:
        say(f"      [-] [{park_fid}] fetch fail: {why}")
        if apply:
            conn = connect(); conn.set_client_encoding("UTF8")
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                write_sentinel(cur, park_fid, per_park_url, f"catalog_fetch_fail:{why}")
                conn.commit()
            finally: conn.close()
        return "fetch_fail"

    if is_thin_or_blocked(text_raw):
        say(f"      [-] [{park_fid}] thin/blocked ({len(text_raw)} chars)")
        if apply:
            conn = connect(); conn.set_client_encoding("UTF8")
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                write_sentinel(cur, park_fid, per_park_url, f"catalog_thin:{len(text_raw)}")
                conn.commit()
            finally: conn.close()
        return "thin"

    text = strip_html(text_raw) if "<" in text_raw else text_raw[:60000]

    if not name_match(text, park_name):
        say(f"      [-] [{park_fid}] name not on per-park page")
        if apply:
            conn = connect(); conn.set_client_encoding("UTF8")
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                write_sentinel(cur, park_fid, per_park_url, "catalog_name_not_on_page")
                conn.commit()
            finally: conn.close()
        return "no_match"

    try:
        extracted = call_llm_amenities(text, park_name, city)
    except Exception as e:
        say(f"      [!] [{park_fid}] LLM error: {e}")
        return "llm_error"

    if not extracted.get('name_match'):
        say(f"      [-] [{park_fid}] LLM says name_match=false")
        if apply:
            conn = connect(); conn.set_client_encoding("UTF8")
            try:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                write_sentinel(cur, park_fid, per_park_url, "catalog_llm_name_no_match")
                conn.commit()
            finally: conn.close()
        return "no_match"

    conf = {'high': 0.92, 'medium': 0.78, 'low': 0.60}.get(
                extracted.get('confidence', 'medium'), 0.78)
    cite = extracted.pop('cite_quote', None)
    extracted.pop('name_match', None)
    extracted.pop('confidence', None)
    claimed = {k: v for k, v in extracted.items() if v is not None}
    say(f"      [+] [{park_fid}] {len(claimed)}/13 fields: {sorted(claimed.keys())}")

    if apply and claimed:
        conn = connect(); conn.set_client_encoding("UTF8")
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # write_park_v1 uses source='per_park_amenities_v1'; override to mark catalog provenance
            cur.execute("""
                INSERT INTO public.dog_park_enrichment_provenance
                    (dog_park_fid, field_group, source, source_url, confidence,
                     is_canonical, relevance_verified, claimed_values, cite_quote,
                     extraction_method, notes, updated_at)
                VALUES (%s, 'park_v1', 'catalog_walker_v1', %s, %s,
                        true, true, %s::jsonb, %s, 'catalog_walker',
                        'walk_dog_park_operator_catalog.py — operator catalog → per-park extract', now())
                ON CONFLICT (dog_park_fid, field_group, source) DO UPDATE
                  SET source_url        = EXCLUDED.source_url,
                      confidence        = EXCLUDED.confidence,
                      claimed_values    = EXCLUDED.claimed_values,
                      cite_quote        = EXCLUDED.cite_quote,
                      extraction_method = EXCLUDED.extraction_method,
                      is_canonical      = true,
                      relevance_verified= true,
                      updated_at        = now()
            """, (park_fid, per_park_url, conf,
                  json.dumps(claimed), cite))
            conn.commit()
            cur.execute("SELECT public.promote_canonical_dog_park_policy(%s)", (park_fid,))
            conn.commit()
        finally:
            conn.close()
    return "extracted" if claimed else "empty"


# ── Per-operator catalog walk ───────────────────────────────────────────

def walk_one_operator(op_id: int, op_name: str, catalog_url: str | None,
                      apply: bool, workers: int,
                      op_state: str = "CA") -> dict:
    """Returns per-op outcome dict. op_state controls which state's
    beach_discovery_queue receives extracted beaches."""
    def say(msg: str) -> None:
        with PRINT_LOCK:
            print(msg, flush=True)

    say(f"\n[OP {op_id}] {op_name}")
    say(f"  catalog: {catalog_url or '(none — would need web_search; skipped in v1)'}")

    out = {"op_id": op_id, "catalog_fetched": False, "catalog_parks_listed": 0,
           "matched_to_gold": 0, "extracted": 0, "fetch_fails": 0,
           "no_match": 0, "thin": 0, "unmatched_catalog_entries": 0,
           "catalog_beaches_listed": 0, "beaches_queued": 0}

    # Step 1: fetch catalog (with web_search escalation when URL missing/dead/wrong)
    text = None
    catalog_url_used = catalog_url
    if catalog_url:
        text_raw, why = smart_fetch(catalog_url)
        if text_raw is not None and not is_thin_or_blocked(text_raw):
            out['catalog_fetched'] = True
            text = strip_html(text_raw) if "<" in text_raw else text_raw[:80000]
        else:
            say(f"  [-] catalog fetch failed/thin ({why if text_raw is None else 'thin'}) → web_search escalation")

    # Step 2: LLM enumerate parks (web_search if no usable catalog content)
    use_web_search = text is None
    if use_web_search:
        # Pass a web_search instruction; LLM finds the catalog page itself
        text = (f"[NO CATALOG CONTENT FETCHED — use web_search to find {op_name}'s "
                f"OFFICIAL dog-park catalog/listing page (must be on the operator's "
                f".gov or .org domain, NOT Yelp / BringFido / aggregators). "
                f"Then enumerate every dog park listed with name + per-park URL.]")
    try:
        result = call_llm_enumerate_catalog(text, op_name, enable_web_search=use_web_search)
    except Exception as e:
        say(f"  [!] catalog LLM error: {e}")
        return out

    if not result.get('operator_match'):
        say(f"  [-] LLM says no operator match (web_search={use_web_search})")
        return out

    parks = result.get('parks') or []
    out['catalog_parks_listed'] = len(parks)
    say(f"  catalog enumerated {len(parks)} parks")

    # Step 2b: BEACH extraction on the same fetched content (additive pass
    # per Franz "unify" 2026-05-27). Beaches are routed to
    # beach_discovery_queue, NOT dog_park_discovery_queue.
    try:
        beach_result = call_llm_enumerate_beaches(
            text, op_name, enable_web_search=use_web_search)
        beaches = beach_result.get('beaches') or []
        out['catalog_beaches_listed'] = len(beaches)
        if beaches:
            say(f"  catalog enumerated {len(beaches)} beaches")
            # Find operator city from existing gold candidates (probed below)
            # or query operator table directly.
            op_city = None
            conn2 = connect(); conn2.set_client_encoding("UTF8")
            try:
                cur2 = conn2.cursor()
                cur2.execute(
                    "SELECT address_city FROM public.dog_parks_gold "
                    " WHERE inferred_operator_id=%s AND address_city IS NOT NULL "
                    " LIMIT 1", (op_id,))
                r = cur2.fetchone()
                if r:
                    op_city = r[0]
            finally:
                conn2.close()
            n_queued = insert_beach_queue_rows(
                op_id, op_name, op_state, op_city,
                catalog_url_used, beaches, apply=apply)
            out['beaches_queued'] = n_queued
            if apply:
                say(f"  queued {n_queued} beach rows to beach_discovery_queue")
            else:
                say(f"  (dry-run) would queue {len(beaches)} beach rows")
    except Exception as e:
        say(f"  [!] beach LLM error (non-fatal): {e}")

    # Step 3: fetch gold candidates for this operator
    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT fid, name, address_city
              FROM public.dog_parks_gold
             WHERE inferred_operator_id = %s AND is_active
        """, (op_id,))
        candidates = list(cur.fetchall())
    finally:
        conn.close()

    # Step 4: match each catalog entry to a gold candidate; queue unmatched
    matches: list[tuple[dict, dict, str]] = []  # (catalog_entry, gold_row, resolved_url)
    queue_rows: list[dict] = []
    catalog_src_url = catalog_url_used or catalog_url or '(web_search)'
    for entry in parks:
        cat_name = entry.get('name', '')
        cat_url  = entry.get('url')
        # Resolve relative URLs against catalog base (best-effort; cat_url may be None)
        if cat_url:
            if cat_url.startswith('/') and catalog_src_url and catalog_src_url.startswith('http'):
                cat_url = urllib.parse.urljoin(catalog_src_url, cat_url)
            elif not cat_url.startswith('http') and catalog_src_url and catalog_src_url.startswith('http'):
                cat_url = urllib.parse.urljoin(catalog_src_url, cat_url)
        gold, sim = fuzzy_match(cat_name, candidates)
        # SPATIAL ESCALATION (arena pattern) — if string sim is ambiguous
        # (no match OR borderline), Google Places geocode the catalog entry
        # + ST_Distance against ALL operator's gold candidates. Resolves cases
        # where string match misses ("Doyle Community Park" vs "Rincon Valley
        # Community Park") but spatial proximity is decisive.
        used_spatial = False
        if (gold is None and 0.40 <= sim < 0.60) or (gold is not None and sim < 0.80):
            # Skip if no city context (geocoding will be noisy)
            op_city = candidates[0].get('address_city') if candidates else None
            spat_gold, dist_m, status = spatial_match(cat_name, op_city, 'CA', candidates)
            used_spatial = True
            if spat_gold and dist_m < 150:
                # Strong spatial confirm — promote/override fuzzy decision
                gold = spat_gold
                sim = max(sim, 0.85)  # bump to high-confidence
                say(f"    [spatial<150m] '{cat_name}' → [{gold['fid']}] {gold['name']} (dist={dist_m:.0f}m)")
            elif spat_gold and dist_m > 500 and gold is not None:
                # Fuzzy said yes but spatial disagrees — distinct park, demote to queue-only
                queue_rows.append({
                    'catalog_park_name': cat_name,
                    'catalog_park_url':  cat_url,
                    'blurb':             entry.get('blurb'),
                    'best_similarity':   sim,
                    'best_match_fid':    None,
                    'best_match_name':   None,
                    'status':            'pending',
                })
                say(f"    [spatial>500m] '{cat_name}' real new park (fuzzy said {gold['name']}; dist={dist_m:.0f}m)")
                out['unmatched_catalog_entries'] += 1
                continue
            # else: 150-500m or no geocode → fall through to fuzzy decision

        if gold is None:
            say(f"    [unmatched sim={sim:.2f}{' spatial' if used_spatial else ''}] '{cat_name}' → discovery queue")
            queue_rows.append({
                'catalog_park_name': cat_name,
                'catalog_park_url':  cat_url,
                'blurb':             entry.get('blurb'),
                'best_similarity':   sim,
                'best_match_fid':    None,
                'best_match_name':   None,
                'status':            'pending',
            })
            out['unmatched_catalog_entries'] += 1
            continue
        # Borderline match (0.6-0.8) without spatial promotion: extract AND queue for review
        if sim < 0.80:
            queue_rows.append({
                'catalog_park_name': cat_name,
                'catalog_park_url':  cat_url,
                'blurb':             entry.get('blurb'),
                'best_similarity':   sim,
                'best_match_fid':    gold['fid'],
                'best_match_name':   gold['name'],
                'status':            'needs_review',
            })
        if not cat_url:
            say(f"    [match {sim:.2f}] '{cat_name}' → [{gold['fid']}] (no per-park URL → skip extraction)")
            continue
        say(f"    [match {sim:.2f}] '{cat_name}' → [{gold['fid']}] {gold['name']} → {cat_url[:60]}")
        matches.append((entry, gold, cat_url))
        out['matched_to_gold'] += 1

    # Persist discovery queue rows
    if queue_rows and apply:
        conn = connect(); conn.set_client_encoding("UTF8")
        try:
            cur = conn.cursor()
            for q in queue_rows:
                cur.execute("""
                    INSERT INTO public.dog_park_discovery_queue
                        (operator_id, operator_name, catalog_park_name, catalog_park_url,
                         blurb, catalog_source_url, best_similarity, best_match_fid,
                         best_match_name, status, discovered_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
                    ON CONFLICT (operator_id, catalog_park_url) DO UPDATE SET
                        best_similarity = excluded.best_similarity,
                        best_match_fid  = excluded.best_match_fid,
                        best_match_name = excluded.best_match_name,
                        catalog_park_name = excluded.catalog_park_name,
                        blurb           = excluded.blurb,
                        updated_at      = now()
                """, (op_id, op_name, q['catalog_park_name'], q['catalog_park_url'],
                      q['blurb'], catalog_src_url, q['best_similarity'],
                      q['best_match_fid'], q['best_match_name'], q['status']))
            conn.commit()
            say(f"    queued {len(queue_rows)} discovery rows")
        finally:
            conn.close()

    # Step 5: per-park extract in parallel
    if not matches:
        return out
    say(f"  extracting {len(matches)} matched parks (workers={workers})...")
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(extract_one_park, gold['fid'], gold['name'],
                             gold.get('address_city'), cat_url, apply)
                   for entry, gold, cat_url in matches]
        for fut in as_completed(futures):
            try:
                outcome = fut.result()
            except Exception as e:
                with PRINT_LOCK:
                    print(f"      [!] worker raised: {e}", flush=True)
                outcome = "llm_error"
            if outcome == "extracted":   out['extracted'] += 1
            elif outcome == "fetch_fail": out['fetch_fails'] += 1
            elif outcome == "no_match":  out['no_match'] += 1
            elif outcome == "thin":      out['thin'] += 1

    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--op-ids", type=str, default=None,
                    help="Comma-separated operator.id (singular). Default: auto-discover for --state.")
    ap.add_argument("--state", type=str, default="CA",
                    help="State filter for auto-discovery (default CA).")
    ap.add_argument("--min-parks", type=int, default=3,
                    help="Operator must have ≥N active parks in state to be a target.")
    ap.add_argument("--workers", type=int, default=3,
                    help="Per-park parallel workers. Default 3 — core dog-park coverage script; "
                         "cap < 12 per [[supabase-pool-cap-vs-dagster-concurrency]].")
    ap.add_argument("--chunk", type=int, default=3,
                    help="Process operators in chunks of N, sleeping between (T5 tuning).")
    ap.add_argument("--sleep", type=int, default=30,
                    help="Seconds to sleep between operator chunks (T5 tuning).")
    args = ap.parse_args()

    target_ops = ([int(x) for x in args.op_ids.split(",")] if args.op_ids
                  else auto_operator_targets(args.state, args.min_parks))

    conn = connect(); conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT id, name FROM public.operator WHERE id = ANY(%s)", (target_ops,))
    ops = {r['id']: r['name'] for r in cur.fetchall()}
    conn.close()

    print(f"Catalog walker — targeting {len(target_ops)} operators; apply={args.apply}; "
          f"workers={args.workers}; chunk={args.chunk}; sleep={args.sleep}s")

    # T5: chunked operator runs to ease Anthropic API + DNS load
    per_op = []
    for i, op_id in enumerate(target_ops):
        op_name = ops.get(op_id, f"op_{op_id}")
        result = walk_one_operator(op_id, op_name,
                                   None,   # T2: always None → forces web_search discovery
                                   args.apply, args.workers,
                                   op_state=args.state)
        per_op.append(result)
        # Sleep between chunks (not after last op)
        if (i + 1) % args.chunk == 0 and (i + 1) < len(target_ops):
            with PRINT_LOCK:
                print(f"\n--- chunk break: sleeping {args.sleep}s before next batch ---", flush=True)
            time.sleep(args.sleep)

    print(f"\n{'='*70}\nSUMMARY:")
    print(f"  {'op_id':>6} {'fetched':>8} {'parks':>6} {'matched':>8} "
          f"{'extracted':>10} {'unmatched':>10} {'beaches':>8} {'queued':>7}")
    totals = {"catalog_parks_listed": 0, "matched_to_gold": 0, "extracted": 0,
              "unmatched_catalog_entries": 0,
              "catalog_beaches_listed": 0, "beaches_queued": 0}
    for r in per_op:
        print(f"  {r['op_id']:>6} {'Y' if r['catalog_fetched'] else 'N':>8} "
              f"{r['catalog_parks_listed']:>6} {r['matched_to_gold']:>8} "
              f"{r['extracted']:>10} {r['unmatched_catalog_entries']:>10} "
              f"{r.get('catalog_beaches_listed',0):>8} "
              f"{r.get('beaches_queued',0):>7}")
        for k in totals: totals[k] += r.get(k, 0)
    print(f"  {'TOTAL':>6} {'':>8} {totals['catalog_parks_listed']:>6} "
          f"{totals['matched_to_gold']:>8} {totals['extracted']:>10} "
          f"{totals['unmatched_catalog_entries']:>10} "
          f"{totals['catalog_beaches_listed']:>8} "
          f"{totals['beaches_queued']:>7}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
