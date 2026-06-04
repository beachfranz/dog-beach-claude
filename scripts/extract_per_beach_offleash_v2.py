"""
extract_per_beach_offleash_v2.py — codify-pattern port of per-beach off-leash extractor.

V1 (extract_per_beach_offleash.py) got 0/25 because gather_urls() returned
agency catalog roots (sandiego.gov, parks.ca.gov, ocparks.com) that don't
mention specific beaches by name.

V2 ports five codify infrastructure pieces:
  P2. Park-platform candidate builders (CDPR / SD City / LA County / OC Parks
      / generic operator-slug) — mirrors codify's _candidates_municode pattern.
  P3. Smart-fetch routing (Playwright for JS-rendered, urllib for static) +
      richer AUTH_DOMAINS scoring + DEEPLINK_MARKERS gate (reject root URLs).
  P4. Anthropic web_search_20250305 fallback when fetch is thin/blocked.
  P5. Sentinel-row writes (BEP source='per_beach_offleash_v2_no_result') so
      freshness guards skip re-extracting next run.

For the cite-required LLM prompt + name-match + geo-gate, we reuse v1.

Usage:
  python scripts/extract_per_beach_offleash_v2.py --dry-run  # default
  python scripts/extract_per_beach_offleash_v2.py --apply --limit 5
  python scripts/extract_per_beach_offleash_v2.py --apply --fids 8472,8333
"""
from __future__ import annotations
import argparse, json, os, re, sys, time
import urllib.parse, urllib.request, urllib.error
from pathlib import Path

import psycopg2.extras
from anthropic import Anthropic

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.common.db import connect
from scripts.common.llm import SONNET
from scripts.extract_research_v2 import (
    strip_html, name_match, geo_gate, AUTH_DOMAINS as RV2_AUTH,
)

# Wire codify's smart-fetch + Playwright support
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts" / "fetch"))
try:
    from fetch_html import fetch as playwright_fetch  # type: ignore
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    playwright_fetch = None  # type: ignore
    PLAYWRIGHT_AVAILABLE = False

ANTHROPIC = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]


# ── Phase 10b corpus extension — state-park-listing URLs ───────────────
# Loaded once at module init from scripts/state_park_urls.json. Used by
# candidates_for_beach() as a state-park fallback when the beach is in a
# state-park context (mng_agncy=state-parks agency or name contains
# "State Beach" / "State Park" / "State Recreation").
try:
    _STATE_PARK_URLS_PATH = Path(__file__).resolve().parent / "state_park_urls.json"
    with open(_STATE_PARK_URLS_PATH, encoding="utf-8") as _f:
        _STATE_PARK_URLS = json.load(_f)
except (FileNotFoundError, json.JSONDecodeError):
    _STATE_PARK_URLS = {}


# ── P3 — Richer AUTH_DOMAINS (port codify's wider list) ────────────────
AUTH_DOMAINS = dict(RV2_AUTH)
AUTH_DOMAINS.update({
    # Codify's stronger tiers for park-platform domains
    "beaches.lacounty.gov": 4,
    "cityofcapitola.org": 4,
    "malibucity.org": 4,
    "santabarbaraca.gov": 4,
    "cityofsantacruz.com": 4,
    "catalinaconservancy.org": 3,
    # Aggregators demoted
    "californiabeaches.com": 1,
    "thedogbeach.com": 1,
    "westend.com": 0,
})


def authority_score(url: str) -> int:
    host = (urllib.parse.urlparse(url).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    for dom, score in AUTH_DOMAINS.items():
        if host.endswith(dom):
            return score
    if host.endswith(".gov"):  return 3
    if host.endswith(".ca.us"): return 3
    if host.endswith(".org"):  return 2
    return 1


# ── P3 — DEEPLINK_MARKERS (codify lines 736-749, slim version) ─────────
DEEPLINK_MARKERS = [
    r"page_id=", r"nodeId=", r"\?topic=", r"sectionNum=", r"secid=", r"cite=",
    r"#[A-Za-z0-9]",
    r"/beaches/[a-z\-]+",
    r"/parks/[a-z\-]+/[a-z\-]+",      # parks/parent/child
    r"/park-and-recreation/parks/[a-z\-]+",
    r"\.html$",
    r"\.pdf",
    r"/dog[s]?[-/]",                  # dog-policy / dogs/ pages
    r"/leash",
]

def is_url_deep_enough(url: str) -> bool:
    """Reject only the most obvious agency-root URLs. The downstream
    name_match gate is the real filter — we just want to skip clearly-shallow
    catalog roots (sandiego.gov/, sandiego.gov/park-and-recreation/).
    Sub-routing built into the platform builders means most candidates ARE
    deep enough by construction; this gate is a backstop."""
    if not url:
        return False
    parsed = urllib.parse.urlparse(url)
    path = parsed.path
    query = parsed.query
    # bare domain (but path "/" with a useful query like ?page_id=N is fine)
    if path in ("", "/") and not query:
        return False
    if path in ("", "/") and query:
        # query-bearing root (parks.ca.gov/?page_id=24290) is a deep-link
        return True
    # path with no concrete identifier (just /agency-section/) — only reject
    # if path is short AND no query param AND no fragment
    segs = [s for s in path.split("/") if s]
    if len(segs) <= 1 and not query and not parsed.fragment:
        # Single short segment with nothing else — risky. But if it has a
        # known deep-link marker (page_id, beach slug, etc.), accept.
        for m in DEEPLINK_MARKERS:
            if re.search(m, url):
                return True
        # Still accept if the single segment is long-ish (beach slug)
        return len(segs[0]) >= 4
    return True


# ── P3 — Smart-fetch (port codify's _smart_fetch with platform routing) ─
HEADERS_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Safari/537.36"
)

# Platforms that need Playwright (JS-rendered or known-block)
PLAYWRIGHT_HOSTS = {
    "parks.ca.gov",
    "sanjoseca.gov", "cityofdavis.org",  # AVG / Cloudflare blockers per pin
    "newportbeachca.gov",
    # SF Rec & Parks Facilities is a JS SPA — body text empty without longer wait
    "sfrecpark.org",
    # Common civicplus / facility-directory SPAs that also need JS rendering
    "civicplus.com", "oaklandca.gov", "downeyca.org",
    "elsegundorecparks.org", "redwoodcity.org",
    "alamedaca.gov", "santaclaraca.gov", "whittierprcs.org",
    "southpasadenaca.gov", "lagunabeachcity.net",
    "sanramon.ca.gov", "cityofsacramento.gov",
}

# Per-host Playwright wait override (some SPAs hydrate slowly). Keep modest —
# the URL-slug-based name_match fallback in extract_dog_park_amenities catches
# pages where body hasn't fully hydrated, so we don't need long waits.
PLAYWRIGHT_WAIT_OVERRIDES = {
    "sfrecpark.org": 6.0,        # Facilities SPA — short wait + slug fallback
    "civicplus.com": 6.0,
    "santaclaraca.gov": 6.0,
}

# Nav timeout per host (some operator sites are heavy). Default 30s.
PLAYWRIGHT_TIMEOUT_OVERRIDES_MS = {
    "sfrecpark.org": 60000,
    "civicplus.com": 60000,
    "cityofsacramento.gov": 60000,
    "alamedaca.gov": 60000,
    "elsegundorecparks.org": 60000,
    "whittierprcs.org": 60000,
}

def smart_fetch(url: str, timeout: int = 15) -> tuple[str | None, str]:
    """Fetch URL via Playwright for known JS/blocked hosts, urllib otherwise.
    Returns (text, why). text is None on failure."""
    host = (urllib.parse.urlparse(url).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    use_playwright = any(host.endswith(h) for h in PLAYWRIGHT_HOSTS)

    if use_playwright and PLAYWRIGHT_AVAILABLE:
        wait_s = 4.0
        timeout_ms = 30000
        for host_pattern, override_wait in PLAYWRIGHT_WAIT_OVERRIDES.items():
            if host.endswith(host_pattern):
                wait_s = override_wait
                break
        for host_pattern, override_timeout in PLAYWRIGHT_TIMEOUT_OVERRIDES_MS.items():
            if host.endswith(host_pattern):
                timeout_ms = override_timeout
                break
        try:
            text = playwright_fetch(url, selector=None, raw_html=False,
                                    wait_seconds=wait_s, timeout_ms=timeout_ms)
            return (text or ""), "ok_playwright"
        except Exception as e:
            return None, f"playwright_error:{type(e).__name__}"

    # urllib path
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": HEADERS_UA,
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        with urllib.request.urlopen(req, timeout=timeout) as r:
            if r.status != 200:
                return None, f"http_{r.status}"
            raw = r.read()
            charset = r.headers.get_content_charset() or "utf-8"
            body = raw.decode(charset, errors="replace")
            return body, "ok_urllib"
    except urllib.error.HTTPError as e:
        # 403 / 503 — often Cloudflare. Try Playwright if available.
        if e.code in (403, 503) and PLAYWRIGHT_AVAILABLE:
            try:
                wait_s = 4.0
                timeout_ms = 30000
                for host_pattern, override_wait in PLAYWRIGHT_WAIT_OVERRIDES.items():
                    if host.endswith(host_pattern):
                        wait_s = override_wait
                        break
                for host_pattern, override_timeout in PLAYWRIGHT_TIMEOUT_OVERRIDES_MS.items():
                    if host.endswith(host_pattern):
                        timeout_ms = override_timeout
                        break
                text = playwright_fetch(url, selector=None, raw_html=False,
                                        wait_seconds=wait_s, timeout_ms=timeout_ms)
                if text:
                    return text, f"ok_playwright_after_{e.code}"
            except Exception:
                pass
        return None, f"http_{e.code}"
    except Exception as e:
        return None, f"fetch_error:{type(e).__name__}"


def is_thin_or_blocked(text: str | None) -> bool:
    """Cloudflare / JS-shell detection. Triggers web_search fallback."""
    if not text or len(text) < 500:
        return True
    low = text.lower()[:5000]
    cf_keywords = [
        "performing security verification", "ray id", "just a moment",
        "checking your browser", "ddos protection", "enable javascript",
    ]
    return any(kw in low for kw in cf_keywords)


# ── P2 — Park-platform candidate builders (analog of _candidates_municode) ─

def _slug(name: str) -> str:
    """Dash-separated lowercase slug, alphanumerics only."""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")

def _slug_short(name: str) -> str:
    """Like _slug but drops common suffixes (state-beach, county-beach, etc.)"""
    s = _slug(name)
    for suffix in ("-state-beach", "-county-beach", "-municipal-beach",
                   "-state-park", "-beach", "-park"):
        if s.endswith(suffix):
            s = s[:-len(suffix)]
            break
    return s


def candidates_for_beach(beach: dict) -> list[dict]:
    """Returns list of {url, platform, auth} candidates for a beach.
    Each builder is platform-specific and generates 0..N URLs to try."""
    name = beach['name']
    state = (beach.get('state') or 'CA').upper()
    mng = (beach.get('mng_agncy') or '').lower()
    city = beach.get('city_name') or ''
    cands: list[dict] = []

    # ─── Phase 10b — operator-source-URL candidates ────────────────────
    # Pre-loaded by main() from operator_dogs_policy.source_url for each
    # operator attributed to this beach via entity_operator. These URLs
    # already passed Phase 10's policy_found gate — they're known to
    # contain dog-policy text. Auth=5 (highest) because operator-published
    # + already-vetted. The per-beach name_match gate filters to beaches
    # whose name actually appears on the page (applies_to_all=false ops
    # often have a URL specific to ONE of their beaches; this naturally
    # selects that beach and writes no_match for the others).
    for url in (beach.get('op_source_urls') or []):
        if url:
            cands.append({
                "url": url,
                "platform": "op_dogs_policy_source",
                "auth": 5,
            })

    # ─── Phase 10b — state-park-listing fallback ───────────────────────
    # For beaches in a state-park context (name suggests state ownership
    # or mng_agncy is a state-parks agency), include the canonical
    # state-parks-listing URL from scripts/state_park_urls.json. Auth=3
    # — useful but lower-priority than operator-specific URLs.
    name_lc = name.lower()
    is_state_park_beach = (
        "state park" in name_lc
        or "state beach" in name_lc
        or "state recreation" in name_lc
        or "state seashore" in name_lc
    )
    if is_state_park_beach and state in _STATE_PARK_URLS:
        entry = _STATE_PARK_URLS[state]
        if isinstance(entry, dict) and entry.get("url"):
            cands.append({
                "url": entry["url"],
                "platform": f"state_parks_listing_{state.lower()}",
                "auth": 3,
            })

    # ─── CDPR (California Dept of Parks and Recreation) ────────────────
    # Per-park pages live at parks.ca.gov/?page_id=N. N is per-park and not
    # derivable from name. Use the canonical dog page + park-search URL.
    if "california department of parks" in mng or "state beach" in name.lower() or "state park" in name.lower():
        cands.append({
            "url": "https://www.parks.ca.gov/?page_id=24290",  # CDPR pet policy
            "platform": "cdpr_pet_policy",
            "auth": 4,
        })
        # The site has a search like parks.ca.gov/?Page_id=24290&keyword=name
        # but no public site-search URL. Use Google site: as a fallback URL
        # source via web_search, not direct.
        # Per-park guess via parks.ca.gov/<slug>:
        slug = _slug_short(name)
        cands.append({
            "url": f"https://www.parks.ca.gov/{slug}",
            "platform": "cdpr_park_slug",
            "auth": 4,
        })

    # ─── City of San Diego ─────────────────────────────────────────────
    if "san diego, city of" in mng or city.lower() == "san diego":
        # Canonical SD beach-dog page (already proven gold)
        cands.append({
            "url": "https://www.sandiego.gov/park-and-recreation/parks/dogs/bchdog",
            "platform": "sd_beach_dogs",
            "auth": 4,
        })
        cands.append({
            "url": "https://www.sandiego.gov/park-and-recreation/parks/dogs/leash",
            "platform": "sd_leash",
            "auth": 4,
        })

    # ─── LA County Beaches & Harbors ───────────────────────────────────
    if "los angeles county" in mng or "los angeles, county of" in mng:
        slug = _slug_short(name)
        cands.append({
            "url": f"https://beaches.lacounty.gov/{slug}/",
            "platform": "la_county_beach",
            "auth": 4,
        })
        cands.append({
            "url": "https://beaches.lacounty.gov/dogs/",
            "platform": "la_county_dogs",
            "auth": 4,
        })

    # ─── City of Los Angeles ───────────────────────────────────────────
    if city.lower() == "los angeles":
        # LA RAP beach rules
        cands.append({
            "url": f"https://www.laparks.org/beach/{_slug_short(name)}",
            "platform": "la_rap_beach",
            "auth": 4,
        })

    # ─── Orange County (OC Parks) ──────────────────────────────────────
    if "orange, county of" in mng or "orange county" in mng.replace(",", ""):
        slug = _slug_short(name)
        cands.append({
            "url": f"https://ocparks.com/beaches/{slug}",
            "platform": "oc_parks",
            "auth": 3,
        })

    # ─── City of Coronado ──────────────────────────────────────────────
    if city.lower() == "coronado":
        cands.append({
            "url": "https://www.coronado.ca.us/services/animal_control/dog_beach",
            "platform": "coronado_dog_beach",
            "auth": 4,
        })

    # ─── City of Capitola ──────────────────────────────────────────────
    if city.lower() == "capitola":
        cands.append({
            "url": "https://www.cityofcapitola.org/animal-services/page/dog-beach-rules",
            "platform": "capitola_dog",
            "auth": 4,
        })

    # ─── City of Malibu ────────────────────────────────────────────────
    if city.lower() == "malibu":
        cands.append({
            "url": "https://www.malibucity.org/CivicAlerts.aspx?CID=Animals",
            "platform": "malibu_animals",
            "auth": 4,
        })

    # ─── City of Santa Barbara ─────────────────────────────────────────
    if city.lower() == "santa barbara":
        cands.append({
            "url": "https://www.santabarbaraca.gov/government/departments/parks-recreation/parks-facilities/parks/leash-laws",
            "platform": "sb_leash",
            "auth": 4,
        })
        cands.append({
            "url": f"https://www.santabarbaraca.gov/parks/{_slug_short(name)}",
            "platform": "sb_park",
            "auth": 4,
        })

    # ─── City of Santa Cruz ────────────────────────────────────────────
    if city.lower() == "santa cruz":
        cands.append({
            "url": "https://www.cityofsantacruz.com/government/city-departments/parks-recreation/parks-beaches/dogs",
            "platform": "sc_dogs",
            "auth": 4,
        })

    # ─── City of Newport Beach ─────────────────────────────────────────
    if city.lower() == "newport beach":
        cands.append({
            "url": "https://www.newportbeachca.gov/government/departments/recreation-senior-services/recreation-services/pet-friendly-newport-beach",
            "platform": "nb_pet_friendly",
            "auth": 4,
        })

    return cands


# ── LLM call with cite-required + optional web_search fallback ─────────

PROMPT_SYSTEM_TEMPLATE = (
    "You are extracting dog-policy information about '{name}{geo}'. "
    "Use ONLY the provided source page (or your web_search results if enabled). "
    "CRITICAL: only answer about THIS beach specifically. If the source mentions "
    "OTHER nearby beaches (e.g., a different beach in the same city), do NOT "
    "confuse those rules with this beach. If the source mentions this beach "
    "but does not address off-leash, return 'unknown'. If the source does not "
    "mention this beach by name (or its commonly-known abbreviation), return "
    "'no_match'."
)

PROMPT_USER_TEMPLATE = """SOURCE PAGE CONTENT:
{content}

QUESTION: For '{name}{geo}' SPECIFICALLY, is there ANY time window (any hours, any season, any specific designated area on the sand) when dogs are allowed OFF-LEASH on the sand? Time-windowed off-leash (e.g., "before 9am only") counts as YES.

Return JSON:
{{
  "answer": "yes" | "no" | "unknown" | "no_match",
  "quote": "<verbatim quote from source, 50-300 chars, MUST contain the beach name or a distinctive part of it>",
  "time_window": "<descriptive time/area window if yes; null if no/unknown>",
  "confidence": "high" | "medium" | "low"
}}"""


def call_llm(content: str, beach_name: str, city: str | None,
             enable_web_search: bool = False) -> dict:
    """Cite-required per-beach off-leash question.
    enable_web_search=True adds Anthropic's web_search_20250305 tool for
    Cloudflare/thin-content fallback (codify P4 pattern)."""
    geo = f", {city}" if city else ""
    sys_p = PROMPT_SYSTEM_TEMPLATE.format(name=beach_name, geo=geo)
    user_p = PROMPT_USER_TEMPLATE.format(name=beach_name, geo=geo,
                                         content=content[:30000])

    body = {
        "model": SONNET,
        "max_tokens": 900,
        "system": sys_p,
        "messages": [{"role": "user", "content": user_p}],
    }
    if enable_web_search:
        body["tools"] = [{
            "type": "web_search_20250305",
            "name": "web_search",
            "max_uses": 3,
        }]

    import urllib.request
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode("utf-8"), method="POST",
    )
    req.add_header("x-api-key", ANTHROPIC_KEY)
    req.add_header("anthropic-version", "2023-06-01")
    req.add_header("content-type", "application/json")
    with urllib.request.urlopen(req, timeout=180) as r:
        resp = json.loads(r.read().decode("utf-8"))

    text_blocks = [b.get("text", "") for b in resp.get("content", []) if b.get("type") == "text"]
    raw = text_blocks[-1] if text_blocks else ""
    raw = raw.strip()
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
        m = re.search(r'"answer"\s*:\s*"([^"]+)"', raw)
        return {"answer": m.group(1) if m else "parse_error", "raw": raw[:300]}


# ── Main ───────────────────────────────────────────────────────────────

DEFAULT_CANDIDATES = [
    8472, 8333, 8715, 705, 8340, 8867, 9069, 8298, 8430, 8301,
    8564, 8351, 9720, 8247, 8254, 9334, 8386, 623, 8554, 8546,
    6247, 4916, 8457, 1474, 6136,
]


def write_sentinel(cur, fid: int, url: str | None, why: str) -> None:
    """P5 — write a no-result sentinel BEP row so freshness guards skip
    re-extracting next run. Pattern mirrors operator_extractions_no_url_sentinel."""
    claimed = {
        "status": "no_url_found",
        "why": why,
        "extracted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    cur.execute("""
        INSERT INTO public.beach_enrichment_provenance
            (gold_fid, field_group, source, confidence, claimed_values,
             source_url, is_canonical, relevance_verified, notes, updated_at)
        VALUES (%s, 'dogs', 'per_beach_offleash_v2_no_result', 0.10, %s::jsonb,
                %s, false, false,
                'extract_per_beach_offleash_v2.py — sentinel; no candidate URL passed gates',
                now())
        ON CONFLICT (gold_fid, field_group, source) WHERE gold_fid IS NOT NULL
        DO UPDATE SET
            claimed_values = EXCLUDED.claimed_values,
            source_url = EXCLUDED.source_url,
            updated_at = now()
    """, (fid, json.dumps(claimed), url))


def write_canonical(cur, fid: int, winning: dict) -> None:
    """Write yes-with-cite extraction as canonical BEP row."""
    resp = winning['resp']
    claimed = {
        'allowed': 'yes',
        'leash_required': 'off_leash',
        'area_sand': 'off_leash',
        'this_beach_has_off_leash_anytime': 'yes',
        'source_quote': resp.get('quote'),
        'time_window': resp.get('time_window'),
        'confidence_text': resp.get('confidence'),
        'extraction_method': winning.get('platform'),
    }
    conf = {'high': 0.92, 'medium': 0.78, 'low': 0.62}.get(
        resp.get('confidence', 'medium'), 0.78)
    cur.execute("""
        INSERT INTO public.beach_enrichment_provenance
            (gold_fid, field_group, source, confidence, claimed_values,
             source_url, is_canonical, relevance_verified, notes, updated_at)
        VALUES (%s, 'dogs', 'per_beach_offleash_v2', %s, %s::jsonb,
                %s, true, true,
                'extract_per_beach_offleash_v2.py — per-beach explicit off-leash with cite (codify-port URL discovery)',
                now())
        ON CONFLICT (gold_fid, field_group, source) WHERE gold_fid IS NOT NULL
        DO UPDATE SET
            confidence = EXCLUDED.confidence,
            claimed_values = EXCLUDED.claimed_values,
            source_url = EXCLUDED.source_url,
            is_canonical = true,
            relevance_verified = true,
            updated_at = now()
    """, (fid, conf, json.dumps(claimed), winning['url']))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--fids", type=str, default=None)
    ap.add_argument("--max-cands-per-beach", type=int, default=5)
    ap.add_argument("--no-web-search", action="store_true",
                    help="Disable Anthropic web_search fallback")
    args = ap.parse_args()

    if args.fids:
        target_fids = [int(x.strip()) for x in args.fids.split(",")]
    else:
        target_fids = DEFAULT_CANDIDATES
    if args.limit:
        target_fids = target_fids[:args.limit]

    conn = connect()
    conn.set_client_encoding("UTF8")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Phase 10b: also pull operator_dogs_policy.source_url per beach via
    # entity_operator. Some beaches have 0 attributed ops with URLs; some
    # have 1+. Array-aggregated to a single column.
    cur.execute("""
        SELECT g.fid, g.name, g.county_name, g.state, g.lat, g.lon,
               cu.mng_agncy, j.name AS city_name,
               array_remove(
                 array_agg(DISTINCT odp.source_url),
                 NULL
               ) AS op_source_urls
          FROM public.beaches_gold g
          LEFT JOIN public.cpad_units cu ON cu.unit_id = g.cpad_unit_id
          LEFT JOIN public.jurisdictions j ON j.id = g.c1_jurisdiction_id
          LEFT JOIN public.entity_operator eo
                 ON eo.entity_type = 'beach' AND eo.entity_id = g.fid
          LEFT JOIN public.operator_dogs_policy odp
                 ON odp.operator_id = eo.operator_id
                AND odp.policy_found = true
                AND odp.source_url IS NOT NULL
         WHERE g.fid = ANY(%s)
         GROUP BY g.fid, g.name, g.county_name, g.state, g.lat, g.lon,
                  cu.mng_agncy, j.name
    """, (target_fids,))
    beaches = {b['fid']: b for b in cur.fetchall()}

    print(f"v2 extractor — targeting {len(beaches)} beaches; apply={args.apply}")

    summary = {"yes_direct": 0, "yes_web_search": 0, "no": 0,
               "unknown": 0, "no_match": 0, "sentinel": 0, "skipped": 0}
    yes_fids: list[int] = []

    for fid in target_fids:
        if fid not in beaches:
            continue
        b = dict(beaches[fid])
        name = b['name']
        city = b['city_name']
        mng = b.get('mng_agncy') or '-'
        print(f"\n[{fid}] {name}  city={city or '-'}  mng={mng[:40]}")

        cands = candidates_for_beach(b)
        cands = sorted(cands, key=lambda c: -c['auth'])[:args.max_cands_per_beach]
        print(f"  candidates: {len(cands)}")

        winning = None
        last_ans = "no_candidates"
        ws_tried = False

        for c in cands:
            if not is_url_deep_enough(c['url']):
                print(f"    [-] {c['url'][:70]}  (not deep enough)")
                continue
            text_raw, why = smart_fetch(c['url'])
            if text_raw is None:
                print(f"    [-] {c['url'][:70]}  (fetch fail: {why})")
                continue
            # If thin/blocked AND web_search allowed, try web_search fallback (P4)
            if is_thin_or_blocked(text_raw) and not args.no_web_search and not ws_tried:
                ws_tried = True
                print(f"    [~] {c['url'][:70]}  (thin/blocked → web_search fallback)")
                try:
                    resp = call_llm(
                        f"[ORIGINAL URL was thin/blocked: {c['url']}]\nUse web_search to find the dog policy for '{name}, {city or b.get('county_name','CA')}'.",
                        name, city, enable_web_search=True,
                    )
                except Exception as e:
                    print(f"    [!] web_search error: {e}")
                    continue
                ans = resp.get('answer')
                qt = (resp.get('quote') or '')[:80]
                print(f"    [{ans}] (web_search) quote: {qt}")
                last_ans = ans
                if ans == 'yes' and name_match(resp.get('quote', ''), name):
                    winning = {'url': c['url'], 'resp': resp,
                               'platform': c['platform'] + '+websearch'}
                    summary['yes_web_search'] += 1
                    break
                continue
            # Normal path: strip + gate + LLM
            html = text_raw
            text = strip_html(html) if "<" in html else html[:60000]
            if not name_match(text, name):
                print(f"    [-] {c['url'][:70]}  (name not on page)")
                continue
            if not geo_gate(text, city, b['county_name']):
                print(f"    [-] {c['url'][:70]}  (geo gate)")
                continue
            try:
                resp = call_llm(text, name, city)
            except Exception as e:
                print(f"    [!] LLM error: {e}")
                continue
            ans = resp.get('answer')
            qt = (resp.get('quote') or '')[:80]
            print(f"    [{ans}] auth={c['auth']} via {c['platform']} url={c['url'][:50]}")
            print(f"        quote: {qt}")
            last_ans = ans
            if ans == 'yes' and name_match(resp.get('quote', ''), name):
                winning = {'url': c['url'], 'resp': resp, 'platform': c['platform']}
                summary['yes_direct'] += 1
                break

        # P4 — beach-level web_search escalation when ALL candidates failed
        # (no fetch passed gates). Mirrors codify Step 6.8 escalation.
        if not winning and not args.no_web_search and not ws_tried:
            print(f"    [~] all candidates exhausted → beach-level web_search")
            try:
                resp = call_llm(
                    f"You are extracting dog off-leash policy for '{name}', "
                    f"{city or b.get('county_name','CA') + ' County'}, California. "
                    f"All deterministic URL candidates failed. "
                    f"Use web_search to find the operative policy from an "
                    f"AUTHORITATIVE source (.gov, .ca.us, park system website, "
                    f"NOT bringfido/yelp/tripadvisor). Return the verbatim "
                    f"quote that proves the beach allows or prohibits off-leash.",
                    name, city, enable_web_search=True,
                )
                ans = resp.get('answer')
                qt = (resp.get('quote') or '')[:80]
                print(f"    [{ans}] (web_search escalation) quote: {qt}")
                last_ans = ans
                if ans == 'yes' and name_match(resp.get('quote', ''), name):
                    winning = {'url': '[web_search]', 'resp': resp,
                               'platform': 'web_search_escalation'}
                    summary['yes_web_search'] += 1
            except Exception as e:
                print(f"    [!] web_search escalation error: {e}")

        if winning:
            yes_fids.append(fid)
            if args.apply:
                write_canonical(cur, fid, winning)
                conn.commit()
                cur.execute("SELECT public.compute_beach_field_consensus(%s)", (fid,))
                cur.execute("SELECT public.promote_canonical_dogs_to_beach_dog_policy(%s)", (fid,))
                conn.commit()
        else:
            if last_ans == 'no':            summary['no'] += 1
            elif last_ans == 'unknown':     summary['unknown'] += 1
            elif last_ans == 'no_match':    summary['no_match'] += 1
            elif last_ans == 'no_candidates': summary['skipped'] += 1
            else:                           summary['sentinel'] += 1
            # P5 — write sentinel (skip if dry-run)
            if args.apply:
                write_sentinel(cur, fid, cands[0]['url'] if cands else None,
                               why=f"last_ans={last_ans}; tried={len(cands)} cands")
                conn.commit()

    print(f"\n{'='*60}\nSUMMARY:")
    for k, v in summary.items():
        print(f"  {k:18} = {v}")
    if yes_fids:
        print(f"\nYES fids ({len(yes_fids)}): {','.join(str(f) for f in yes_fids)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
