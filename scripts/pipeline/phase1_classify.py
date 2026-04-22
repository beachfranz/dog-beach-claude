#!/usr/bin/env python3
"""
Phase 1 — Dog policy classification for beaches_staging.

For each beach:
  1. Run staged Tavily searches (official-first → broad → geo → dog_friendly → restriction)
  2. Fetch and pattern-match each candidate page
  3. Accumulate ALL evidence — no early stopping
  4. Require 2+ independent sources agreeing for 'confirmed' confidence
  5. Single source = 'probable'; conflict = 'needs_review'

Writes back to beaches_staging:
  dogs_allowed, access_rule, policy_source_url, policy_confidence,
  policy_notes (matched_text + sources), review_status, review_notes,
  quality_tier → 'gold' (confirmed/probable) or stays 'silver' (needs_review/unknown)

Usage:
  python phase1_classify.py --county "Orange" --state "CA"
  python phase1_classify.py --county "Orange" --state "CA" --dry-run
  python phase1_classify.py --county "Orange" --state "CA" --resume
  python phase1_classify.py --county "Orange" --state "CA" --limit 5
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
import time
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import warnings
import httpx
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from dotenv import load_dotenv

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
from rapidfuzz import fuzz
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter
from tqdm import tqdm

load_dotenv(Path(__file__).parent / ".env")

# ── Config ────────────────────────────────────────────────────────────────────

TAVILY_API_KEY   = os.environ["TAVILY_API_KEY"]
SUPABASE_URL     = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

SEARCH_RESULT_LIMIT   = 10
FETCH_CHAR_LIMIT      = 25_000
SEARCH_TIMEOUT        = 30.0
FETCH_TIMEOUT         = 30.0
CHECKPOINT_EVERY      = 10
WORKERS               = 4
MAX_QUERIES_PER_BEACH = 15
MAX_PAGES_PER_BEACH   = 8
MIN_SOURCES_CONFIRMED = 2   # sources that must agree for 'confirmed'

USER_AGENT = "dog-beach-scout/1.0 (contact: franz@franzfunk.com)"

NEGATIVE_QUERY_TERMS = "-hotel -restaurant -apartment -yelp -wedding -resort -rv -airbnb -tripadvisor"

# ── Domain classification ─────────────────────────────────────────────────────

DOMAIN_CLASS_WEIGHTS = {
    "official":        1.00,
    "parks_authority": 0.95,
    "tourism":         0.75,
    "local_media":     0.60,
    "aggregator":      0.45,
    "blog":            0.30,
    "other":           0.20,
}

GENERIC_OFFICIAL_DOMAINS = [
    "nps.gov", "recreation.gov", "fws.gov", "blm.gov", "fs.usda.gov",
]

STATE_OFFICIAL_DOMAINS: dict[str, list[str]] = {
    "AL": ["alabama.gov", "alapark.com", "dcnr.alabama.gov"],
    "AK": ["alaska.gov", "dnr.alaska.gov", "parks.alaska.gov"],
    "AZ": ["az.gov", "azstateparks.com"],
    "AR": ["arkansas.gov", "arkansasstateparks.com"],
    "CA": ["ca.gov", "parks.ca.gov", "www.parks.ca.gov"],
    "CO": ["colorado.gov", "cpw.state.co.us"],
    "CT": ["ct.gov", "portal.ct.gov"],
    "DE": ["delaware.gov", "destateparks.com"],
    "FL": ["fl.gov", "floridastateparks.org", "dep.state.fl.us", "myfwc.com"],
    "GA": ["ga.gov", "gastateparks.org"],
    "HI": ["hawaii.gov", "dlnr.hawaii.gov"],
    "ID": ["idaho.gov", "parksandrecreation.idaho.gov"],
    "IL": ["illinois.gov", "dnr.illinois.gov"],
    "IN": ["in.gov", "dnr.in.gov"],
    "LA": ["louisiana.gov", "lastateparks.com"],
    "MA": ["mass.gov"],
    "MD": ["maryland.gov", "dnr.maryland.gov"],
    "ME": ["maine.gov"],
    "MI": ["michigan.gov"],
    "MN": ["mn.gov", "dnr.state.mn.us"],
    "MS": ["ms.gov", "mdwfp.com"],
    "NC": ["nc.gov", "ncparks.gov"],
    "NJ": ["nj.gov", "dep.nj.gov", "njparksandforests.org"],
    "NY": ["ny.gov", "parks.ny.gov"],
    "OR": ["oregon.gov", "stateparks.oregon.gov"],
    "RI": ["ri.gov", "dem.ri.gov"],
    "SC": ["sc.gov", "southcarolinaparks.com"],
    "TX": ["texas.gov", "tpwd.texas.gov"],
    "VA": ["virginia.gov", "dcr.virginia.gov"],
    "WA": ["wa.gov", "parks.wa.gov"],
    "WI": ["wisconsin.gov", "dnr.wisconsin.gov"],
}

# Known official parks/rec sites that use .com TLDs
TRUSTED_PARKS_DOMAINS = {
    "ocparks.com", "laparks.org", "sfrecpark.org", "nycgovparks.org",
    "ebparks.org", "sdparks.org", "mprb.org",
}

# .gov domains that are document/legal repositories, not beach policy sources
BLOCKED_GOV_DOMAINS = {
    "ceqanet.lci.ca.gov",  # CA environmental review docs — matches "restricted" on construction language
}

def classify_domain(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if host in BLOCKED_GOV_DOMAINS:
        return "other"
    if any(x in host for x in [".gov", ".us", ".mil"]):
        return "official"
    # parks_authority: whitelist known official parks orgs; also .org/.edu with parks keywords
    # Exclude .com fan/aggregator sites even if they contain "parks" in the name
    if host in TRUSTED_PARKS_DOMAINS:
        return "parks_authority"
    if any(x in host for x in ["parks", "stateparks", "recreation", "wildlife"]):
        if ".org" in host or ".edu" in host:
            return "parks_authority"
        # .com sites with parks in hostname → aggregator (e.g. funorangecountyparks.com)
        return "aggregator"
    if any(x in host for x in ["visit", "tourism", "chamber", "discover"]):
        return "tourism"
    if any(x in host for x in ["times", "tribune", "patch", "news", "herald"]):
        return "local_media"
    if any(x in host for x in ["bringfido", "tripadvisor", "yelp", "alltrails", "dogfriendly"]):
        return "aggregator"
    if any(x in host for x in ["blog", "wordpress", "substack", "medium"]):
        return "blog"
    return "other"

# ── Policy patterns ───────────────────────────────────────────────────────────

DOG_FRIENDLY_PATTERNS = [
    r"\bdog[- ]friendly\b",
    r"\bdogs?\s+(are\s+)?welcome\b",
    r"\bdog beach\b",
    r"\bpopular dog beach\b",
    r"\bbring your dog\b",
    r"\boff[- ]leash dog beach\b",
]

PROHIBITED_PATTERNS = [
    r"\bdogs?\s+(are\s+)?not\s+allowed\b",
    r"\bno dogs?\b",
    r"\bdogs?\s+prohibited\b",
    r"\bdogs?\s+forbidden\b",
    r"\bpets?\s+prohibited\b",
    r"\bpets?\s+not\s+allowed\b",
]

RESTRICTED_PATTERNS = [
    r"\bdogs?\s+allowed\s+only\b",
    r"\bon[- ]leash\b",
    r"\bleashed\b",
    r"\bbefore\s+\d{1,2}(:\d{2})?\s*(am|pm)\b",
    r"\bafter\s+\d{1,2}(:\d{2})?\s*(am|pm)\b",
    r"\bseasonal\b",
    r"\bdesignated area\b",
    r"\bdesignated section\b",
    r"\bmust be leashed\b",
    r"\bleash required\b",
]

ALLOWED_PATTERNS = [
    r"\bdogs?\s+allowed\b",
    r"\bpets?\s+allowed\b",
    r"\bdogs?\s+permitted\b",
    r"\bpets?\s+permitted\b",
]

NEGATION_CONTEXT = [
    r"boardwalk", r"parking lot", r"trail", r"picnic area", r"except service animals",
]

def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()

def find_hits(text: str, patterns: list[str]) -> list[str]:
    hits = []
    lowered = text.lower()
    for pat in patterns:
        for m in re.finditer(pat, lowered, re.IGNORECASE):
            start = max(0, m.start() - 120)
            end   = min(len(text), m.end() + 120)
            hits.append(normalize(text[start:end]))
    return hits

def classify_text(text: str) -> tuple[Optional[str], str]:
    text = normalize(text)
    friendly  = find_hits(text, DOG_FRIENDLY_PATTERNS)
    prohibited = find_hits(text, PROHIBITED_PATTERNS)
    restricted = find_hits(text, RESTRICTED_PATTERNS)
    allowed    = find_hits(text, ALLOWED_PATTERNS)
    if friendly:   return "dog_friendly", friendly[0]
    if prohibited: return "prohibited",   prohibited[0]
    if restricted: return "restricted",   restricted[0]
    if allowed:    return "allowed",      allowed[0]
    return None, ""

# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class BeachRecord:
    id:           int
    display_name: str
    city:         str
    county:       str
    state:        str
    latitude:     float
    longitude:    float
    formatted_address: str

@dataclass
class SearchHit:
    title:       str
    url:         str
    snippet:     str = ""
    rank:        int = 0
    query:       str = ""
    query_stage: str = ""

@dataclass
class Evidence:
    url:          str
    domain_class: str
    policy:       str
    matched_text: str
    score:        float
    query_stage:  str

@dataclass
class ClassificationResult:
    beach_id:      int
    policy:        str          # dog_friendly | allowed | restricted | prohibited | unknown
    confidence:    str          # confirmed | probable | needs_review
    primary_url:   str
    matched_text:  str
    all_sources:   list[str]
    conflict:      bool
    review_notes:  str
    evidence_count: int

# ── Supabase client ───────────────────────────────────────────────────────────

def sb_headers() -> dict[str, str]:
    return {
        "apikey":        SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }

def fetch_beaches(county: str, state: str, limit: Optional[int] = None, ids: Optional[list[int]] = None) -> list[BeachRecord]:
    from urllib.parse import quote
    if ids:
        id_list = ",".join(str(i) for i in ids)
        params = (
            f"id=in.({id_list})"
            f"&select=id,display_name,city,county,state,latitude,longitude,formatted_address"
            f"&order=id"
        )
    else:
        params = (
            f"county=eq.{quote(county)}&state=eq.{quote(state)}"
            f"&or=(dedup_status.is.null,dedup_status.eq.reviewed)"
            f"&select=id,display_name,city,county,state,latitude,longitude,formatted_address"
            f"&order=id"
        )
        if limit:
            params += f"&limit={limit}"
    url = f"{SUPABASE_URL}/rest/v1/beaches_staging?{params}"
    resp = httpx.get(url, headers=sb_headers(), timeout=30)
    resp.raise_for_status()
    rows = resp.json()
    return [
        BeachRecord(
            id=r["id"],
            display_name=r["display_name"] or "",
            city=r.get("city") or "",
            county=r.get("county") or "",
            state=r.get("state") or "",
            latitude=float(r["latitude"]),
            longitude=float(r["longitude"]),
            formatted_address=r.get("formatted_address") or "",
        )
        for r in rows
    ]

def write_result(result: ClassificationResult, dry_run: bool, new_display_name: str | None = None) -> None:
    dogs_allowed = result.policy in ("dog_friendly", "allowed", "restricted")
    access_rule  = {
        "dog_friendly": "off_leash",
        "allowed":      "on_leash",
        "restricted":   "on_leash",
        "prohibited":   None,
        "unknown":      None,
    }.get(result.policy)

    review_status = None
    if result.confidence == "needs_review" or result.conflict:
        review_status = "Needs Review"

    quality_tier = "silver"
    if result.confidence in ("confirmed", "probable") and not result.conflict and result.policy != "unknown":
        quality_tier = "gold"

    payload: dict[str, Any] = {
        "dogs_allowed":       dogs_allowed if result.policy != "unknown" else None,
        "access_rule":        access_rule,
        "policy_source_url":  result.primary_url or None,
        "policy_confidence":  result.confidence,
        "policy_notes":       result.matched_text[:500] if result.matched_text else None,
        "review_status":      review_status,
        "review_notes":       result.review_notes or None,
        "quality_tier":       quality_tier,
        "updated_at":         "now()",
    }
    if new_display_name:
        payload["display_name"] = new_display_name

    payload = {k: v for k, v in payload.items() if v is not None or k in ("dogs_allowed", "access_rule")}

    if dry_run:
        if new_display_name:
            print(f"  [dry-run] would rename id={result.beach_id} display_name -> '{new_display_name}'")
        print(f"  [dry-run] would write to id={result.beach_id}: {payload}")
        return

    url = f"{SUPABASE_URL}/rest/v1/beaches_staging?id=eq.{result.beach_id}"
    resp = httpx.patch(url, headers=sb_headers(), json=payload, timeout=15)
    resp.raise_for_status()

# ── Tavily search ─────────────────────────────────────────────────────────────

@retry(wait=wait_exponential_jitter(1, 8), stop=stop_after_attempt(4),
       retry=retry_if_exception_type(httpx.HTTPError))
async def tavily_search(
    client: httpx.AsyncClient,
    query: str,
    stage: str,
    include_domains: list[str],
) -> list[SearchHit]:
    payload: dict[str, Any] = {
        "api_key":      TAVILY_API_KEY,
        "query":        query,
        "search_depth": "basic" if stage in ("official_first", "exact_broad") else "advanced",
        "max_results":  SEARCH_RESULT_LIMIT,
        "include_answer": False,
        "include_raw_content": False,
        "topic": "general",
    }
    if include_domains:
        payload["include_domains"] = include_domains[:10]
    resp = await client.post("https://api.tavily.com/search", json=payload, timeout=SEARCH_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    hits = []
    for i, item in enumerate(data.get("results", [])[:SEARCH_RESULT_LIMIT], 1):
        url = item.get("url")
        if url:
            hits.append(SearchHit(
                title=normalize(item.get("title", "")),
                url=url,
                snippet=normalize(item.get("content", "")),
                rank=i,
                query=query,
                query_stage=stage,
            ))
    return hits

# ── Page fetch ────────────────────────────────────────────────────────────────

async def fetch_page(client: httpx.AsyncClient, url: str, cache_dir: Path) -> str:
    key = cache_dir / f"{hashlib.sha1(url.encode()).hexdigest()}.txt"
    if key.exists():
        return key.read_text(encoding="utf-8", errors="ignore")
    try:
        resp = await client.get(url, headers={"User-Agent": USER_AGENT},
                                timeout=FETCH_TIMEOUT, follow_redirects=True)
        resp.raise_for_status()
        ctype = resp.headers.get("content-type", "")
        if "pdf" in ctype.lower():
            key.write_text("")
            return ""
        soup = BeautifulSoup(resp.text, "lxml")
        for tag in soup(["script", "style", "noscript", "svg", "header", "footer", "nav", "form"]):
            tag.extract()
        texts = []
        for selector in ["main", "article", "body"]:
            node = soup.select_one(selector)
            if node:
                texts.append(node.get_text(" ", strip=True))
                break
        if not texts:
            texts.append(soup.get_text(" ", strip=True))
        text = normalize(" ".join(texts))[:FETCH_CHAR_LIMIT]
    except Exception:
        text = ""
    key.write_text(text, encoding="utf-8")
    return text

# ── Generic name detection ────────────────────────────────────────────────────

GENERIC_BEACH_WORDS = {
    "main", "north", "south", "east", "west", "upper", "lower", "central",
    "city", "town", "state", "public", "municipal", "park", "pier", "little",
    "big", "long", "sandy", "rocky", "hidden", "secret", "vista", "scenic",
    "ocean", "bay", "cove", "point", "shore", "surf", "sea", "lake",
}

def is_generic_name(name: str) -> bool:
    """True if the beach name is too common to search unambiguously."""
    words = [w.lower().rstrip("'s") for w in name.split() if w.lower() != "beach"]
    if not words:
        return True
    # Single non-'beach' word that is a known generic term
    if len(words) == 1 and words[0] in GENERIC_BEACH_WORDS:
        return True
    # Exactly two words total (e.g. "Main Beach", "North Beach") — almost always ambiguous
    if len(name.split()) <= 2:
        return True
    return False

def search_name(rec: BeachRecord) -> tuple[str, bool]:
    """
    Return (name_to_use_in_queries, was_disambiguated).
    For generic names, prefix the city to disambiguate.
    """
    base = normalize(rec.display_name)
    if is_generic_name(base) and rec.city:
        return f"{rec.city} {base}", True
    return base, False

# ── Query builder ─────────────────────────────────────────────────────────────

def build_queries(rec: BeachRecord) -> list[tuple[str, str, list[str]]]:
    """Returns list of (stage, query, include_domains)."""
    name, _ = search_name(rec)
    city  = rec.city
    state = rec.state

    official_domains = GENERIC_OFFICIAL_DOMAINS + STATE_OFFICIAL_DOMAINS.get(state, [])
    if city:
        slug = re.sub(r"[^a-z0-9]", "", city.lower())
        official_domains += [f"{slug}.gov", f"cityof{slug}.org", f"ci.{slug}.ca.us"]

    queries: list[tuple[str, str, list[str]]] = []

    # Stage 1: official-first
    for q in [
        f'"{name}" dogs',
        f'"{name}" beach dogs',
        f'"{name}" dog policy',
    ]:
        queries.append(("official_first", q, official_domains))

    # Stage 2: exact broad
    for q in [
        f'"{name}" dogs allowed {NEGATIVE_QUERY_TERMS}',
        f'"{name}" dog policy {NEGATIVE_QUERY_TERMS}',
        f'"{name}" beach dogs {NEGATIVE_QUERY_TERMS}',
    ]:
        queries.append(("exact_broad", q, []))

    # Stage 3: geo disambiguation
    if city:
        for q in [
            f'"{name}" {city} {state} dogs allowed {NEGATIVE_QUERY_TERMS}',
            f'"{name}" {city} dog policy {NEGATIVE_QUERY_TERMS}',
        ]:
            queries.append(("geo_disambiguation", q, []))

    # Stage 4: dog-friendly probe
    for q in [
        f'"{name}" "dog friendly" {NEGATIVE_QUERY_TERMS}',
        f'"{name}" "dogs welcome" {NEGATIVE_QUERY_TERMS}',
        f'"{name}" "dog beach" {NEGATIVE_QUERY_TERMS}',
    ]:
        queries.append(("dog_friendly_probe", q, official_domains))

    # Stage 5: restriction probe
    for q in [
        f'"{name}" dogs leash {NEGATIVE_QUERY_TERMS}',
        f'"{name}" dogs prohibited {NEGATIVE_QUERY_TERMS}',
        f'"{name}" seasonal dog restrictions {NEGATIVE_QUERY_TERMS}',
    ]:
        queries.append(("restriction_probe", q, official_domains))

    # Dedupe
    seen: set[str] = set()
    deduped = []
    for stage, q, domains in queries:
        if q not in seen:
            seen.add(q)
            deduped.append((stage, q, domains))
    return deduped[:MAX_QUERIES_PER_BEACH]

# ── Scoring ───────────────────────────────────────────────────────────────────

def score_hit(rec: BeachRecord, hit: SearchHit, page_text: str, policy: str) -> float:
    score = DOMAIN_CLASS_WEIGHTS.get(classify_domain(hit.url), 0.2)
    hay   = f"{hit.title} {hit.snippet} {page_text[:5000]}".lower()
    name  = rec.display_name.lower()

    if name and name in hay:
        score += 0.35
    elif name and fuzz.partial_ratio(name, hay) >= 80:
        score += 0.18

    if rec.city and rec.city.lower() in hay:
        score += 0.12

    if any(k in hay for k in ["dog", "dogs", "pet", "pets"]):
        score += 0.08
    if any(k in hay for k in ["beach rules", "policy", "hours", "allowed", "prohibited", "leash"]):
        score += 0.08

    if policy == "prohibited":   score += 0.30
    elif policy == "restricted": score += 0.25
    elif policy == "dog_friendly": score += 0.20
    elif policy == "allowed":    score += 0.12

    if any(re.search(ctx, hay) for ctx in NEGATION_CONTEXT):
        score -= 0.30

    score += max(0, (12 - hit.rank)) * 0.01
    return score

# ── Name gate ─────────────────────────────────────────────────────────────────

def _is_ca_official(url: str) -> bool:
    """True for California state/local .gov domains — inherently in-state."""
    host = urlparse(url).netloc.lower()
    # CA state agencies: parks.ca.gov, coastal.ca.gov, etc.
    if ".ca.gov" in host:
        return True
    # CA city domains follow pattern <cityname>ca.gov (newportbeachca.gov, huntingtonbeachca.gov)
    if re.search(r"ca\.gov$", host):
        return True
    return False

def _name_match(name: str, hay: str) -> bool:
    """True if name appears in hay. Tighter fuzzy threshold for short names."""
    if name in hay:
        return True
    # Short names (≤2 non-beach words) match too broadly — raise the bar
    words = [w for w in name.split() if w != "beach"]
    threshold = 85 if len(words) <= 2 else 70
    return fuzz.partial_ratio(name, hay) >= threshold

def passes_name_gate(rec: BeachRecord, hit: SearchHit, page_text: str) -> bool:
    domain_cls = classify_domain(hit.url)
    hay  = f"{hit.title} {hit.snippet} {page_text[:4000]}".lower()
    name = rec.display_name.lower()
    state  = rec.state.lower()  if rec.state  else ""
    county = rec.county.lower() if rec.county else ""

    # CA state/local .gov: domain is inherently California — skip all geo checks
    # (avoids filtering newportbeachca.gov pages that don't say "California" explicitly)
    if _is_ca_official(hit.url):
        return _name_match(name, hay)

    state_in_hay  = bool(state  and state  in hay)
    county_in_hay = bool(county and county in hay)

    # All sources must mention the state — blocks cross-state collisions
    # (SC parks, NC Sunset Beach) and cross-country federal sites (fws.gov Parker River MA)
    if not state_in_hay:
        return False

    # Non-official sources also require county — prevents within-CA wrong-location matches
    # (NorCal NPS pages, aggregators covering multiple states)
    # Official sources (.gov) already verified by state check; county would false-negative
    # legitimate city gov pages that focus locally and omit county name
    if domain_cls != "official" and not county_in_hay:
        return False

    # Tighter fuzzy threshold for short/ambiguous names
    return _name_match(name, hay)

# ── Snippet pre-screening (opt #2) ────────────────────────────────────────────

def snippet_policy(hit: SearchHit) -> Optional[str]:
    """Quick policy classification from title+snippet alone — no page fetch needed."""
    text = f"{hit.title} {hit.snippet}"
    policy, _ = classify_text(text)
    return policy

# ── City-level policy cache (opt #4) ─────────────────────────────────────────

# Populated as we process beaches; key = (city, state), value = Evidence
_city_policy_cache: dict[tuple[str, str], Evidence] = {}

def get_cached_city_policy(rec: BeachRecord) -> Optional[Evidence]:
    return _city_policy_cache.get((rec.city, rec.state))

def cache_city_policy(rec: BeachRecord, ev: Evidence) -> None:
    key = (rec.city, rec.state)
    existing = _city_policy_cache.get(key)
    # Only cache authoritative sources; prefer higher-scored ones
    if ev.domain_class in ("official", "parks_authority"):
        if existing is None or ev.score > existing.score:
            _city_policy_cache[key] = ev

# ── Per-beach research ────────────────────────────────────────────────────────

AUTHORITATIVE = {"official", "parks_authority"}
HIGH_CONFIDENCE_THRESHOLD = 1.5  # score above which a single auth source triggers early exit

async def research_beach(
    client: httpx.AsyncClient,
    rec: BeachRecord,
    cache_dir: Path,
) -> tuple[ClassificationResult, str | None]:
    """Returns (result, new_display_name_or_None)."""
    _, was_disambiguated = search_name(rec)
    queries = build_queries(rec)
    seen_urls: set[str] = set()
    evidence_list: list[Evidence] = []
    current_stage = None
    stage_auth_found = False  # tracks if current/prior stages found authoritative source

    for stage, query, include_domains in queries:
        # Opt #3: if we've moved past official_first/exact_broad and already have a
        # high-confidence authoritative hit, skip remaining stages
        if stage not in ("official_first", "exact_broad") and stage != current_stage:
            auth_ev = [e for e in evidence_list if e.domain_class in AUTHORITATIVE]
            if auth_ev and not _auth_conflict(auth_ev):
                best_auth = max(auth_ev, key=lambda e: e.score)
                if best_auth.score >= HIGH_CONFIDENCE_THRESHOLD:
                    break  # sufficient authoritative evidence found — skip probes

        current_stage = stage

        try:
            hits = await tavily_search(client, query, stage, include_domains)
        except Exception:
            continue

        for hit in hits:
            if hit.url in seen_urls:
                continue
            seen_urls.add(hit.url)

            # Opt #2: snippet pre-screening — classify snippet before fetching page
            snippet_pol = snippet_policy(hit)
            domain_cls  = classify_domain(hit.url)

            # If snippet gives no signal AND source is low-authority, skip fetch
            if snippet_pol is None and domain_cls not in AUTHORITATIVE:
                continue

            page_text = await fetch_page(client, hit.url, cache_dir)
            if not page_text:
                continue

            if not passes_name_gate(rec, hit, page_text):
                continue

            policy, matched_text = classify_text(page_text)
            if not policy:
                # Fall back to snippet classification if page text is unhelpful
                if snippet_pol:
                    policy = snippet_pol
                    _, matched_text = classify_text(f"{hit.title} {hit.snippet}")
                    if not matched_text:
                        continue
                else:
                    continue

            ev_score = score_hit(rec, hit, page_text, policy)
            ev = Evidence(
                url=hit.url,
                domain_class=domain_cls,
                policy=policy,
                matched_text=matched_text,
                score=ev_score,
                query_stage=stage,
            )
            evidence_list.append(ev)
            cache_city_policy(rec, ev)  # Opt #4: populate city cache

            # Opt #1: early exit if single authoritative source is unambiguous and high-scoring
            if domain_cls in AUTHORITATIVE and ev_score >= HIGH_CONFIDENCE_THRESHOLD:
                auth_so_far = [e for e in evidence_list if e.domain_class in AUTHORITATIVE]
                if not _auth_conflict(auth_so_far):
                    break  # clean authoritative signal — stop fetching more pages this stage

            if len(evidence_list) >= MAX_PAGES_PER_BEACH:
                break

        if len(evidence_list) >= MAX_PAGES_PER_BEACH:
            break

    # Opt #4: if we found nothing, check city-level cache for a fallback
    if not evidence_list:
        cached = get_cached_city_policy(rec)
        if cached:
            evidence_list = [cached]

    result = _choose_result(rec, evidence_list)

    new_name = None
    if was_disambiguated and evidence_list:
        proposed, _ = search_name(rec)
        new_name = proposed

    return result, new_name


def _auth_conflict(auth_evidence: list[Evidence]) -> bool:
    """True if authoritative sources disagree with each other."""
    return len(set(e.policy for e in auth_evidence)) > 1


def _choose_result(rec: BeachRecord, evidence: list[Evidence]) -> ClassificationResult:
    if not evidence:
        return ClassificationResult(
            beach_id=rec.id,
            policy="unknown",
            confidence="needs_review",
            primary_url="",
            matched_text="",
            all_sources=[],
            conflict=False,
            review_notes="No classifiable policy text found across all search results.",
            evidence_count=0,
        )

    evidence.sort(key=lambda e: e.score, reverse=True)

    auth_evidence = [e for e in evidence if e.domain_class in AUTHORITATIVE]
    low_evidence  = [e for e in evidence if e.domain_class not in AUTHORITATIVE]

    # Authority-first: determine winning policy from authoritative sources when available
    if auth_evidence:
        auth_policies = set(e.policy for e in auth_evidence)
        conflict = len(auth_policies) > 1

        if conflict:
            # Genuine conflict between authoritative sources — flag for review
            best = max(auth_evidence, key=lambda e: e.score)
            confidence = "needs_review"
            review_notes = f"Authoritative sources disagree: {', '.join(sorted(auth_policies))}. Manual review required."
        else:
            # All authoritative sources agree
            best = max(auth_evidence, key=lambda e: e.score)
            auth_hosts = set(urlparse(e.url).netloc for e in auth_evidence)
            low_policies = set(e.policy for e in low_evidence)
            discrepancy = low_policies and not low_policies.issubset(auth_policies)

            if len(auth_hosts) >= MIN_SOURCES_CONFIRMED:
                confidence = "confirmed"
                review_notes = f"{len(auth_hosts)} authoritative sources agree: {best.policy}."
            else:
                confidence = "confirmed"  # single authoritative source outranks any number of low-auth sources
                review_notes = f"Authoritative source ({best.domain_class}): {best.policy}."

            if discrepancy:
                review_notes += f" Note: tourism/aggregator sources say '{', '.join(sorted(low_policies))}' — official takes precedence."

    else:
        # No authoritative sources — fall back to vote-based among low-authority
        conflict = False
        best = evidence[0]
        agreeing_hosts = set(
            urlparse(e.url).netloc for e in evidence if e.policy == best.policy
        )
        if len(agreeing_hosts) >= MIN_SOURCES_CONFIRMED:
            confidence = "probable"
            review_notes = f"{len(agreeing_hosts)} non-authoritative sources agree: {best.policy}. No official source found."
        else:
            confidence = "needs_review"
            review_notes = f"No authoritative source found. Single non-official source ({best.domain_class}): {best.policy}."

    # Primary URL: best authoritative source matching winning policy, else overall best
    auth_matching = [e for e in auth_evidence if e.policy == best.policy]
    primary = max(auth_matching, key=lambda e: e.score) if auth_matching else best

    return ClassificationResult(
        beach_id=rec.id,
        policy=best.policy,
        confidence=confidence,
        primary_url=primary.url,
        matched_text=primary.matched_text,
        all_sources=[e.url for e in evidence],
        conflict=conflict,
        review_notes=review_notes,
        evidence_count=len(evidence),
    )

# ── Checkpoint ────────────────────────────────────────────────────────────────

def load_checkpoint(checkpoint_file: Path) -> set[int]:
    if not checkpoint_file.exists():
        return set()
    data = json.loads(checkpoint_file.read_text())
    return set(data.get("completed_ids", []))

def save_checkpoint(checkpoint_file: Path, completed_ids: set[int], results: list[dict]) -> None:
    checkpoint_file.write_text(json.dumps({
        "completed_ids": sorted(completed_ids),
        "count": len(completed_ids),
        "updated_at": int(time.time()),
    }, indent=2))

# ── Main ──────────────────────────────────────────────────────────────────────

async def run(args: argparse.Namespace) -> None:
    checkpoint_dir  = Path(args.checkpoint_dir)
    cache_dir       = checkpoint_dir / "pages"
    checkpoint_file = checkpoint_dir / "progress.json"
    results_file    = checkpoint_dir / "results.jsonl"
    review_file     = checkpoint_dir / "needs_review.jsonl"

    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    ids = [int(i) for i in args.ids.split(",")] if args.ids else None
    print(f"Fetching beaches: county={args.county}, state={args.state}" + (f", ids={ids}" if ids else ""))
    beaches = fetch_beaches(args.county, args.state, args.limit or None, ids=ids)
    print(f"  Found {len(beaches)} beaches")

    completed_ids = load_checkpoint(checkpoint_file) if args.resume else set()
    pending = [b for b in beaches if b.id not in completed_ids]
    print(f"  {len(completed_ids)} already done, {len(pending)} to process")

    semaphore = asyncio.Semaphore(WORKERS)
    limits    = httpx.Limits(max_keepalive_connections=WORKERS, max_connections=WORKERS * 2)

    async with httpx.AsyncClient(limits=limits, http2=True) as client:
        pbar = tqdm(total=len(pending), desc="Classifying", unit="beach")
        results_fh = results_file.open("a", encoding="utf-8")
        review_fh  = review_file.open("a", encoding="utf-8")

        async def worker(rec: BeachRecord) -> None:
            async with semaphore:
                new_name = None
                try:
                    result, new_name = await research_beach(client, rec, cache_dir)
                except Exception as e:
                    result = ClassificationResult(
                        beach_id=rec.id, policy="unknown", confidence="needs_review",
                        primary_url="", matched_text="", all_sources=[], conflict=False,
                        review_notes=f"Pipeline error: {e}", evidence_count=0,
                    )

                effective_name = new_name or rec.display_name
                row = {**asdict(result), "display_name": effective_name, "city": rec.city,
                       "name_disambiguated": new_name is not None}
                results_fh.write(json.dumps(row) + "\n")
                results_fh.flush()

                if result.confidence == "needs_review" or result.conflict:
                    review_fh.write(json.dumps(row) + "\n")
                    review_fh.flush()

                if not args.dry_run:
                    try:
                        write_result(result, dry_run=False, new_display_name=new_name)
                    except Exception as e:
                        tqdm.write(f"  Write error id={rec.id}: {e}")
                else:
                    write_result(result, dry_run=True, new_display_name=new_name)

                completed_ids.add(rec.id)

                if len(completed_ids) % CHECKPOINT_EVERY == 0:
                    save_checkpoint(checkpoint_file, completed_ids, [])

                pbar.update(1)
                pbar.set_postfix(done=len(completed_ids))

        tasks = [asyncio.create_task(worker(b)) for b in pending]
        try:
            await asyncio.gather(*tasks)
        finally:
            pbar.close()
            results_fh.close()
            review_fh.close()

    save_checkpoint(checkpoint_file, completed_ids, [])

    # Summary
    print("\n-- Summary --------------------------------------------------")
    if results_file.exists():
        rows = [json.loads(l) for l in results_file.read_text().splitlines() if l.strip()]
        by_policy     = {}
        by_confidence = {}
        for r in rows:
            by_policy[r["policy"]]         = by_policy.get(r["policy"], 0) + 1
            by_confidence[r["confidence"]] = by_confidence.get(r["confidence"], 0) + 1
        print("Policy breakdown:")
        for k, v in sorted(by_policy.items()):
            print(f"  {k:15s} {v}")
        print("Confidence breakdown:")
        for k, v in sorted(by_confidence.items()):
            print(f"  {k:15s} {v}")
    print(f"\nCheckpoint: {checkpoint_dir}")
    print(f"Review queue: {review_file}")


def main() -> int:
    p = argparse.ArgumentParser(description="Phase 1: dog policy classification")
    p.add_argument("--county",          required=True)
    p.add_argument("--state",           required=True)
    p.add_argument("--checkpoint-dir",  default="./checkpoints/phase1")
    p.add_argument("--limit",           type=int, default=0)
    p.add_argument("--ids",             default="", help="Comma-separated beach IDs to process")
    p.add_argument("--resume",          action="store_true")
    p.add_argument("--dry-run",         action="store_true")
    args = p.parse_args()
    asyncio.run(run(args))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
