#!/usr/bin/env python3
"""
Beach dog-policy enrichment pipeline.

Purpose
-------
Read a CSV of U.S. beaches, research dog-access policy for each beach using
per-beach web search + page extraction, and append structured results.

Key design choices
------------------
* Per-beach lookup. No jurisdiction-level rollups.
* Strict `dog_friendly`: only when the source explicitly says the beach is
  dog-friendly or otherwise clearly markets it as a dog beach / dog destination.
* `restricted` covers leash, time-of-day, seasonal, or area-based limits.
* Checkpointed, resumable, rate-limited, and progress-logged.
* Manual-review queue for low-confidence or conflicting cases.

Search provider
---------------
Set ONE of these environment variables:
  * SERPAPI_API_KEY
  * TAVILY_API_KEY

Examples
--------
python beach_dog_policy_pipeline.py \
  --input /path/to/US_beaches.csv \
  --output /path/to/US_beaches_dog_policy.csv \
  --checkpoint-dir ./checkpoints \
  --workers 6

Resume from checkpoint:
python beach_dog_policy_pipeline.py \
  --input /path/to/US_beaches.csv \
  --output /path/to/US_beaches_dog_policy.csv \
  --checkpoint-dir ./checkpoints \
  --resume
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import os
import random
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import quote_plus, urlparse

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter
from tqdm import tqdm


# -----------------------------
# Configuration / constants
# -----------------------------
USER_AGENT = "beach-dog-policy-pipeline/1.0 (contact: replace-with-your-email@example.com)"
SEARCH_RESULT_LIMIT = 10
FETCH_CHAR_LIMIT = 25000
SEARCH_TIMEOUT = 30.0
FETCH_TIMEOUT = 30.0
CHECKPOINT_EVERY = 50
HIGH_CONFIDENCE_OFFICIAL_SCORE = 1.22
OFFICIAL_DOMAIN_HINTS = (
    ".gov",
    ".us",
    ".ca.gov",
    ".state.nj.us",
    "stateparks",
    "parks",
    "recreation",
    "county",
    "cityof",
    "ci.",
)

DOMAIN_CLASS_WEIGHTS = {
    "official": 1.00,
    "parks_authority": 0.95,
    "tourism": 0.75,
    "local_media": 0.60,
    "aggregator": 0.45,
    "blog": 0.30,
    "other": 0.20,
}

NEGATIVE_QUERY_TERMS = "-hotel -restaurant -apartment -yelp -wedding -resort -rv"

STATE_NAME_TO_ABBR = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO",
    "montana": "MT", "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
    "new mexico": "NM", "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH",
    "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "district of columbia": "DC",
}

STATE_ABBR_TO_OFFICIAL_DOMAINS = {
    "AL": ["alabama.gov", "alapark.com", "dcnr.alabama.gov"],
    "AK": ["alaska.gov", "dnr.alaska.gov", "parks.alaska.gov"],
    "AZ": ["az.gov", "azstateparks.com", "new.azstateparks.com"],
    "AR": ["arkansas.gov", "arkansasstateparks.com"],
    "CA": ["ca.gov", "parks.ca.gov", "www.parks.ca.gov", "stateparks.com"],
    "CO": ["colorado.gov", "cpw.state.co.us", "cpwshop.com"],
    "CT": ["ct.gov", "portal.ct.gov", "ctparks.com"],
    "DE": ["delaware.gov", "destateparks.com"],
    "FL": ["fl.gov", "floridastateparks.org", "myfwc.com", "dep.state.fl.us"],
    "GA": ["ga.gov", "gastateparks.org", "explore.gastateparks.org"],
    "HI": ["hawaii.gov", "dlnr.hawaii.gov", "gohawaii.com"],
    "IA": ["iowa.gov", "iowadnr.gov"],
    "ID": ["idaho.gov", "parksandrecreation.idaho.gov"],
    "IL": ["illinois.gov", "dnr.illinois.gov"],
    "IN": ["in.gov", "dnr.in.gov"],
    "KS": ["ks.gov", "ksoutdoors.com"],
    "KY": ["kentucky.gov", "parks.ky.gov"],
    "LA": ["louisiana.gov", "lastateparks.com", "crt.state.la.us"],
    "MA": ["mass.gov", "mass.gov/info-details/dogs-at-dcr-parks", "mass.gov/locations/dcr-parks-and-beaches"],
    "MD": ["maryland.gov", "dnr.maryland.gov", "parkreservations.maryland.gov"],
    "ME": ["maine.gov", "maine.gov/dacf/parks", "visitmaine.com"],
    "MI": ["michigan.gov", "michigan.gov/dnr", "midnrreservations.com"],
    "MN": ["mn.gov", "dnr.state.mn.us"],
    "MO": ["mo.gov", "mostateparks.com"],
    "MS": ["ms.gov", "mdwfp.com"],
    "MT": ["mt.gov", "fwp.mt.gov", "stateparks.mt.gov"],
    "NC": ["nc.gov", "ncparks.gov", "deq.nc.gov"],
    "ND": ["nd.gov", "parkrec.nd.gov"],
    "NE": ["nebraska.gov", "outdoornebraska.gov"],
    "NH": ["nh.gov", "nhstateparks.org"],
    "NJ": ["nj.gov", "dep.nj.gov", "state.nj.us", "njparksandforests.org"],
    "NM": ["newmexico.gov", "emnrd.nm.gov", "stateparks.emnrd.nm.gov"],
    "NV": ["nv.gov", "parks.nv.gov", "travelnevada.com"],
    "NY": ["ny.gov", "parks.ny.gov", "dec.ny.gov"],
    "OH": ["ohio.gov", "ohiodnr.gov"],
    "OK": ["ok.gov", "travelok.com", "stateparks.com/oklahoma"],
    "OR": ["oregon.gov", "stateparks.oregon.gov", "traveloregon.com"],
    "PA": ["pa.gov", "dcnr.pa.gov"],
    "RI": ["ri.gov", "riparks.ri.gov", "dem.ri.gov"],
    "SC": ["sc.gov", "southcarolinaparks.com", "discoversouthcarolina.com"],
    "TX": ["texas.gov", "tpwd.texas.gov", "texasstateparks.reserveamerica.com"],
    "UT": ["utah.gov", "stateparks.utah.gov"],
    "VA": ["virginia.gov", "dcr.virginia.gov", "virginiastateparks.gov"],
    "VT": ["vermont.gov", "vtstateparks.com"],
    "WA": ["wa.gov", "parks.wa.gov", "wdfw.wa.gov"],
    "WI": ["wisconsin.gov", "dnr.wisconsin.gov"],
    "WV": ["wv.gov", "wvstateparks.com"],
}

GENERIC_OFFICIAL_INCLUDE_DOMAINS = [
    "nps.gov", "recreation.gov", "fws.gov", "blm.gov", "fs.usda.gov",
]

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
    r"\bon leash\b",
    r"\bleashed\b",
    r"\bbefore\s+\d{1,2}(:\d{2})?\s*(am|pm)\b",
    r"\bafter\s+\d{1,2}(:\d{2})?\s*(am|pm)\b",
    r"\bseasonal\b",
    r"\bseason\b",
    r"\bexcept\b",
    r"\bdesignated area\b",
    r"\bdesignated section\b",
    r"\bbetween .* and .*\b",
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
    r"boardwalk",
    r"parking lot",
    r"trail",
    r"picnic area",
    r"except service animals",
]


# -----------------------------
# Data classes
# -----------------------------
@dataclass
class BeachRecord:
    idx: int
    fid: Any
    name: str
    address_parts: list[str]
    country: str
    wkt: str
    lon: float
    lat: float

    @property
    def search_address(self) -> str:
        return ", ".join([p for p in self.address_parts if p])

    @property
    def geo_hint(self) -> str:
        return f"{self.lat:.6f}, {self.lon:.6f}"


@dataclass
class SearchHit:
    title: str
    url: str
    snippet: str = ""
    rank: int = 0
    query: str = ""
    query_stage: str = ""


@dataclass
class ClassificationResult:
    dog_policy: str
    dog_policy_detail: str
    source: str
    source_url: str
    confidence: str
    matched_text: str
    search_query_used: str = ""
    query_stage_used: str = ""
    notes: str = ""
    review_required: bool = False
    evidence_count: int = 0
    conflict_detected: bool = False


# -----------------------------
# Utility functions
# -----------------------------
def parse_point_wkt(wkt: str) -> tuple[float, float]:
    m = re.match(r"POINT\s*\(([-0-9.]+)\s+([-0-9.]+)\)", str(wkt).strip(), re.IGNORECASE)
    if not m:
        raise ValueError(f"Unsupported or invalid WKT point: {wkt}")
    lon = float(m.group(1))
    lat = float(m.group(2))
    return lon, lat


def normalize_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text or " ")
    return text.strip()


def slugify(text: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text.lower()).strip("-")
    return text[:120] if text else "item"


def is_officialish_domain(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return any(hint in host for hint in OFFICIAL_DOMAIN_HINTS)


def sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def make_dirs(*paths: Path) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def _clean_loc_token(value: str) -> str:
    value = normalize_text(value)
    value = re.sub(r"\bUnited States\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+,", ",", value)
    return value.strip(" ,")


def infer_city_state(rec: BeachRecord, rev_display: str = "") -> tuple[str, str]:
    city = ""
    state = ""
    addr = [p for p in rec.address_parts if p]
    joined = ", ".join(addr + ([rev_display] if rev_display else []))
    m = re.search(r"\b([A-Z][a-z]+(?:[ -][A-Z][a-z]+)*)\s*,\s*([A-Z]{2})\b", joined)
    if m:
        city, state = m.group(1), m.group(2)
    else:
        m2 = re.search(r"\b([A-Z][a-z]+(?:[ -][A-Z][a-z]+)*)\s*,\s*([A-Z][a-z]+)\b", joined)
        if m2:
            city, state = m2.group(1), m2.group(2)
    return _clean_loc_token(city), _clean_loc_token(state)


def normalize_state_token(state: str) -> str:
    st = normalize_text(state).strip()
    if not st:
        return ""
    if len(st) == 2 and st.isalpha():
        return st.upper()
    return STATE_NAME_TO_ABBR.get(st.lower(), st.upper())


def tavily_include_domains_for_record(rec: BeachRecord, stage: str, rev_display: str = "") -> list[str]:
    city, state = infer_city_state(rec, rev_display)
    state_abbr = normalize_state_token(state)
    domains: list[str] = []

    if stage == "official_first":
        domains.extend(GENERIC_OFFICIAL_INCLUDE_DOMAINS)
        domains.extend(STATE_ABBR_TO_OFFICIAL_DOMAINS.get(state_abbr, []))
        if city:
            city_slug = re.sub(r"[^a-z0-9]", "", city.lower())
            domains.extend([
                f"{city_slug}.gov",
                f"cityof{city_slug}.org",
                f"ci.{city_slug}.ca.us",
                f"{city_slug}.org",
            ])
    elif stage in {"dog_friendly_probe", "restriction_probe"}:
        domains.extend(GENERIC_OFFICIAL_INCLUDE_DOMAINS)
        domains.extend(STATE_ABBR_TO_OFFICIAL_DOMAINS.get(state_abbr, []))

    # dedupe and keep only host-like values
    cleaned = []
    seen = set()
    for d in domains:
        d = normalize_text(d).lower().replace("https://", "").replace("http://", "").strip("/")
        if not d or "/" in d:
            d = d.split("/", 1)[0]
        if d and d not in seen:
            seen.add(d)
            cleaned.append(d)
    return cleaned[:10]


def record_to_search_queries(rec: BeachRecord, rev_display: str = "") -> list[tuple[str, str]]:
    name = normalize_text(rec.name)
    city, state = infer_city_state(rec, rev_display)
    queries: list[tuple[str, str]] = []

    if name:
        # Stage 1: official-first
        queries.extend([
            ("official_first", f'site:.gov "{name}" dogs'),
            ("official_first", f'site:.gov "{name}" beach dogs'),
            ("official_first", f'site:.us "{name}" dogs'),
        ])

        # Stage 2: exact broad
        queries.extend([
            ("exact_broad", f'"{name}" dogs allowed {NEGATIVE_QUERY_TERMS}'),
            ("exact_broad", f'"{name}" dog policy {NEGATIVE_QUERY_TERMS}'),
            ("exact_broad", f'"{name}" beach dogs {NEGATIVE_QUERY_TERMS}'),
        ])

        # Stage 3: geographic disambiguation
        if city and state:
            queries.extend([
                ("geo_disambiguation", f'"{name}" {city} {state} dogs allowed {NEGATIVE_QUERY_TERMS}'),
                ("geo_disambiguation", f'"{name}" {city} dog policy {NEGATIVE_QUERY_TERMS}'),
                ("geo_disambiguation", f'"{name}" near {city} dogs beach {NEGATIVE_QUERY_TERMS}'),
            ])
        elif city:
            queries.extend([
                ("geo_disambiguation", f'"{name}" {city} dogs allowed {NEGATIVE_QUERY_TERMS}'),
                ("geo_disambiguation", f'"{name}" near {city} dogs beach {NEGATIVE_QUERY_TERMS}'),
            ])

        # Stage 4: strict dog-friendly
        queries.extend([
            ("dog_friendly_probe", f'"{name}" "dog friendly" {NEGATIVE_QUERY_TERMS}'),
            ("dog_friendly_probe", f'"{name}" "dogs welcome" {NEGATIVE_QUERY_TERMS}'),
            ("dog_friendly_probe", f'"{name}" "dog beach" {NEGATIVE_QUERY_TERMS}'),
        ])

        # Stage 5: restriction discovery
        queries.extend([
            ("restriction_probe", f'"{name}" dogs leash {NEGATIVE_QUERY_TERMS}'),
            ("restriction_probe", f'"{name}" dogs hours {NEGATIVE_QUERY_TERMS}'),
            ("restriction_probe", f'"{name}" seasonal dog restrictions {NEGATIVE_QUERY_TERMS}'),
            ("restriction_probe", f'"{name}" dogs prohibited {NEGATIVE_QUERY_TERMS}'),
        ])

    # Fallbacks when name is weak or absent
    if rec.search_address:
        queries.append(("address_fallback", f'"{rec.search_address}" beach dogs {NEGATIVE_QUERY_TERMS}'))
    queries.append(("coordinate_fallback", f'beach near {rec.lat:.6f},{rec.lon:.6f} dogs allowed {NEGATIVE_QUERY_TERMS}'))
    if city and state:
        queries.append(("coordinate_fallback", f'dog policy beach near {city} {state} {NEGATIVE_QUERY_TERMS}'))

    # de-dup while preserving order
    seen = set()
    deduped: list[tuple[str, str]] = []
    for stage, q in queries:
        if q not in seen:
            seen.add(q)
            deduped.append((stage, q))
    return deduped


# -----------------------------
# Search providers
# -----------------------------
class SearchProvider:
    async def search(
        self,
        client: httpx.AsyncClient,
        query: str,
        limit: int = SEARCH_RESULT_LIMIT,
        *,
        location: str = "",
        lat: float | None = None,
        lon: float | None = None,
        stage: str = "",
    ) -> list[SearchHit]:
        raise NotImplementedError


class SerpApiProvider(SearchProvider):
    endpoint = "https://serpapi.com/search.json"

    def __init__(self, api_key: str):
        self.api_key = api_key

    @retry(wait=wait_exponential_jitter(1, 8), stop=stop_after_attempt(4), retry=retry_if_exception_type(httpx.HTTPError))
    async def search(self, client: httpx.AsyncClient, query: str, limit: int = SEARCH_RESULT_LIMIT, *, location: str = "", lat: float | None = None, lon: float | None = None, stage: str = "") -> list[SearchHit]:
        params = {
            "engine": "google",
            "q": query,
            "api_key": self.api_key,
            "num": limit,
            "hl": "en",
            "gl": "us",
            "google_domain": "google.com",
        }
        if location:
            params["location"] = location
        if lat is not None and lon is not None:
            params["ludocid"] = ""
            params["ll"] = f"@{lat},{lon},14z"
        resp = await client.get(self.endpoint, params=params, timeout=SEARCH_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        hits: list[SearchHit] = []
        for i, item in enumerate(data.get("organic_results", [])[:limit], start=1):
            link = item.get("link") or item.get("url")
            if not link:
                continue
            hits.append(SearchHit(
                title=normalize_text(item.get("title", "")),
                url=link,
                snippet=normalize_text(item.get("snippet", "")),
                rank=i,
                query=query,
                query_stage=stage,
            ))
        return hits


class TavilyProvider(SearchProvider):
    endpoint = "https://api.tavily.com/search"

    def __init__(self, api_key: str):
        self.api_key = api_key

    @retry(wait=wait_exponential_jitter(1, 8), stop=stop_after_attempt(4), retry=retry_if_exception_type(httpx.HTTPError))
    async def search(self, client: httpx.AsyncClient, query: str, limit: int = SEARCH_RESULT_LIMIT, *, location: str = "", lat: float | None = None, lon: float | None = None, stage: str = "") -> list[SearchHit]:
        payload = {
            "api_key": self.api_key,
            "query": query,
            "search_depth": "basic" if stage in {"official_first", "exact_broad"} else "advanced",
            "max_results": limit,
            "include_answer": False,
            "include_raw_content": False,
            "topic": "general",
        }
        if location:
            payload["location"] = location
        include_domains = []
        if lat is not None and lon is not None:
            # lat/lon are not native Tavily ranking inputs here; they are still embedded in queries upstream.
            pass
        if hasattr(self, "_include_domains") and stage in self._include_domains:
            include_domains = self._include_domains[stage]
        if include_domains:
            payload["include_domains"] = include_domains[:10]
        resp = await client.post(self.endpoint, json=payload, timeout=SEARCH_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        hits: list[SearchHit] = []
        for i, item in enumerate(data.get("results", [])[:limit], start=1):
            url = item.get("url")
            if not url:
                continue
            hits.append(SearchHit(
                title=normalize_text(item.get("title", "")),
                url=url,
                snippet=normalize_text(item.get("content", "")),
                rank=i,
                query=query,
                query_stage=stage,
            ))
        return hits


def build_search_provider() -> SearchProvider:
    serp_key = os.getenv("SERPAPI_API_KEY", "").strip()
    tavily_key = os.getenv("TAVILY_API_KEY", "").strip()
    if serp_key:
        return SerpApiProvider(serp_key)
    if tavily_key:
        return TavilyProvider(tavily_key)
    raise RuntimeError(
        "No search provider configured. Set SERPAPI_API_KEY or TAVILY_API_KEY in your environment."
    )


# -----------------------------
# Content extraction
# -----------------------------
async def fetch_page_text(client: httpx.AsyncClient, url: str) -> str:
    headers = {"User-Agent": USER_AGENT}
    resp = await client.get(url, headers=headers, timeout=FETCH_TIMEOUT, follow_redirects=True)
    resp.raise_for_status()
    ctype = resp.headers.get("content-type", "")
    if "pdf" in ctype.lower():
        return ""
    html = resp.text
    soup = BeautifulSoup(html, "lxml")

    for tag in soup(["script", "style", "noscript", "svg", "header", "footer", "nav", "form"]):
        tag.extract()

    texts: list[str] = []
    for selector in ["main", "article", "body"]:
        node = soup.select_one(selector)
        if node:
            texts.append(node.get_text(" ", strip=True))
    if not texts:
        texts.append(soup.get_text(" ", strip=True))

    text = normalize_text(" ".join(texts))
    return text[:FETCH_CHAR_LIMIT]


# -----------------------------
# Reverse geocoding (hint only)
# -----------------------------
@retry(wait=wait_exponential_jitter(1, 6), stop=stop_after_attempt(4), retry=retry_if_exception_type(httpx.HTTPError))
async def reverse_geocode_hint(client: httpx.AsyncClient, lat: float, lon: float, cache_dir: Path) -> dict[str, Any]:
    cache_key = cache_dir / f"reverse_{lat:.6f}_{lon:.6f}.json"
    if cache_key.exists():
        return json.loads(cache_key.read_text())

    url = "https://nominatim.openstreetmap.org/reverse"
    params = {
        "lat": lat,
        "lon": lon,
        "format": "jsonv2",
        "addressdetails": 1,
        "zoom": 16,
    }
    headers = {"User-Agent": USER_AGENT}
    resp = await client.get(url, params=params, headers=headers, timeout=SEARCH_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    cache_key.write_text(json.dumps(data, indent=2))
    return data


# -----------------------------
# Classification logic
# -----------------------------
def find_pattern_hits(text: str, patterns: list[str]) -> list[str]:
    hits: list[str] = []
    lowered = text.lower()
    for pat in patterns:
        for m in re.finditer(pat, lowered, flags=re.IGNORECASE):
            start = max(0, m.start() - 120)
            end = min(len(text), m.end() + 120)
            hits.append(normalize_text(text[start:end]))
    return hits


def classify_domain(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if any(x in host for x in [".gov", ".us", ".mil"]):
        return "official"
    if any(x in host for x in ["parks", "stateparks", "recreation"]):
        return "parks_authority"
    if any(x in host for x in ["visit", "tourism", "chamber"]):
        return "tourism"
    if any(x in host for x in ["times", "tribune", "patch", "news"]):
        return "local_media"
    if any(x in host for x in ["bringfido", "beaches", "tripadvisor", "yelp"]):
        return "aggregator"
    if any(x in host for x in ["blog", "wordpress", "substack"]):
        return "blog"
    return "other"


def score_hit_for_record(rec: BeachRecord, hit: SearchHit, page_text: str) -> float:
    score = DOMAIN_CLASS_WEIGHTS.get(classify_domain(hit.url), 0.2)
    haystack = f"{hit.title} {hit.snippet} {page_text[:5000]}".lower()
    name = rec.name.lower().strip()
    if name and name in haystack:
        score += 0.35
    elif name and fuzz.partial_ratio(name, haystack) >= 80:
        score += 0.18

    if rec.search_address and any(part.lower() in haystack for part in rec.address_parts[:2] if part):
        score += 0.20

    if any(k in haystack for k in ["dog", "dogs", "pet", "pets"]):
        score += 0.08
    if any(k in haystack for k in ["beach rules", "beach policy", "park rules", "hours", "allowed", "prohibited", "leash"]):
        score += 0.08

    policy, evidence = classify_text(page_text)
    if policy == "prohibited":
        score += 0.30
    elif policy == "restricted":
        score += 0.25
    elif policy == "dog_friendly":
        score += 0.20
    elif policy == "allowed":
        score += 0.12

    if any(ctx in evidence.lower() for ctx in NEGATION_CONTEXT):
        score -= 0.30

    score += max(0, (12 - hit.rank)) * 0.01
    return score


def classify_text(text: str) -> tuple[Optional[str], str]:
    text = normalize_text(text)
    friendly_hits = find_pattern_hits(text, DOG_FRIENDLY_PATTERNS)
    prohibited_hits = find_pattern_hits(text, PROHIBITED_PATTERNS)
    restricted_hits = find_pattern_hits(text, RESTRICTED_PATTERNS)
    allowed_hits = find_pattern_hits(text, ALLOWED_PATTERNS)

    if friendly_hits:
        return "dog_friendly", friendly_hits[0]
    if prohibited_hits:
        return "prohibited", prohibited_hits[0]
    if restricted_hits:
        return "restricted", restricted_hits[0]
    if allowed_hits:
        return "allowed", allowed_hits[0]
    return None, ""


def confidence_for(url: str, policy: str, evidence_count: int, conflict: bool) -> str:
    domain_class = classify_domain(url)
    if conflict:
        return "low"
    if domain_class in {"official", "parks_authority"} and evidence_count >= 1:
        return "high"
    if domain_class in {"tourism", "local_media"} and evidence_count >= 1:
        return "medium"
    if evidence_count >= 2:
        return "medium"
    return "low"


def derive_detail(policy: str, evidence: str) -> str:
    evidence = normalize_text(evidence)
    if not evidence:
        return "No reliable policy text found."
    if len(evidence) <= 240:
        return evidence
    return evidence[:237] + "..."


def is_high_confidence_official_candidate(rec: BeachRecord, hit: SearchHit, page_text: str) -> bool:
    policy, evidence = classify_text(page_text)
    if not policy:
        return False
    if classify_domain(hit.url) not in {"official", "parks_authority"}:
        return False
    score = score_hit_for_record(rec, hit, page_text)
    if score < HIGH_CONFIDENCE_OFFICIAL_SCORE:
        return False
    title_snippet = (hit.title + " " + hit.snippet).lower()
    rec_name = rec.name.lower().strip()
    exact_or_strong = (rec_name and rec_name in title_snippet) or (rec_name and fuzz.partial_ratio(rec_name, title_snippet) >= 88)
    return exact_or_strong or any(part.lower() in page_text.lower() for part in rec.address_parts[:2] if part)


def choose_best_result(rec: BeachRecord, candidates: list[tuple[SearchHit, str]]) -> ClassificationResult:
    scored: list[tuple[float, SearchHit, str, str, str]] = []
    all_policies: list[str] = []

    for hit, page_text in candidates:
        policy, evidence = classify_text(page_text)
        if not policy:
            continue
        score = score_hit_for_record(rec, hit, page_text)
        scored.append((score, hit, page_text, policy, evidence))
        all_policies.append(policy)

    if not scored:
        return ClassificationResult(
            dog_policy="unknown",
            dog_policy_detail="No reliable dog policy could be found for this beach.",
            source="",
            source_url="",
            confidence="low",
            matched_text="",
            notes="No candidate pages contained classifiable policy language.",
            review_required=True,
        )

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_hit, _best_page_text, best_policy, best_evidence = scored[0]

    unique_policies = sorted(set(all_policies))
    conflict = len(unique_policies) > 1
    confidence = confidence_for(best_hit.url, best_policy, len(scored), conflict)

    notes = []
    if conflict:
        notes.append(f"Conflicting policies seen across sources: {', '.join(unique_policies)}")
    if classify_domain(best_hit.url) not in {"official", "parks_authority"}:
        notes.append("Best source is not official or parks authority.")
    if rec.name and fuzz.partial_ratio(rec.name.lower(), (best_hit.title + ' ' + best_hit.snippet).lower()) < 60:
        notes.append("Best source is a fuzzy rather than exact beach-name match.")

    review_required = conflict or confidence == "low"

    return ClassificationResult(
        dog_policy=best_policy,
        dog_policy_detail=derive_detail(best_policy, best_evidence),
        source=urlparse(best_hit.url).netloc,
        source_url=best_hit.url,
        confidence=confidence,
        matched_text=best_evidence,
        search_query_used=best_hit.query,
        query_stage_used=best_hit.query_stage,
        notes=" | ".join(notes),
        review_required=review_required,
        evidence_count=len(scored),
        conflict_detected=conflict,
    )


# -----------------------------
# Pipeline runner
# -----------------------------
class Pipeline:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.input_path = Path(args.input)
        self.output_path = Path(args.output)
        self.checkpoint_dir = Path(args.checkpoint_dir)
        self.cache_dir = self.checkpoint_dir / "cache"
        self.pages_dir = self.cache_dir / "pages"
        self.search_dir = self.cache_dir / "search"
        self.reverse_dir = self.cache_dir / "reverse"
        self.trace_dir = self.checkpoint_dir / "trace"
        self.trace_ndjson_file = self.checkpoint_dir / "trace.ndjson"
        make_dirs(self.checkpoint_dir, self.cache_dir, self.pages_dir, self.search_dir, self.reverse_dir, self.trace_dir)
        self.progress_file = self.checkpoint_dir / "progress.json"
        self.partial_file = self.checkpoint_dir / "partial_results.csv"
        self.review_file = self.checkpoint_dir / "manual_review.csv"
        self.provider = build_search_provider()
        self.df = pd.read_csv(self.input_path)
        self.results_by_idx: dict[int, dict[str, Any]] = {}
        if args.resume:
            self._load_partial()

    def _load_partial(self) -> None:
        if self.partial_file.exists():
            partial = pd.read_csv(self.partial_file)
            if "_row_idx" in partial.columns:
                for _, row in partial.iterrows():
                    self.results_by_idx[int(row["_row_idx"])] = row.to_dict()
                print(f"Loaded {len(self.results_by_idx):,} completed rows from {self.partial_file}")

    def beach_record_from_row(self, idx: int, row: pd.Series) -> BeachRecord:
        lon, lat = parse_point_wkt(row["WKT"])
        address_parts = [
            str(row.get("ADDR1", "") or "").strip(),
            str(row.get("ADDR2", "") or "").strip(),
            str(row.get("ADDR3", "") or "").strip(),
            str(row.get("ADDR4", "") or "").strip(),
            str(row.get("ADDR5", "") or "").strip(),
        ]
        return BeachRecord(
            idx=idx,
            fid=row.get("fid"),
            name=str(row.get("NAME", "") or "").strip(),
            address_parts=[p for p in address_parts if p and p.lower() != "nan"],
            country=str(row.get("COUNTRY", "") or "").strip(),
            wkt=str(row.get("WKT", "") or "").strip(),
            lon=lon,
            lat=lat,
        )

    async def cached_search(self, client: httpx.AsyncClient, query: str, *, stage: str = "", location: str = "", lat: float | None = None, lon: float | None = None, include_domains: list[str] | None = None) -> list[SearchHit]:
        cache_id = json.dumps({"q": query, "stage": stage, "location": location, "lat": lat, "lon": lon, "include_domains": include_domains or []}, sort_keys=True)
        key = self.search_dir / f"{sha1_text(cache_id)}.json"
        if key.exists():
            data = json.loads(key.read_text())
            return [SearchHit(**x) for x in data]
        previous_include = getattr(self.provider, "_include_domains", None)
        try:
            if isinstance(self.provider, TavilyProvider):
                self.provider._include_domains = {stage: include_domains or []}
            hits = await self.provider.search(client, query, limit=SEARCH_RESULT_LIMIT, location=location, lat=lat, lon=lon, stage=stage)
        finally:
            if isinstance(self.provider, TavilyProvider):
                self.provider._include_domains = previous_include or {}
        key.write_text(json.dumps([asdict(h) for h in hits], indent=2))
        return hits

    async def cached_fetch(self, client: httpx.AsyncClient, url: str) -> str:
        key = self.pages_dir / f"{sha1_text(url)}.txt"
        if key.exists():
            return key.read_text(errors="ignore")
        try:
            text = await fetch_page_text(client, url)
        except Exception:
            text = ""
        key.write_text(text)
        return text

    async def process_record(self, client: httpx.AsyncClient, rec: BeachRecord) -> dict[str, Any]:
        trace: dict[str, Any] = {
            "row_idx": rec.idx,
            "fid": rec.fid,
            "name": rec.name,
            "coordinates": {"lat": rec.lat, "lon": rec.lon},
            "address_parts": rec.address_parts,
            "events": [],
        }
        # Reverse geocode is only used to enrich queries, never to assign the final policy.
        try:
            rev = await reverse_geocode_hint(client, rec.lat, rec.lon, self.reverse_dir)
        except Exception as e:
            rev = {}
            trace["events"].append({"type": "reverse_geocode_error", "error": str(e)})

        rev_display = normalize_text(rev.get("display_name", ""))
        location_hint = ", ".join([x for x in infer_city_state(rec, rev_display) if x])
        trace["reverse_geocode_hint"] = rev_display
        trace["location_hint"] = location_hint

        queries = record_to_search_queries(rec, rev_display)
        trace["queries"] = [{"stage": stage, "query": query} for stage, query in queries[: self.args.max_queries_per_record]]

        seen_urls: set[str] = set()
        candidates: list[tuple[SearchHit, str]] = []

        stop_early = False
        early_stop_reason = ""
        for stage, query in queries[: self.args.max_queries_per_record]:
            include_domains = tavily_include_domains_for_record(rec, stage, rev_display) if isinstance(self.provider, TavilyProvider) else None
            hits = await self.cached_search(client, query, stage=stage, location=location_hint, lat=rec.lat, lon=rec.lon, include_domains=include_domains)
            trace_event = {
                "type": "search",
                "stage": stage,
                "query": query,
                "include_domains": include_domains or [],
                "hits_returned": len(hits),
                "hits": [],
            }
            for hit in hits:
                hit_trace = {
                    "url": hit.url,
                    "title": hit.title,
                    "snippet": hit.snippet,
                    "domain_class": classify_domain(hit.url),
                    "kept": False,
                }
                if hit.url in seen_urls:
                    hit_trace["skip_reason"] = "duplicate_url"
                    trace_event["hits"].append(hit_trace)
                    continue
                seen_urls.add(hit.url)
                page_text = await self.cached_fetch(client, hit.url)
                if not page_text:
                    hit_trace["skip_reason"] = "empty_page"
                    trace_event["hits"].append(hit_trace)
                    continue
                # Lightweight beach-match gate to avoid classifying irrelevant pages.
                hay = (hit.title + " " + hit.snippet + " " + page_text[:3500]).lower()
                rec_name = rec.name.lower().strip()
                if rec_name and rec_name not in hay:
                    locality_ok = any(part.lower() in hay for part in rec.address_parts[:2] if part)
                    fuzzy_score = fuzz.partial_ratio(rec_name, hay)
                    hit_trace["locality_ok"] = locality_ok
                    hit_trace["fuzzy_score"] = fuzzy_score
                    if not locality_ok and fuzzy_score < 65:
                        hit_trace["skip_reason"] = "beach_match_gate"
                        trace_event["hits"].append(hit_trace)
                        continue
                policy, evidence = classify_text(page_text)
                candidate_score = score_hit(hit, page_text)
                hit_trace.update({
                    "policy": policy,
                    "score": candidate_score,
                    "matched_text": evidence[:500],
                    "kept": True,
                })
                candidates.append((hit, page_text))
                trace_event["hits"].append(hit_trace)
                if is_high_confidence_official_candidate(rec, hit, page_text):
                    stop_early = True
                    early_stop_reason = f"high_confidence_official:{stage}:{hit.url}"
                    trace_event["early_stop_triggered"] = True
                    trace_event["early_stop_url"] = hit.url
                    break
                if len(candidates) >= self.args.max_pages_per_record:
                    trace_event["page_cap_reached"] = True
                    break
            trace["events"].append(trace_event)
            if stop_early or len(candidates) >= self.args.max_pages_per_record:
                break

        result = choose_best_result(rec, candidates)
        trace["candidate_count"] = len(candidates)
        trace["early_stop"] = stop_early
        trace["early_stop_reason"] = early_stop_reason
        trace["final_result"] = {
            "dog_policy": result.dog_policy,
            "dog_policy_detail": result.dog_policy_detail,
            "source": result.source,
            "source_url": result.source_url,
            "confidence": result.confidence,
            "matched_text": result.matched_text,
            "search_query_used": result.search_query_used,
            "query_stage_used": result.query_stage_used,
            "review_required": result.review_required,
            "evidence_count": result.evidence_count,
            "conflict_detected": result.conflict_detected,
            "research_notes": result.notes,
        }
        self.write_trace(rec, trace)
        payload = {
            "_row_idx": rec.idx,
            "dog_policy": result.dog_policy,
            "dog_policy_detail": result.dog_policy_detail,
            "source": result.source,
            "source_url": result.source_url,
            "confidence": result.confidence,
            "matched_text": result.matched_text,
            "search_query_used": result.search_query_used,
            "query_stage_used": result.query_stage_used,
            "review_required": result.review_required,
            "evidence_count": result.evidence_count,
            "conflict_detected": result.conflict_detected,
            "research_notes": result.notes,
            "reverse_geocode_hint": rev_display,
            "search_queries": " || ".join([q for _stage, q in queries[: self.args.max_queries_per_record]]),
            "processed_at_epoch": int(time.time()),
        }
        return payload


    def write_trace(self, rec: BeachRecord, trace: dict[str, Any]) -> None:
        if not self.args.debug_trace:
            return
        safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", (rec.name or "unnamed").strip())[:80] or "unnamed"
        trace_path = self.trace_dir / f"{rec.idx:06d}_{safe_name}.json"
        trace_json = json.dumps(trace, indent=2, ensure_ascii=False)
        trace_path.write_text(trace_json)
        ndjson_line = json.dumps(trace, ensure_ascii=False) + "\n"
        with self.trace_ndjson_file.open("a", encoding="utf-8") as fh:
            fh.write(ndjson_line)

    def flush_partial(self) -> None:
        if not self.results_by_idx:
            return
        partial = pd.DataFrame(sorted(self.results_by_idx.values(), key=lambda x: x["_row_idx"]))
        partial.to_csv(self.partial_file, index=False, quoting=csv.QUOTE_MINIMAL)
        if self.progress_file:
            status = {
                "completed": len(self.results_by_idx),
                "total": len(self.df),
                "remaining": len(self.df) - len(self.results_by_idx),
                "output_path": str(self.output_path),
                "partial_file": str(self.partial_file),
                "review_file": str(self.review_file),
                "updated_at_epoch": int(time.time()),
            }
            self.progress_file.write_text(json.dumps(status, indent=2))

    def write_outputs(self) -> None:
        if not self.results_by_idx:
            raise RuntimeError("No results available to write.")
        results = pd.DataFrame(sorted(self.results_by_idx.values(), key=lambda x: x["_row_idx"]))
        enriched = self.df.copy()
        enriched["_row_idx"] = enriched.index
        enriched = enriched.merge(results, on="_row_idx", how="left")
        enriched.drop(columns=["_row_idx"], inplace=True)
        enriched.to_csv(self.output_path, index=False, quoting=csv.QUOTE_MINIMAL)

        review = enriched[enriched["review_required"] == True].copy()  # noqa: E712
        review.to_csv(self.review_file, index=False, quoting=csv.QUOTE_MINIMAL)

    async def run(self) -> None:
        pending_rows: list[tuple[int, pd.Series]] = [
            (idx, row)
            for idx, row in self.df.iterrows()
            if idx not in self.results_by_idx
        ]
        if self.args.limit:
            pending_rows = pending_rows[: self.args.limit]

        limits = httpx.Limits(max_keepalive_connections=self.args.workers, max_connections=self.args.workers * 2)
        headers = {"User-Agent": USER_AGENT}
        semaphore = asyncio.Semaphore(self.args.workers)

        async with httpx.AsyncClient(headers=headers, limits=limits, http2=True) as client:
            pbar = tqdm(total=len(pending_rows), desc="Researching beaches", unit="beach")

            async def worker(idx: int, row: pd.Series) -> None:
                rec = self.beach_record_from_row(idx, row)
                async with semaphore:
                    payload = await self.process_record(client, rec)
                    self.results_by_idx[idx] = payload
                    # Light jitter reduces accidental burstiness on remote sites.
                    await asyncio.sleep(random.uniform(0.05, 0.25))
                    if len(self.results_by_idx) % CHECKPOINT_EVERY == 0:
                        self.flush_partial()
                pbar.update(1)
                pbar.set_postfix(done=len(self.results_by_idx), total=len(self.df))

            tasks = [asyncio.create_task(worker(idx, row)) for idx, row in pending_rows]
            try:
                await asyncio.gather(*tasks)
            finally:
                pbar.close()

        self.flush_partial()
        self.write_outputs()


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Per-beach dog-policy enrichment pipeline")
    p.add_argument("--input", required=True, help="Input CSV path")
    p.add_argument("--output", required=True, help="Output CSV path")
    p.add_argument("--checkpoint-dir", required=True, help="Checkpoint/cache directory")
    p.add_argument("--workers", type=int, default=6, help="Concurrent workers")
    p.add_argument("--resume", action="store_true", help="Resume from partial checkpoint if available")
    p.add_argument("--limit", type=int, default=0, help="Optional limit for test runs")
    p.add_argument("--max-queries-per-record", type=int, default=12, help="Search queries to try per beach")
    p.add_argument("--max-pages-per-record", type=int, default=6, help="Candidate pages to inspect per beach")
    p.add_argument("--debug-trace", action="store_true", help="Write per-record JSON traces under checkpoint-dir/trace and append a consolidated trace.ndjson file")
    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    pipe = Pipeline(args)
    asyncio.run(pipe.run())
    print(f"\nDone. Final CSV: {pipe.output_path}")
    print(f"Partial checkpoint: {pipe.partial_file}")
    print(f"Manual review queue: {pipe.review_file}")
    print(f"Progress file: {pipe.progress_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
