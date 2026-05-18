"""derive_policy_source_for_jurisdiction.py — Codify v1 driver.

Per `docs/codify_cascade_v1_runbook.md` Track 1 + `docs/codify_pip_resolver_architecture.md`.
Replaces the manual + sub-agent process for per-jurisdiction codification.

Architecture (state-agnostic):
  Step 0   — classify(jurisdiction, state) → governance_class
  Step 0.5 — triage (CDP→parent, tribal→defer, federal→branch)
  Step 1   — scope check (existing ps? supplement vs new)
  Step 2   — discover platform (per-state priority + per-class title heuristic)
  Step 3   — navigate to operative chapter (TOC scrape + LLM fallback)
  Step 4   — fetch verbatim text (Playwright + per-platform selectors)
  Step 5   — REMOVED in new architecture (beach mapping happens in resolver, not here)
  Step 6   — LLM rule decision → produce (polygon_id, rule_text, subtype, domain) tuples
  Step 7   — emit migration
  Step 8   — quality gates
  Step 9   — commit + apply
  Step 9.5 — temporal extractor follow-up

This first pass implements Steps 0/0.5/1 only. Steps 2-4 are stubbed pending
the next build session. Stops cleanly with a dry-run report when called.

Usage:
  python scripts/derive_policy_source_for_jurisdiction.py \
    --jurisdiction "Skagit County" --state WA
  python scripts/derive_policy_source_for_jurisdiction.py \
    --jurisdiction "City of Bainbridge Island" --state WA --dry-run
  python scripts/derive_policy_source_for_jurisdiction.py \
    --state WA --pilot 5
  python scripts/derive_policy_source_for_jurisdiction.py \
    --from-csv config/wa_jurisdictions_queue.csv
"""

from __future__ import annotations

# Truststore for AV-MITM Windows SSL (same idiom as extract_temporal_from_policy_source.py).
try:
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import argparse
import csv
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, asdict
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
ENV = ROOT / "scripts" / "pipeline" / ".env"
load_dotenv(ENV)

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY  = os.environ["SUPABASE_SERVICE_KEY"]
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL = "claude-sonnet-4-5-20250929"

# Wire in the existing Playwright fetcher (scripts/fetch/fetch_html.py)
# for JS-rendered platforms. Lazy import so the script still runs if
# Playwright isn't installed (urllib fallback only).
sys.path.insert(0, str(ROOT / "scripts" / "fetch"))
try:
    from fetch_html import fetch as playwright_fetch  # type: ignore
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    playwright_fetch = None  # type: ignore
    PLAYWRIGHT_AVAILABLE = False

# ─── HTTP helpers ──────────────────────────────────────────────────────

def supa(path: str, *, params: dict | None = None, method: str = "GET",
         body: dict | list | None = None, upsert: bool = False) -> list:
    """PostgREST client. Mirrors extract_temporal_from_policy_source.py."""
    url = f"{SUPABASE_URL}{path}"
    if params:
        url = url + "?" + urllib.parse.urlencode(params, safe=",.()*")
    req = urllib.request.Request(url, method=method)
    req.add_header("apikey", SERVICE_KEY)
    req.add_header("Authorization", f"Bearer {SERVICE_KEY}")
    if body is not None:
        req.add_header("Content-Type", "application/json")
        req.data = json.dumps(body).encode("utf-8")
        prefers = ["return=representation"]
        if upsert:
            prefers.append("resolution=merge-duplicates")
        req.add_header("Prefer", ",".join(prefers))
    with urllib.request.urlopen(req, timeout=60) as r:
        text = r.read().decode("utf-8")
        return json.loads(text) if text else []


# ─── Step 0 — Classify jurisdiction ────────────────────────────────────

# TIGER CLASSFP per public.jurisdictions.place_type (column comment):
#   C1/C2/C3/C5/C6/C7 = incorporated city
#   U1                = census-designated place (unincorporated)
# For governance routing in the codify pipeline, see
# project_codify_v1_governance_aware_design.md.

GOVERNANCE_CLASSES = {
    "incorporated_city",  # has its own municipal code
    "county",             # county ordinances
    "cdp",                # unincorporated; covered by parent county
    "state",              # state-level (rare per-jurisdiction codify)
    "tribal",             # deferred per Franz 2026-05-17
    "federal",            # NPS / USFWS / etc.; separate codify path
    "special_district",   # port, water, harbor, etc.
    "unknown",            # diagnostic: not classifiable
}


@dataclass
class JurisdictionClassification:
    """Output of Step 0 classify_jurisdiction().

    `polygon_key` is the natural key of the source-of-truth polygon row,
    cast to text. For city/cdp = jurisdictions.id. For county = counties.geoid.
    For federal/tribal/special_district = pad_us_units.unit_id.
    """
    name:               str
    state:              str
    governance_class:   str
    polygon_table:      str | None  # 'jurisdictions' | 'counties' | 'pad_us_units'
    polygon_key:        str | None
    parent_county:      str | None  # for cdp triage
    classify_notes:     str | None


# State → FIPS state code. Stable; hardcoded to avoid a lookup query.
STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08",
    "CT": "09", "DE": "10", "DC": "11", "FL": "12", "GA": "13", "HI": "15",
    "ID": "16", "IL": "17", "IN": "18", "IA": "19", "KS": "20", "KY": "21",
    "LA": "22", "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27",
    "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46",
    "TN": "47", "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53",
    "WV": "54", "WI": "55", "WY": "56",
}


def classify_jurisdiction(name: str, state: str) -> JurisdictionClassification:
    """Look up the jurisdiction in existing DB tables and classify.

    Strategy (lookup order matches the data layout):
      1. If name looks like a county → query public.counties (joined on state_fp).
      2. Else query public.jurisdictions (TIGER Places: cities + CDPs).
      3. Else heuristic on name → federal / tribal / special_district / unknown.

    Reuses these existing tables; does NOT create new ones.
    """
    low = name.lower()
    is_county_named = low.endswith(" county") or " county " in low

    # ── (1) County lookup via public.counties (state_fp + name without suffix) ──
    if is_county_named:
        bare = name.replace(" County", "").replace(" county", "").strip()
        state_fp = STATE_FIPS.get(state)
        if state_fp:
            rows = supa("/rest/v1/counties", params={
                "select":   "geoid,name,name_full",
                "name":     f"eq.{bare}",
                "state_fp": f"eq.{state_fp}",
                "limit":    "1",
            })
            if rows:
                r = rows[0]
                return JurisdictionClassification(
                    name=name, state=state, governance_class="county",
                    polygon_table="counties", polygon_key=r["geoid"],
                    parent_county=None,
                    classify_notes=f"name_full={r.get('name_full')}",
                )
        # Couldn't find in counties; fall through to heuristic
        return JurisdictionClassification(
            name=name, state=state, governance_class="county",
            polygon_table=None, polygon_key=None, parent_county=None,
            classify_notes=f"county-named but not found in counties (state_fp={state_fp})",
        )

    # ── (2) City / CDP lookup via public.jurisdictions ──
    name_candidates = [name]
    if name.startswith("City of "):
        name_candidates.append(name.replace("City of ", ""))

    rows_exact: list = []
    for cand in name_candidates:
        rows = supa("/rest/v1/jurisdictions", params={
            "select": "id,name,namelsad,place_type,state,county",
            "name":   f"eq.{cand}",
            "state":  f"eq.{state}",
            "limit":  "5",
        })
        if rows:
            rows_exact = rows
            break

    if rows_exact:
        r = rows_exact[0]
        pt = (r.get("place_type") or "").upper()
        if pt.startswith("C"):
            gc = "incorporated_city"
        elif pt == "U1":
            gc = "cdp"
        else:
            gc = "unknown"
        return JurisdictionClassification(
            name=name, state=state, governance_class=gc,
            polygon_table="jurisdictions", polygon_key=str(r["id"]),
            parent_county=r.get("county"),
            classify_notes=f"place_type={pt} namelsad={r.get('namelsad')}",
        )

    # ── (3) Federal / tribal / special_district via naming heuristic ──
    if any(k in low for k in ["national park", "national forest", "national monument",
                              "national seashore", "national recreation area",
                              "national wildlife refuge", "national marine sanctuary"]):
        return JurisdictionClassification(
            name=name, state=state, governance_class="federal",
            polygon_table="pad_us_units", polygon_key=None,
            parent_county=None,
            classify_notes="federal land manager (PAD-US lookup needed in next iteration)",
        )
    if any(k in low for k in ["tribe", "nation", "reservation", "rancheria"]):
        return JurisdictionClassification(
            name=name, state=state, governance_class="tribal",
            polygon_table="pad_us_units", polygon_key=None,
            parent_county=None,
            classify_notes="tribal authority (PAD-US TRIB lookup needed); DEFER per Franz",
        )
    if any(k in low for k in ["district", "port of", "harbor"]):
        return JurisdictionClassification(
            name=name, state=state, governance_class="special_district",
            polygon_table="pad_us_units", polygon_key=None,
            parent_county=None,
            classify_notes="special district (PAD-US DIST lookup needed)",
        )

    return JurisdictionClassification(
        name=name, state=state, governance_class="unknown",
        polygon_table=None, polygon_key=None, parent_county=None,
        classify_notes="not found in counties/jurisdictions and no naming heuristic matched",
    )


# ─── Step 0.5 — Triage ─────────────────────────────────────────────────

@dataclass
class TriageDecision:
    """Output of Step 0.5 triage."""
    action:     str  # 'proceed' | 'skip_covered_by_parent' | 'defer' | 'branch_federal'
    reason:     str
    recurse_to: str | None  # parent jurisdiction name if recursing


def triage(jc: JurisdictionClassification) -> TriageDecision:
    """Decide whether to proceed, skip, defer, or branch on this jurisdiction."""
    if jc.governance_class == "cdp":
        # CDP triage: covered by parent county. We could recurse on parent_county
        # but for the script's purposes, skip with reference; the parent gets
        # codified as its own jurisdiction in the queue.
        parent = jc.parent_county or "(unknown county)"
        return TriageDecision(
            action="skip_covered_by_parent",
            reason=f"CDP → covered by parent county {parent}",
            recurse_to=parent,
        )
    if jc.governance_class == "tribal":
        return TriageDecision(
            action="defer",
            reason="Tribal authority deferred per Franz 2026-05-17",
            recurse_to=None,
        )
    if jc.governance_class == "federal":
        return TriageDecision(
            action="branch_federal",
            reason="Federal land manager — separate codify path (TODO: derive_policy_source_for_federal_authority.py)",
            recurse_to=None,
        )
    if jc.governance_class == "unknown":
        return TriageDecision(
            action="defer",
            reason=f"Could not classify jurisdiction: {jc.classify_notes}",
            recurse_to=None,
        )
    # incorporated_city, county, state, special_district → proceed
    return TriageDecision(
        action="proceed",
        reason=f"Proceed with {jc.governance_class} codify path",
        recurse_to=None,
    )


# ─── Step 1 — Scope check ─────────────────────────────────────────────

@dataclass
class ScopeCheck:
    """Output of Step 1 scope check."""
    agency_id:         int | None
    agency_name:       str | None
    agency_type:       str | None
    agency_web_url:    str | None    # for Step 6.7 agency-policy fallback
    existing_ps_count: int
    existing_ps_ids:   list[int]
    decision:          str  # 'create_new' | 'supplement_existing' | 'no_agency'


# Map governance_class → agency.type enum value (per agency_type enum).
GOVERNANCE_TO_AGENCY_TYPE = {
    "incorporated_city": "city",
    "county":            "county",
    "state":             "state",
    "tribal":            "tribal",
    "federal":           "federal",
    "special_district":  "special_district",
}


def scope_check(jc: JurisdictionClassification) -> ScopeCheck:
    """Find the agency row for this jurisdiction; count existing ps rows.

    Per playbook §1: if jurisdiction has ≥1 existing ps row → SUPPLEMENT;
    else → CREATE_NEW. If no agency row exists at all → NO_AGENCY (caller
    must decide whether to create the agency canonically — see tenet 4).
    """
    expected_type = GOVERNANCE_TO_AGENCY_TYPE.get(jc.governance_class)
    if not expected_type:
        return ScopeCheck(None, None, None, None, 0, [], "no_agency")

    # Try bare-name canonical (per tenet 4 — bare name, not "X, City of")
    agency_name_candidates = [jc.name]
    if jc.name.startswith("City of "):
        agency_name_candidates.append(jc.name.replace("City of ", ""))
    elif jc.name.endswith(" County") and expected_type == "county":
        agency_name_candidates.append(jc.name)  # already bare-name-like

    agency_row = None
    for cand in agency_name_candidates:
        rows = supa("/rest/v1/agency", params={
            "select": "id,name,type,web_url",
            "name":   f"eq.{cand}",
            "type":   f"eq.{expected_type}",
            "limit":  "1",
        })
        if rows:
            agency_row = rows[0]
            break

    if not agency_row:
        return ScopeCheck(None, None, expected_type, None, 0, [], "no_agency")

    # Count existing policy_source rows issued by this agency
    ps_rows = supa("/rest/v1/policy_source", params={
        "select": "id,citation",
        "issuing_agency_id": f"eq.{agency_row['id']}",
        "limit":  "50",
    })

    decision = "supplement_existing" if ps_rows else "create_new"
    return ScopeCheck(
        agency_id=agency_row["id"],
        agency_name=agency_row["name"],
        agency_type=agency_row["type"],
        agency_web_url=agency_row.get("web_url"),
        existing_ps_count=len(ps_rows),
        existing_ps_ids=[r["id"] for r in ps_rows] if ps_rows else [],
        decision=decision,
    )


# ─── Step 2 — Platform discovery ───────────────────────────────────────

# Per-state platform priority. WA = codepublishing dominant (per the WA
# codify audit); CA = Municode dominant; OR = Municode-then-codepublishing.
# Inline config for v1; migrate to DB table if it grows. See
# docs/codify_pip_resolver_architecture.md.
STATE_PLATFORM_PRIORITY = {
    "WA": ["codepublishing", "municode", "amlegal", "county_codes"],
    "OR": ["municode", "codepublishing", "amlegal"],
    "CA": ["municode", "amlegal", "qcode", "ecode360", "codepublishing", "county_codes"],
    "MI": ["municode", "amlegal", "ecode360", "codepublishing"],
    "MA": ["ecode360", "amlegal", "municode"],
    # default for unspecified: try all in CA's order
    "_default": ["municode", "amlegal", "qcode", "ecode360", "codepublishing", "county_codes"],
}

# Per-platform fetch config. JS-rendered platforms route through Playwright
# (via scripts/fetch/fetch_html.py). Static-HTML platforms use urllib.
# `selector` is the CSS selector for the meaningful content area (per
# [[municode-fetchable]] for Municode).
PLATFORM_FETCH_CONFIG = {
    "municode":         {"mode": "playwright", "wait_seconds": 12.0, "selector": ".codes-chunks-pg"},
    "ecode360":         {"mode": "playwright", "wait_seconds": 8.0,  "selector": None},
    "codepublishing":   {"mode": "urllib",     "wait_seconds": 0,    "selector": None},
    "amlegal":          {"mode": "urllib",     "wait_seconds": 0,    "selector": ".section-content"},
    "qcode":            {"mode": "urllib",     "wait_seconds": 0,    "selector": None},
    "county_codes":     {"mode": "urllib",     "wait_seconds": 0,    "selector": None},
    # Step 6.7 fallback: agency homepages often Cloudflare-blocked
    "agency_homepage":  {"mode": "playwright", "wait_seconds": 4.0,  "selector": None},
}


@dataclass
class PlatformCandidate:
    platform:      str          # 'municode' | 'codepublishing' | etc.
    candidate_url: str
    notes:         str | None = None


@dataclass
class PlatformDiscovery:
    valid_url:    str | None
    platform:     str | None
    tried:        list[dict]    # [{candidate_url, status, why}]
    notes:        str | None


def _slugify_jurisdiction(name: str, strip_county_suffix: bool = True) -> str:
    """Bare slug — lowercase, spaces → underscores, no City of prefix.
    Set strip_county_suffix=False to keep " County" (some platforms need it)."""
    base = name.replace("City of ", "")
    if strip_county_suffix:
        base = base.replace(" County", "")
    return base.lower().replace(" ", "_").replace(".", "").replace("'", "")


def _slug_no_separator(name: str, strip_county_suffix: bool = False) -> str:
    """codepublishing-style — concatenated, no spaces/underscores, preserves case.
    Default keeps County suffix (codepublishing uses 'SkagitCounty')."""
    base = name.replace("City of ", "")
    if strip_county_suffix:
        base = base.replace(" County", "")
    return "".join(c for c in base if c.isalnum())


def _candidates_municode(jc: JurisdictionClassification, state: str) -> list[PlatformCandidate]:
    """Municode: library.municode.com/<state>/<slug>/codes/<doc>

    Per [[ca-codify-v1-lessons]]:
    - 4 doc_slugs in priority order cover 100% of CA Municode URLs:
        code_of_ordinances (83%) → municipal_code → ordinance_code → code
    - Counties ALWAYS use `_county`-suffixed slug (CA: 35/35 = 100%);
      cities use bare slug. No need to try both.

    Net: 1 slug × 4 docs = 4 candidates. Step 2 returns on first valid
    hit so most cases terminate after candidate 1 (~12s Playwright)."""
    state_lc = state.lower()
    doc_slugs = ["code_of_ordinances", "municipal_code", "ordinance_code", "code"]
    if jc.governance_class == "county":
        slug = _slugify_jurisdiction(jc.name, strip_county_suffix=False)
    else:
        slug = _slugify_jurisdiction(jc.name)
    return [
        PlatformCandidate(
            platform="municode",
            candidate_url=f"https://library.municode.com/{state_lc}/{slug}/codes/{ds}",
            notes=f"slug={slug} doc={ds}",
        )
        for ds in doc_slugs
    ]


def _candidates_codepublishing(jc: JurisdictionClassification, state: str) -> list[PlatformCandidate]:
    """codepublishing: www.codepublishing.com/<STATE>/<JurisdictionConcat>/
    Counties keep 'County' suffix ('SkagitCounty'); cities don't."""
    state_uc = state.upper()
    concat = _slug_no_separator(jc.name)
    return [
        PlatformCandidate(
            platform="codepublishing",
            candidate_url=f"https://www.codepublishing.com/{state_uc}/{concat}/",
            notes=f"concat={concat}",
        ),
    ]


def _candidates_amlegal(jc: JurisdictionClassification, state: str) -> list[PlatformCandidate]:
    """amlegal: codelibrary.amlegal.com/codes/<slug>/latest/<slug>_<state>/"""
    state_lc = state.lower()
    bare = _slugify_jurisdiction(jc.name)
    return [
        PlatformCandidate(
            platform="amlegal",
            candidate_url=f"https://codelibrary.amlegal.com/codes/{bare}/latest/{bare}_{state_lc}/",
            notes=f"slug={bare}",
        ),
    ]


def _candidates_qcode(jc: JurisdictionClassification, state: str) -> list[PlatformCandidate]:
    """qcode: qcode.us/codes/<slug>/"""
    bare = _slugify_jurisdiction(jc.name).replace("_", "-")
    return [
        PlatformCandidate(
            platform="qcode",
            candidate_url=f"https://qcode.us/codes/{bare}/",
            notes=f"slug={bare}",
        ),
    ]


def _candidates_ecode360(jc: JurisdictionClassification, state: str) -> list[PlatformCandidate]:
    """ecode360: ecode360.com/<ABBR> — ABBR is a 5-char code; not derivable
    formulaically. v1 returns a guess based on the first 5 letters of the name;
    real lookups need the per-jurisdiction abbr stored or discovered separately."""
    bare = _slugify_jurisdiction(jc.name).replace("_", "")
    abbr_guess = bare[:5].upper()
    return [
        PlatformCandidate(
            platform="ecode360",
            candidate_url=f"https://ecode360.com/{abbr_guess}",
            notes=f"abbr_guess={abbr_guess} (formulaic; may need real lookup)",
        ),
    ]


def _candidates_county_codes(jc: JurisdictionClassification, state: str) -> list[PlatformCandidate]:
    """county.codes: www.county.codes/codes/<state>-<county>/"""
    if jc.governance_class != "county":
        return []
    state_lc = state.lower()
    bare = _slugify_jurisdiction(jc.name).replace("_county", "")
    return [
        PlatformCandidate(
            platform="county_codes",
            candidate_url=f"https://www.county.codes/codes/{state_lc}-{bare}/",
            notes=f"county-slug={bare}",
        ),
    ]


# ─── Quality gate: subtype-aware URL depth check ──────────────────────

# Per [[ca-root-url-discipline-violation]] decision 2026-05-18:
# - municipal_code / federal_regulation / state_statute / state_regulation /
#   tribal_code / special_district_ordinance / superintendents_compendium /
#   mou / lease_agreement / operating_agreement / concession_lease /
#   agency_administrative_policy*: must deep-link to a section/chapter
# - operator_posted_policy: page-level acceptable (the page IS the source)
# - agency_administrative_policy: page-level acceptable IF the page IS the
#   operative source (city's "Dogs in X" page, NPS pets page, etc.) —
#   judgment call; flag for review rather than fail hard
DEEPLINK_REQUIRED_SUBTYPES = {
    "municipal_code", "federal_regulation", "state_statute", "state_regulation",
    "tribal_code", "special_district_ordinance", "superintendents_compendium",
    "mou", "lease_agreement", "operating_agreement", "concession_lease",
}

DEEPLINK_MARKERS = [
    r"nodeId=", r"\?topic=", r"sectionNum=", r"page_id=", r"secid=", r"cite=",
    r"#[A-Za-z0-9]",
    r"/section-[0-9]+",
    r"ecode360\.com/[0-9]+",
    r"municipal\.codes/[A-Za-z]+/",
    r"amlegal.*/0-0-0-[0-9]+",
    r"elaws\.us.*_sec",
    r"federalregister\.gov/documents/[0-9]{4}/",
    r"public\.law/(rules|statutes)/",
    r"county\.codes/Code/",
    r"\.html$",   # codepublishing chapter HTML pages are deep enough
    r"\.pdf",     # PDFs are acceptable; #page= anchor a plus
]


def is_url_deep_enough(subtype: str | None, source_url: str | None) -> tuple[bool, str]:
    """Subtype-aware URL depth check for Step 8 quality gates.

    Returns (ok, why). ok=False means the migration should fail at quality gate.
    """
    import re
    if source_url is None or source_url.strip() == "":
        return False, "source_url is null/empty"
    if subtype is None:
        return False, "subtype is null"
    if subtype in {"operator_posted_policy", "agency_administrative_policy"}:
        # Page-level is acceptable for these subtypes (the page IS the source).
        # Still warn if there's no path at all.
        from urllib.parse import urlparse
        path = urlparse(source_url).path
        if path in ("", "/"):
            return False, f"{subtype} URL has no path (bare domain)"
        return True, "ok (page-level acceptable for this subtype)"
    if subtype in DEEPLINK_REQUIRED_SUBTYPES:
        for marker in DEEPLINK_MARKERS:
            if re.search(marker, source_url):
                return True, f"ok (matched deep-link marker {marker!r})"
        return False, f"{subtype} requires section/chapter deep link; no marker matched"
    # Unknown subtype: lean strict
    return False, f"unknown subtype {subtype!r}; check manually"


PLATFORM_BUILDERS = {
    "municode":       _candidates_municode,
    "codepublishing": _candidates_codepublishing,
    "amlegal":        _candidates_amlegal,
    "qcode":          _candidates_qcode,
    "ecode360":       _candidates_ecode360,
    "county_codes":   _candidates_county_codes,
}


def _smart_fetch(url: str, platform: str | None = None,
                 timeout: int = 15) -> tuple[str | None, str]:
    """Fetch HTML via the right transport for the platform.

    Returns (body_text, why). body_text is None on failure; lowercased on
    success for downstream containment checks.
    """
    cfg = PLATFORM_FETCH_CONFIG.get(platform or "", {"mode": "urllib", "wait_seconds": 0, "selector": None})

    if cfg["mode"] == "playwright":
        if not PLAYWRIGHT_AVAILABLE:
            return None, "playwright_unavailable"
        try:
            text = playwright_fetch(
                url,
                selector=None,             # full-page text for validity/TOC
                raw_html=False,
                wait_seconds=cfg.get("wait_seconds", 8.0),
                timeout_ms=30000,
            )
            return (text or "").lower(), "ok_playwright"
        except Exception as e:
            return None, f"playwright_error:{type(e).__name__}"

    # urllib path (default)
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/124.0.0.0 Safari/537.36"),
            "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
                       "image/avif,image/webp,*/*;q=0.8"),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "identity",
        })
        with urllib.request.urlopen(req, timeout=timeout) as r:
            if r.status != 200:
                return None, f"http_{r.status}"
            body = r.read().decode("utf-8", errors="replace")
            return body.lower(), "ok_urllib"
    except urllib.error.HTTPError as e:
        return None, f"http_{e.code}"
    except Exception as e:
        return None, f"fetch_error:{type(e).__name__}"


def _validity_check(url: str, jc: JurisdictionClassification, state: str,
                    platform: str | None = None,
                    timeout: int = 15) -> tuple[bool, str]:
    """Per playbook §2 validity gauntlet:
       1. Fetch success (urllib or Playwright per-platform)
       2. Body length sane
       3. Body contains jurisdiction NAME (case-insensitive)
       4. Body contains state indicator
       5. For counties: body contains "<County Name> County"
    """
    body, why = _smart_fetch(url, platform=platform, timeout=timeout)
    if body is None:
        return False, why
    if len(body) < 300:
        return False, f"body_too_short({len(body)})"
    bare_name = jc.name.replace("City of ", "").replace(" County", "").lower()
    if bare_name not in body:
        return False, f"name_not_in_body({bare_name!r})"
    state_names = {
        "WA": "washington", "OR": "oregon", "CA": "california", "MI": "michigan",
        "MA": "massachusetts", "AK": "alaska", "TX": "texas", "FL": "florida",
        "NY": "new york",
    }
    state_full = state_names.get(state.upper(), state.lower())
    if state_full not in body and f", {state.lower()}" not in body:
        return False, f"state_not_in_body({state_full!r})"
    if jc.governance_class == "county":
        county_name = jc.name.replace(" County", "").lower()
        if f"{county_name} county" not in body:
            return False, f"county_suffix_not_in_body({county_name!r})"
    return True, "ok"


def step_2_discover_platform(jc: JurisdictionClassification, sc: ScopeCheck) -> PlatformDiscovery:
    """Try platforms in per-state priority order; return first valid."""
    if jc.governance_class in ("tribal", "federal", "cdp", "unknown"):
        return PlatformDiscovery(None, None, [],
                                 f"skip platform discovery for governance_class={jc.governance_class}")

    priority = STATE_PLATFORM_PRIORITY.get(jc.state, STATE_PLATFORM_PRIORITY["_default"])
    tried: list[dict] = []

    for platform in priority:
        builder = PLATFORM_BUILDERS.get(platform)
        if not builder:
            continue
        candidates = builder(jc, jc.state)
        for c in candidates:
            ok, why = _validity_check(c.candidate_url, jc, jc.state, platform=platform)
            tried.append({"candidate_url": c.candidate_url, "platform": platform,
                          "valid": ok, "why": why, "notes": c.notes})
            if ok:
                return PlatformDiscovery(
                    valid_url=c.candidate_url, platform=platform, tried=tried,
                    notes=f"valid on {platform} after {len(tried)} attempts",
                )
            # Pacing: more between Playwright calls (browser startup); less for urllib
            mode = PLATFORM_FETCH_CONFIG.get(platform, {}).get("mode", "urllib")
            time.sleep(1.0 if mode == "playwright" else 0.3)

    return PlatformDiscovery(
        valid_url=None, platform=None, tried=tried,
        notes=f"no valid platform after {len(tried)} attempts",
    )


# ─── Step 3 — Navigate to operative chapter ────────────────────────────

# Per-governance-class title heuristic. Score ranges 0-100 (higher = more
# likely the chapter we want). Per [[codify-v1-governance-aware-design]]
# Step 2: cities organize codes by SUBJECT, counties by DEPARTMENT.
TITLE_HEURISTICS = {
    "incorporated_city": [
        # (score, regex pattern, description)
        (95, r"\b(animal|dog|pet|leash)s?\b",            "subject-animal"),
        (80, r"\b(public peace|nuisance|misdemeanor)\b", "subject-peace"),
        (70, r"\b(health|sanitation)\b",                 "subject-health"),
        (60, r"\b(streets?|traffic|parking|public way)\b", "subject-streets"),
        (50, r"\b(park|recreation|beach)s?\b",           "fallback-parks"),
    ],
    "county": [
        # County codes tend to organize by DEPARTMENT — parks-title FIRST,
        # animals-title second per the WA codify audit deltas finding.
        (95, r"\b(park|recreation|beach)s?\b",           "department-parks"),
        (90, r"\b(animal|dog|pet|leash)s?\b",            "subject-animal"),
        (75, r"\b(public works?|public property)\b",     "department-pubworks"),
        (70, r"\b(health|sanitation|welfare)\b",         "department-health"),
        (60, r"\b(licensing|control)\b",                 "department-licensing"),
    ],
    "special_district": [
        (95, r"\b(animal|dog|pet|leash)s?\b",            "subject-animal"),
        (85, r"\b(park|recreation|beach)s?\b",           "department-parks"),
        (70, r"\b(rule|regulation|ordinance)s?\b",       "generic-rules"),
    ],
}


@dataclass
class ChapterCandidate:
    url:        str
    link_text:  str
    score:      int
    why:        str   # which heuristic matched


@dataclass
class ChapterNavigation:
    best_url:    str | None
    best_text:   str | None
    best_score:  int
    candidates:  list[ChapterCandidate]
    method:      str         # 'static_html' | 'playwright_needed' | 'llm_fallback' | 'error'
    notes:       str | None


def _fetch_html_for_toc(url: str, platform: str | None = None,
                        timeout: int = 20) -> str | None:
    """Fetch the TOC page for chapter navigation. Routes to Playwright for
    JS-rendered platforms; returns raw HTML (mixed-case, untouched) so the
    anchor parser can extract original href + text."""
    cfg = PLATFORM_FETCH_CONFIG.get(platform or "", {"mode": "urllib", "wait_seconds": 0})
    if cfg["mode"] == "playwright":
        if not PLAYWRIGHT_AVAILABLE:
            return None
        try:
            # raw_html=True so we get the rendered HTML with href attributes intact
            return playwright_fetch(
                url, selector=None, raw_html=True,
                wait_seconds=cfg.get("wait_seconds", 8.0),
                timeout_ms=30000,
            )
        except Exception:
            return None
    # urllib path
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/124.0.0.0 Safari/537.36"),
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "identity",
        })
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception:
        return None


def _extract_anchor_links(html: str, base_url: str) -> list[tuple[str, str]]:
    """Pull (href, text) tuples from anchors in the HTML. Resolves relative
    URLs against base_url. Uses regex to avoid the bs4 dependency for now;
    sufficient for codepublishing/qcode/amlegal TOC pages."""
    import re
    out: list[tuple[str, str]] = []
    # <a href="...">text</a> — non-greedy text, ignore nested HTML
    for m in re.finditer(r'<a\s+[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
                         html, re.IGNORECASE | re.DOTALL):
        href = m.group(1).strip()
        text = re.sub(r"<[^>]+>", "", m.group(2)).strip()
        text = re.sub(r"\s+", " ", text)
        if not text or len(text) > 200:
            continue
        # Resolve relative URLs
        if href.startswith("/"):
            from urllib.parse import urlparse
            p = urlparse(base_url)
            href = f"{p.scheme}://{p.netloc}{href}"
        elif not href.startswith(("http://", "https://")):
            # relative to base
            if base_url.endswith("/"):
                href = base_url + href
            else:
                href = base_url.rsplit("/", 1)[0] + "/" + href
        out.append((href, text))
    return out


def _score_title_links(links: list[tuple[str, str]],
                       governance_class: str) -> list[ChapterCandidate]:
    """Apply per-governance-class heuristic; return scored candidates."""
    import re
    heuristics = TITLE_HEURISTICS.get(governance_class, TITLE_HEURISTICS["incorporated_city"])
    scored: dict[str, ChapterCandidate] = {}
    for href, text in links:
        low = text.lower()
        for score, pattern, why in heuristics:
            if re.search(pattern, low):
                # Keep the highest-scoring candidate per URL
                existing = scored.get(href)
                if existing is None or score > existing.score:
                    scored[href] = ChapterCandidate(
                        url=href, link_text=text, score=score, why=why)
                break  # first matching heuristic wins for this link
    return sorted(scored.values(), key=lambda c: c.score, reverse=True)


def step_3_navigate_chapter(jc: JurisdictionClassification,
                            platform: PlatformDiscovery) -> ChapterNavigation:
    """TOC scrape + per-governance-class title heuristic. LLM fallback stubbed."""
    if not platform.valid_url:
        return ChapterNavigation(None, None, 0, [], "error",
                                 "no valid platform from Step 2")

    html = _fetch_html_for_toc(platform.valid_url, platform=platform.platform)
    if not html or len(html) < 500:
        return ChapterNavigation(None, None, 0, [], "error",
                                 f"fetch failed or body too short for {platform.valid_url}")

    links = _extract_anchor_links(html, platform.valid_url)
    if not links:
        return ChapterNavigation(None, None, 0, [], "error",
                                 f"no anchor links found in {platform.valid_url}")

    candidates = _score_title_links(links, jc.governance_class)
    if not candidates:
        return ChapterNavigation(None, None, 0, [], "llm_fallback",
                                 f"no title heuristic match in {len(links)} links "
                                 "(would invoke LLM fallback)")

    top = candidates[0]
    return ChapterNavigation(
        best_url=top.url, best_text=top.link_text, best_score=top.score,
        candidates=candidates[:5],   # cap to top 5 for diagnostics
        method="static_html",
        notes=f"matched '{top.why}' in {len(links)} TOC links",
    )


# ─── Step 4 — Fetch verbatim text ──────────────────────────────────────

# Per-platform content selector for the operative text area. None = full page.
# Per the URL field guide pin + [[municode-fetchable]].
PLATFORM_CONTENT_SELECTOR = {
    "municode":       ".codes-chunks-pg",
    "ecode360":       "#contentArticles",
    "amlegal":        ".section-content",
    "codepublishing": None,   # body text is the chapter content
    "qcode":          None,
    "county_codes":   None,
}


@dataclass
class FetchedSection:
    """One drilled-into section (a sub-chapter discovered inside a title)."""
    url:           str
    link_text:     str | None
    text_excerpt:  str        # first ~3000 chars
    text_length:   int
    has_dog_terms: bool


@dataclass
class FetchedChapter:
    """One top-level chapter result from Step 4 + drilled sub-sections."""
    url:           str
    link_text:     str | None
    text_excerpt:  str        # first ~2000 chars of the title page
    text_length:   int
    has_dog_terms: bool       # quick signal in title content
    drilled:       list[FetchedSection]  # sub-chapters drilled into


@dataclass
class VerbatimFetch:
    """Output of Step 4. Returns top-N chapter fetches for Step 6 LLM."""
    fetched:    list[FetchedChapter]
    fetch_mode: str       # 'static_html' | 'playwright' | 'mixed' | 'error'
    notes:      str | None


def _fetch_text(url: str, platform: str | None) -> tuple[str | None, str | None]:
    """Fetch + extract readable text via per-platform selector. Returns
    (text, raw_html). raw_html is needed to extract sub-chapter anchors
    for drill-down."""
    import re
    selector = PLATFORM_CONTENT_SELECTOR.get(platform or "")
    cfg = PLATFORM_FETCH_CONFIG.get(platform or "", {"mode": "urllib", "wait_seconds": 0})
    is_pw = cfg["mode"] == "playwright"

    if is_pw:
        if not PLAYWRIGHT_AVAILABLE:
            return None, None
        try:
            raw_html = playwright_fetch(url, selector=None, raw_html=True,
                                        wait_seconds=cfg.get("wait_seconds", 8.0),
                                        timeout_ms=30000)
            # Extract text via selector if configured, else full-page
            if selector:
                try:
                    text = playwright_fetch(url, selector=selector, raw_html=False,
                                            wait_seconds=1.0, timeout_ms=15000)
                except Exception:
                    text = None
            else:
                text = re.sub(r"<script[^>]*>.*?</script>", " ", raw_html, flags=re.DOTALL)
                text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL)
                text = re.sub(r"<[^>]+>", " ", text)
                text = re.sub(r"\s+", " ", text).strip()
            return text, raw_html
        except Exception:
            return None, None
    # urllib path
    body, _ = _smart_fetch(url, platform=platform)
    if body is None:
        return None, None
    raw_html = body  # _smart_fetch lower-cased it but anchors still parseable
    if selector and selector.startswith(("#", ".")):
        attr = "id" if selector.startswith("#") else "class"
        value = selector[1:]
        m = re.search(rf'<[^>]+\b{attr}="[^"]*\b{re.escape(value)}\b[^"]*"[^>]*>(.*?)</',
                      body, re.DOTALL)
        text = re.sub(r"<[^>]+>", " ", m.group(1)) if m else body
    else:
        text = re.sub(r"<script[^>]*>.*?</script>", " ", body, flags=re.DOTALL)
        text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL)
        text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text, raw_html


def _has_dog_terms(text: str) -> bool:
    import re
    return bool(re.search(r"\b(dog|leash|animal|pet)s?\b", text or "", re.IGNORECASE))


def _drill_subsections(title_url: str, title_html: str, platform: str | None,
                       jc: JurisdictionClassification,
                       max_subs: int = 3) -> list[FetchedSection]:
    """Given a fetched title page, find sub-chapter anchor links, score them
    with the same heuristic, fetch the top-N sub-pages."""
    if not title_html:
        return []
    anchors = _extract_anchor_links(title_html, title_url)
    # Filter to anchors that look like SUB-pages (not same-page, not external)
    same_dir = title_url.rsplit("/", 1)[0]
    sub_anchors = [(href, text) for href, text in anchors
                   if href != title_url
                   and href.startswith(same_dir)
                   and len(text) > 3]
    scored = _score_title_links(sub_anchors, jc.governance_class)
    if not scored:
        return []

    cfg = PLATFORM_FETCH_CONFIG.get(platform or "", {"mode": "urllib"})
    is_pw = cfg["mode"] == "playwright"

    out: list[FetchedSection] = []
    seen_urls = set()
    for sc in scored[:max_subs * 2]:  # over-sample then de-dupe
        if sc.url in seen_urls:
            continue
        seen_urls.add(sc.url)
        text, _raw = _fetch_text(sc.url, platform=platform)
        if not text or len(text) < 200:
            continue
        out.append(FetchedSection(
            url=sc.url, link_text=sc.link_text,
            text_excerpt=text[:3000], text_length=len(text),
            has_dog_terms=_has_dog_terms(text),
        ))
        if is_pw:
            time.sleep(1.0)
        if len(out) >= max_subs:
            break
    return out


# ─── Step 6 — LLM rule decision ────────────────────────────────────────

# Defaults per [[ca-codify-v1-lessons]]:
#   rule='on_leash' (75%), subtype='municipal_code' for cities (52%) or
#   'agency_administrative_policy' for state agencies (39%), section='sand' (98%),
#   operative_status='operative' (99%), status_note empty (38%) or short (33%).
# Anything outside defaults requires explicit verbatim justification.

RULE_DECISION_PROMPT = """You are a legal-citation parser specialized in dog/beach rules for U.S. municipal codes. Given verbatim text excerpts from one jurisdiction's chapter/section pages, identify the OPERATIVE dog rule.

Input: JSON with `jurisdiction` (name + state + governance_class), `chapters` (list of fetched chapter+section text excerpts with URLs).

Output: JSON with these fields:

{
  "rule":            "on_leash" | "not_allowed" | "off_leash_voice_control" | "off_leash" | "no_rule_found",
  "subtype":         "municipal_code" | "agency_administrative_policy" | "federal_regulation" | "state_statute" | "state_regulation" | "special_district_ordinance" | "tribal_code" | "superintendents_compendium",
  "citation":        "<Jurisdiction> <Code Type> §<section> (<title>)",
  "evidence_quote":  "verbatim sentence(s) from one chapter that establishes the rule",
  "full_text":       "<300-1500 char verbatim quote of the operative section + adjacent context if useful>",
  "deep_link_url":   "<URL of the chapter/section that contains the rule — must be one of the input URLs>",
  "status_note":     null | "<short note — leash length, exception clause, etc. KEEP MINIMAL>",
  "confidence":      0.0..1.0,
  "notes":           null | "<ambiguity flags, alternate interpretations, etc.>",
  "suggested_next_chapter": null | {
      "title":   <integer title number, e.g. 9>,
      "chapter": null | <numeric chapter sub-number, e.g. 41 for Ch 9.41 — null = whole title>,
      "reason":  "<why this is likely the operative location>"
  }
}

When `rule == "no_rule_found"` and you can identify a specific likely-operative chapter NOT in the input (e.g., "the operative rule is probably in Title 9 Parks/Rec which wasn't drilled"), POPULATE `suggested_next_chapter` so the caller can re-drill. The caller can construct the URL automatically; you just need title + chapter numbers.

CRITICAL DEFAULTS (per CA codification data, validate against text):
- 75% of jurisdictions land on `on_leash`. Default to it unless the text says otherwise.
- 38% of rows have NULL status_note. KEEP IT EMPTY unless there's a meaningful detail.
- 33% have a short leash-detail like "6-foot leash". One brief sentence MAX.
- If the text doesn't directly establish a beach-specific dog rule, return `rule: "no_rule_found"` and set `confidence` < 0.4.

CITATION FORMAT:
- "Skagit County Code §7.06.040 (Animal Control)" — canonical form
- "Long Beach Municipal Code §6.16.090 (Dogs)" — canonical
- Always: <jurisdiction> + <code type> + §<section> + (<title>) for parenthetical

RULE TEXT PATTERNS:
- "on a leash no longer than X feet" / "must be on a leash" → `on_leash`
- "no dogs allowed" / "shall not bring" / "prohibited" → `not_allowed`
- "off leash under voice control" / "voice control area" → `off_leash_voice_control`
- "off leash" (no voice-control qualifier) → `off_leash`

WORKED EXAMPLES:

EXAMPLE 1 (Skagit County — Animals title with dangerous-dog focus):
Input: Title 7 Ch 7.06 text excerpts about dangerous dogs, registration, fines.
Output: {
  "rule": "no_rule_found",
  "subtype": "municipal_code",
  "citation": "Skagit County Code Title 7 (Animals)",
  "evidence_quote": "",
  "full_text": "",
  "deep_link_url": "<original title URL>",
  "status_note": "Title 7 covers dangerous dogs, licensing, animal control — no general beach leash provision found in these excerpts. May exist in Parks/Recreation title (Title 9) or county park rules.",
  "confidence": 0.2,
  "notes": "Confident there's no general leash rule HERE; defer for deeper drill into Title 9 Parks"
}

EXAMPLE 2 (Bainbridge Island — Animals title with leash rule):
Input: Title 6 Ch 6.04 §6.04.010 "Dogs running at large prohibited. It is unlawful for any owner of a dog to suffer or permit such dog to run at large within any park, school grounds, or public place."
Output: {
  "rule": "on_leash",
  "subtype": "municipal_code",
  "citation": "Bainbridge Island Municipal Code §6.04.010 (Dogs Running at Large)",
  "evidence_quote": "It is unlawful for any owner of a dog to suffer or permit such dog to run at large within any park, school grounds, or public place.",
  "full_text": "<full verbatim of §6.04.010>",
  "deep_link_url": "<the chapter URL from the input>",
  "status_note": null,
  "confidence": 0.9,
  "notes": null
}

EXAMPLE 3 (no operative rule found):
Input: Title 12 HEALTH excerpts about food handling, sanitation — no dog mentions.
Output: {
  "rule": "no_rule_found",
  "subtype": "municipal_code",
  "citation": "<jurisdiction> <code>",
  "evidence_quote": "",
  "full_text": "",
  "deep_link_url": "<original URL>",
  "status_note": null,
  "confidence": 0.05,
  "notes": "Title 12 is HEALTH/SANITATION, no dog rules present — heuristic mistakenly picked this title"
}

RULES:
1. The `deep_link_url` MUST be one of the input URLs. Never invent.
2. `evidence_quote` MUST be a verbatim substring of the input text. Don't paraphrase.
3. confidence < 0.7 means human review needed; explain in notes.
4. If unsure between subtypes (e.g., municipal_code vs agency_administrative_policy), prefer the more specific (municipal_code for cities/counties; agency_administrative_policy only for non-codified agency policy pages).
5. KEEP status_note minimal. If you can't think of a useful one-liner, set null.
6. Output ONLY the JSON. No prose. No markdown fences.
"""


@dataclass
class CodifiedRule:
    """Output of Step 6 LLM rule decision — one tuple per jurisdiction."""
    polygon_table:  str
    polygon_key:    str
    rule:           str
    subtype:        str
    citation:       str
    full_text:      str
    source_url:     str          # the chosen deep_link_url
    status_note:    str | None
    evidence_quote: str
    domain:         str = "dog_policy"
    confidence:     float = 0.0
    decided_by:     str = ""    # 'sonnet-4-5' | 'human-reviewed' | 'no_rule_found'
    notes:          str | None = None
    # Populated by Step 6 when the LLM identifies a likely-operative chapter
    # not yet drilled. Triggers Step 6.5 re-drill if confidence < 0.7.
    suggested_next_title:   int | None = None
    suggested_next_chapter: int | None = None
    suggested_next_reason:  str | None = None


def _anthropic_rule_call(jurisdiction_blob: dict) -> tuple[dict, int, int, float]:
    """Call Sonnet with the rule-decision prompt. Returns (parsed, in_tokens, out_tokens, cost)."""
    body = {
        "model": MODEL,
        "max_tokens": 3000,
        "system": [
            {"type": "text", "text": RULE_DECISION_PROMPT, "cache_control": {"type": "ephemeral"}},
        ],
        "messages": [
            {"role": "user", "content": json.dumps(jurisdiction_blob, ensure_ascii=False)},
        ],
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
    text = "".join(b.get("text", "") for b in resp.get("content", []))
    stripped = text.strip()
    if stripped.startswith("```"):
        parts = stripped.split("```", 2)
        if len(parts) >= 2:
            inner = parts[1]
            if inner.lower().startswith("json"):
                inner = inner[4:].lstrip()
            stripped = inner.rsplit("```", 1)[0].strip()
    parsed = json.loads(stripped)
    usage = resp.get("usage", {})
    in_t  = usage.get("input_tokens", 0)
    out_t = usage.get("output_tokens", 0)
    cache_r = usage.get("cache_read_input_tokens", 0)
    cost = (in_t * 3.0 + out_t * 15.0 + cache_r * 0.30) / 1_000_000
    return parsed, in_t, out_t, cost


def step_6_decide_rule(jc: JurisdictionClassification, platform: PlatformDiscovery,
                       fetched: VerbatimFetch) -> CodifiedRule | None:
    """Read fetched chapter+section text, ask Sonnet to identify the operative
    rule. Returns one CodifiedRule (or None if no readable input)."""
    if not fetched.fetched:
        return None
    if not ANTHROPIC_KEY:
        return None

    # Pack the FetchedChapter + drilled FetchedSection content into a tight blob
    chapters_blob = []
    for c in fetched.fetched:
        ch_entry = {
            "url":           c.url,
            "title_excerpt": c.text_excerpt,
            "has_dog_terms": c.has_dog_terms,
            "drilled_sections": [
                {"url": d.url, "link": d.link_text,
                 "text": d.text_excerpt, "has_dog_terms": d.has_dog_terms}
                for d in c.drilled
            ],
        }
        chapters_blob.append(ch_entry)

    blob = {
        "jurisdiction": {
            "name":             jc.name,
            "state":            jc.state,
            "governance_class": jc.governance_class,
        },
        "platform":  platform.platform,
        "chapters":  chapters_blob,
    }

    try:
        parsed, in_t, out_t, cost = _anthropic_rule_call(blob)
    except Exception as e:
        print(f"      ERROR: rule-decision LLM call failed: {e}", file=sys.stderr)
        return None

    sng = parsed.get("suggested_next_chapter") or {}
    return CodifiedRule(
        polygon_table=jc.polygon_table or "",
        polygon_key=jc.polygon_key or "",
        rule=parsed.get("rule", "no_rule_found"),
        subtype=parsed.get("subtype", "municipal_code"),
        citation=parsed.get("citation", ""),
        full_text=parsed.get("full_text", ""),
        source_url=parsed.get("deep_link_url", ""),
        status_note=parsed.get("status_note"),
        evidence_quote=parsed.get("evidence_quote", ""),
        confidence=float(parsed.get("confidence", 0.0)),
        decided_by=MODEL,
        notes=parsed.get("notes"),
        suggested_next_title=sng.get("title"),
        suggested_next_chapter=sng.get("chapter"),
        suggested_next_reason=sng.get("reason"),
    )


# ─── Step 6.5 — LLM-guided re-drill ────────────────────────────────────

def _construct_codepublishing_url(jc: JurisdictionClassification,
                                  platform: PlatformDiscovery,
                                  title: int, chapter: int | None) -> str | None:
    """Construct a codepublishing URL for a specific title/chapter.
    Per the platform's directory convention:
      /<STATE>/<JurisdictionConcat>/html/<JC><N2>/<JC><N2>[<M2>].html
    """
    if platform.platform != "codepublishing":
        return None
    if not platform.valid_url:
        return None
    juris_concat = _slug_no_separator(jc.name)  # preserves case + County suffix
    title_dir = f"{juris_concat}{title:02d}"
    if chapter is not None:
        filename = f"{juris_concat}{title:02d}{chapter:02d}.html"
    else:
        filename = f"{juris_concat}{title:02d}.html"
    base = platform.valid_url.rstrip("/")
    return f"{base}/html/{title_dir}/{filename}"


# ─── Step 6.7 — agency-policy fallback (uses existing agency.web_url) ──

# Common URL path patterns where city/agency policy pages live.
AGENCY_POLICY_PATH_HINTS = [
    "/dogs", "/dogs-on-beach", "/dogs-at-the-beach", "/pets",
    "/parks/dogs", "/parks/rules", "/parks-rules", "/park-rules",
    "/beach-rules", "/beaches/rules", "/beach-info",
    "/animal-control", "/animals",
]


def step_6_7_agency_policy_fallback(jc: JurisdictionClassification,
                                    sc: ScopeCheck,
                                    prior_rule: CodifiedRule | None) -> CodifiedRule | None:
    """When codified-source paths (Steps 2-6.5) defer, try the agency's
    own web_url. The agency's "Dogs in X" page IS often the operative
    source per [[ca-codify-v1-lessons]] (39% of CA codify is
    agency_administrative_policy).

    v1 minimal: requires sc.agency_web_url to be populated in DB. The
    no-agency-row case (most WA jurisdictions today) is deferred to Step
    6.8 per [[codify-step-6-8-web-search-fallback]]."""
    if prior_rule and prior_rule.confidence >= 0.7:
        return None  # codified path worked
    if not sc.agency_web_url:
        return None  # no agency URL — needs Step 6.8

    fake_platform = PlatformDiscovery(
        valid_url=sc.agency_web_url, platform="agency_homepage",
        tried=[], notes="agency-policy fallback",
    )

    # Strategy: fetch the agency homepage, extract its nav links, score
    # them with the same animal/dog/leash/park heuristic that Step 3
    # uses, and follow the top 2-3. Path-hint guessing (/dogs, /pets,
    # etc.) doesn't work for most cities since URL structures vary
    # (/index.php/?id=42, /Departments/Parks, etc.) — real nav links
    # are the authoritative source.
    home_text, home_html = _fetch_text(sc.agency_web_url, platform="agency_homepage")
    if not home_html:
        return None

    nav_anchors = _extract_anchor_links(home_html, sc.agency_web_url)
    # Keep only links that stay within the same domain
    from urllib.parse import urlparse
    home_host = urlparse(sc.agency_web_url).netloc
    same_domain_anchors = [(href, text) for href, text in nav_anchors
                           if urlparse(href).netloc in ("", home_host)
                           and href != sc.agency_web_url
                           and len(text) > 2 and len(text) < 80]

    scored = _score_title_links(same_domain_anchors, jc.governance_class)
    # Also include the homepage itself as a fallback context for the LLM
    valid_candidates: list[ChapterCandidate] = [
        ChapterCandidate(url=sc.agency_web_url,
                         link_text=sc.agency_name or "(homepage)",
                         score=20, why="agency_homepage_context"),
    ]
    valid_candidates.extend(scored[:3])

    if len(valid_candidates) <= 1:
        return None  # no dog-relevant nav links found

    fake_nav = ChapterNavigation(
        best_url=valid_candidates[0].url,
        best_text=valid_candidates[0].link_text,
        best_score=valid_candidates[0].score,
        candidates=valid_candidates,
        method="agency_policy_fallback",
        notes=f"reached {len(valid_candidates)} valid path(s) on {sc.agency_web_url}",
    )

    new_fetch = step_4_fetch_verbatim(jc, fake_platform, fake_nav,
                                      max_candidates=3, drill=False)
    if not new_fetch.fetched:
        return None

    new_rule = step_6_decide_rule(jc, fake_platform, new_fetch)
    return new_rule


def step_6_5_llm_guided_redrill(jc: JurisdictionClassification,
                                platform: PlatformDiscovery,
                                first_rule: CodifiedRule) -> CodifiedRule | None:
    """If Step 6 deferred (confidence < 0.7) AND the LLM suggested a specific
    chapter, re-drill there and re-decide. Returns the new rule (which
    supersedes the first) or None if no follow-up possible."""
    if first_rule.confidence >= 0.7:
        return None
    if first_rule.suggested_next_title is None:
        return None

    new_url = _construct_codepublishing_url(
        jc, platform,
        first_rule.suggested_next_title,
        first_rule.suggested_next_chapter,
    )
    if not new_url:
        # Only codepublishing URL construction supported in v1
        return None

    # Validate the constructed URL is fetchable before re-drilling
    body, why = _smart_fetch(new_url, platform=platform.platform)
    if body is None:
        return None

    # Build a single-candidate ChapterNavigation pointing at the suggested URL
    title_label = f"Title {first_rule.suggested_next_title}"
    if first_rule.suggested_next_chapter is not None:
        title_label += f" Ch {first_rule.suggested_next_title}.{first_rule.suggested_next_chapter:02d}"
    fake_nav = ChapterNavigation(
        best_url=new_url, best_text=title_label, best_score=100,
        candidates=[ChapterCandidate(url=new_url, link_text=title_label,
                                      score=100, why="llm_suggested")],
        method="llm_guided",
        notes=f"LLM suggested: {first_rule.suggested_next_reason}",
    )

    # Re-run Step 4 with the new starting point + re-decide
    new_fetch = step_4_fetch_verbatim(jc, platform, fake_nav,
                                       max_candidates=1, drill=True)
    if not new_fetch.fetched:
        return None
    new_rule = step_6_decide_rule(jc, platform, new_fetch)
    return new_rule


def step_4_fetch_verbatim(jc: JurisdictionClassification,
                          platform: PlatformDiscovery,
                          chapter: ChapterNavigation,
                          max_candidates: int = 3,
                          drill: bool = True) -> VerbatimFetch:
    """For each of Step 3's top candidates: fetch the title page, then drill
    into chapter sub-links when title content is dog-relevant. Returns
    aggregated chapter + drilled-section text for Step 6.

    Per [[ca-codify-v1-lessons]] caveat: Step 3's top pick can be wrong;
    Step 6 LLM reads all and decides which contains the operative rule.

    Drilling adds the title→chapter→section navigation for codepublishing /
    Municode where titles are TOC pages and the actual rule text lives in
    chapter sub-pages."""
    if not chapter.candidates:
        return VerbatimFetch([], "error",
                             f"no chapter candidates from Step 3 (method={chapter.method})")

    cfg = PLATFORM_FETCH_CONFIG.get(platform.platform or "", {"mode": "urllib"})
    is_pw = cfg["mode"] == "playwright"

    out: list[FetchedChapter] = []
    for cand in chapter.candidates[:max_candidates]:
        text, raw_html = _fetch_text(cand.url, platform=platform.platform)
        if not text or len(text) < 100:
            continue
        has_dog = _has_dog_terms(text)

        # Drill into sub-chapters when title is dog-relevant (saves cost on
        # the wrong candidates)
        drilled: list[FetchedSection] = []
        if drill and has_dog and raw_html:
            drilled = _drill_subsections(cand.url, raw_html, platform.platform,
                                         jc, max_subs=3)

        out.append(FetchedChapter(
            url=cand.url, link_text=cand.link_text,
            text_excerpt=text[:2000], text_length=len(text),
            has_dog_terms=has_dog, drilled=drilled,
        ))
        if is_pw:
            time.sleep(1.0)

    if not out:
        return VerbatimFetch([], "error",
                             f"no candidates returned readable content (Playwright={is_pw})")

    n_drilled = sum(len(c.drilled) for c in out)
    return VerbatimFetch(
        fetched=out,
        fetch_mode="playwright" if is_pw else "static_html",
        notes=f"fetched {len(out)} chapter(s) + drilled {n_drilled} sub-section(s); "
              f"{sum(1 for c in out if c.has_dog_terms)} chapter(s) dog-relevant",
    )


# ─── Per-jurisdiction orchestration ────────────────────────────────────

def process_jurisdiction(name: str, state: str, dry_run: bool = True) -> dict:
    """Run Steps 0/0.5/1 on one jurisdiction. Returns a report dict."""
    print(f"\n=== {name} ({state}) ===")

    # Step 0 — Classify
    jc = classify_jurisdiction(name, state)
    print(f"  [0] classify → {jc.governance_class}  (polygon_table={jc.polygon_table} polygon_key={jc.polygon_key})")
    if jc.classify_notes:
        print(f"       notes: {jc.classify_notes}")

    # Step 0.5 — Triage
    td = triage(jc)
    print(f"  [0.5] triage → {td.action}  ({td.reason})")
    if td.action != "proceed":
        return {
            "name": name, "state": state,
            "classification": asdict(jc),
            "triage": asdict(td),
            "scope_check": None,
            "steps_2_4": None,
            "final": td.action,
        }

    # Step 1 — Scope check
    sc = scope_check(jc)
    print(f"  [1] scope → {sc.decision}  (agency_id={sc.agency_id} "
          f"name={sc.agency_name!r} type={sc.agency_type})")
    print(f"       existing_ps_count={sc.existing_ps_count}")

    # Step 2 — Platform discovery
    plat = step_2_discover_platform(jc, sc)
    if plat.valid_url:
        print(f"  [2] platform → {plat.platform}  {plat.valid_url}")
        print(f"       ({len(plat.tried)} attempts; first valid wins)")
    else:
        print(f"  [2] platform → NONE FOUND  ({len(plat.tried)} attempts)")
        for t in plat.tried[:6]:
            print(f"       - {t['platform']:<15} {t['candidate_url']}  → {t['why']}")
        if len(plat.tried) > 6:
            print(f"       … and {len(plat.tried)-6} more")

    # Step 3 — Navigate to operative chapter
    chap = step_3_navigate_chapter(jc, plat)
    if chap.best_url:
        print(f"  [3] chapter → {chap.best_text!r}  (score={chap.best_score} {chap.notes})")
        print(f"       url={chap.best_url}")
        if len(chap.candidates) > 1:
            for c in chap.candidates[1:4]:
                print(f"       alt: '{c.link_text}' score={c.score} why={c.why}")
    else:
        print(f"  [3] chapter → NONE  ({chap.method}: {chap.notes})")

    # Step 4 — Fetch verbatim text for top-N chapter candidates (+ drill)
    text = step_4_fetch_verbatim(jc, plat, chap)
    if text.fetched:
        print(f"  [4] verbatim → {text.notes} ({text.fetch_mode})")
        for f in text.fetched:
            marker = "🐕" if f.has_dog_terms else "  "
            print(f"       {marker} title {f.text_length:>6} chars  '{(f.link_text or '')[:40]}'")
            for d in f.drilled:
                dmark = "🐕" if d.has_dog_terms else "  "
                short_url = d.url.rsplit("/", 1)[-1]
                print(f"         └ {dmark} {d.text_length:>6} chars  '{(d.link_text or '')[:40]}'  {short_url}")
    else:
        print(f"  [4] verbatim → none ({text.fetch_mode}: {text.notes})")

    # Step 6 — LLM rule decision
    cr = step_6_decide_rule(jc, plat, text)
    if cr:
        gate_ok, gate_why = is_url_deep_enough(cr.subtype, cr.source_url)
        conf_marker = "✅" if cr.confidence >= 0.7 else ("⚠️ review" if cr.confidence >= 0.4 else "❌ defer")
        gate_marker = "✅" if gate_ok else f"❌ {gate_why}"
        print(f"  [6] rule → {cr.rule}  conf={cr.confidence:.2f} {conf_marker}")
        print(f"       subtype={cr.subtype}  citation={cr.citation!r}")
        print(f"       url={cr.source_url}  url_gate={gate_marker}")
        if cr.evidence_quote:
            print(f"       evidence: {cr.evidence_quote[:140]!r}")
        if cr.status_note:
            print(f"       status_note: {cr.status_note}")
        if cr.notes:
            print(f"       LLM notes: {cr.notes}")

        # Step 6.5 — LLM-guided re-drill if confidence < 0.7 and LLM suggested
        if cr.confidence < 0.7 and cr.suggested_next_title is not None:
            ch_label = f"Title {cr.suggested_next_title}"
            if cr.suggested_next_chapter is not None:
                ch_label += f" Ch {cr.suggested_next_title}.{cr.suggested_next_chapter:02d}"
            print(f"  [6.5] LLM suggested → {ch_label}  ({cr.suggested_next_reason})")
            cr2 = step_6_5_llm_guided_redrill(jc, plat, cr)
            if cr2:
                gate2_ok, gate2_why = is_url_deep_enough(cr2.subtype, cr2.source_url)
                conf2_marker = "✅" if cr2.confidence >= 0.7 else ("⚠️ review" if cr2.confidence >= 0.4 else "❌ defer")
                print(f"  [6.5] re-decide → {cr2.rule}  conf={cr2.confidence:.2f} {conf2_marker}")
                print(f"       subtype={cr2.subtype}  citation={cr2.citation!r}")
                print(f"       url={cr2.source_url}  url_gate={'✅' if gate2_ok else '❌ '+gate2_why}")
                if cr2.evidence_quote:
                    print(f"       evidence: {cr2.evidence_quote[:140]!r}")
                cr = cr2  # supersede
            else:
                print(f"  [6.5] re-drill failed (URL not constructible or fetch error)")

        # Step 6.7 — agency-policy fallback (only if Step 6 + 6.5 both deferred)
        if cr.confidence < 0.7 and sc.agency_web_url:
            print(f"  [6.7] agency-policy fallback → scanning {sc.agency_web_url}")
            cr3 = step_6_7_agency_policy_fallback(jc, sc, cr)
            if cr3:
                gate3_ok, gate3_why = is_url_deep_enough(cr3.subtype, cr3.source_url)
                conf3_marker = "✅" if cr3.confidence >= 0.7 else ("⚠️ review" if cr3.confidence >= 0.4 else "❌ defer")
                print(f"  [6.7] re-decide → {cr3.rule}  conf={cr3.confidence:.2f} {conf3_marker}")
                print(f"       subtype={cr3.subtype}  citation={cr3.citation!r}")
                print(f"       url={cr3.source_url}  url_gate={'✅' if gate3_ok else '❌ '+gate3_why}")
                if cr3.evidence_quote:
                    print(f"       evidence: {cr3.evidence_quote[:140]!r}")
                cr = cr3
            else:
                print(f"  [6.7] agency-policy fallback found nothing valid")
        elif cr.confidence < 0.7 and not sc.agency_web_url:
            print(f"  [6.7] SKIPPED (no agency.web_url; need Step 6.8 web-search per [[codify-step-6-8-web-search-fallback]])")
    else:
        print(f"  [6] rule → SKIPPED (no fetched content or no ANTHROPIC_API_KEY)")
        # If Step 6 didn't even run AND we have an agency.web_url, jump
        # straight to the agency-policy fallback. This is the common case
        # when no platform validated (Step 2 returned NONE FOUND).
        if sc.agency_web_url:
            print(f"  [6.7] agency-policy fallback → scanning {sc.agency_web_url}")
            cr3 = step_6_7_agency_policy_fallback(jc, sc, None)
            if cr3:
                gate3_ok, gate3_why = is_url_deep_enough(cr3.subtype, cr3.source_url)
                conf3_marker = "✅" if cr3.confidence >= 0.7 else ("⚠️ review" if cr3.confidence >= 0.4 else "❌ defer")
                print(f"  [6.7] re-decide → {cr3.rule}  conf={cr3.confidence:.2f} {conf3_marker}")
                print(f"       subtype={cr3.subtype}  citation={cr3.citation!r}")
                print(f"       url={cr3.source_url}  url_gate={'✅' if gate3_ok else '❌ '+gate3_why}")
                if cr3.evidence_quote:
                    print(f"       evidence: {cr3.evidence_quote[:140]!r}")
                cr = cr3
            else:
                print(f"  [6.7] agency-policy fallback found nothing valid")

    return {
        "name": name, "state": state,
        "classification": asdict(jc),
        "triage": asdict(td),
        "scope_check": asdict(sc),
        "platform": asdict(plat),
        "navigate": asdict(chap),
        "fetch": text,
        "final": "step_3_complete; 4 stubbed",
    }


# ─── Main ──────────────────────────────────────────────────────────────

def list_state_jurisdictions(state: str, pilot: int | None = None) -> list[tuple[str, str]]:
    """List jurisdictions for a state from public.jurisdictions, ordered by
    name. Filters to incorporated places (place_type LIKE 'C%') + counties."""
    params: dict = {
        "select": "name,namelsad,place_type",
        "state":  f"eq.{state}",
        "order":  "name.asc",
    }
    if pilot:
        params["limit"] = str(pilot)
    rows = supa("/rest/v1/jurisdictions", params=params)
    return [(r["name"], state) for r in rows
            if (r.get("place_type") or "").upper().startswith("C")
            or "county" in (r.get("name") or "").lower()]


def main() -> int:
    ap = argparse.ArgumentParser(description="Codify v1 — derive policy_source per jurisdiction")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--jurisdiction", help="Single jurisdiction name (e.g. 'Skagit County')")
    g.add_argument("--state", help="Process all jurisdictions in this state")
    g.add_argument("--from-csv", help="CSV with jurisdiction,state columns")
    ap.add_argument("--state-of", dest="state_of",
                    help="State (with --jurisdiction). e.g. WA")
    ap.add_argument("--pilot", type=int, help="Cap to first N jurisdictions (with --state)")
    ap.add_argument("--dry-run", action="store_true", default=True,
                    help="Don't write to DB (default true while Steps 2-4 are stubbed)")
    args = ap.parse_args()

    # Build the list of (name, state) tuples to process
    queue: list[tuple[str, str]] = []
    if args.jurisdiction:
        if not args.state_of:
            print("ERROR: --jurisdiction requires --state-of", file=sys.stderr)
            return 2
        queue.append((args.jurisdiction, args.state_of))
    elif args.state:
        queue = list_state_jurisdictions(args.state, pilot=args.pilot)
    elif args.from_csv:
        with open(args.from_csv, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                queue.append((row["jurisdiction"], row["state"]))

    if not queue:
        print("No jurisdictions to process.", file=sys.stderr)
        return 0

    print(f"Queue: {len(queue)} jurisdiction(s)")
    reports = []
    tally = {"proceed": 0, "skip_covered_by_parent": 0, "defer": 0,
             "branch_federal": 0, "create_new": 0, "supplement_existing": 0,
             "no_agency": 0}
    for name, state in queue:
        try:
            r = process_jurisdiction(name, state, dry_run=args.dry_run)
            reports.append(r)
            tally[r["triage"]["action"]] = tally.get(r["triage"]["action"], 0) + 1
            if r.get("scope_check"):
                tally[r["scope_check"]["decision"]] = tally.get(r["scope_check"]["decision"], 0) + 1
        except Exception as e:
            print(f"  ERROR on {name}: {e}")
            reports.append({"name": name, "state": state, "error": str(e)})
        time.sleep(0.15)  # gentle pacing

    print("\n=== TOTALS ===")
    for k, v in tally.items():
        print(f"  {k:<28} {v}")
    print(f"  total processed:             {len(reports)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
