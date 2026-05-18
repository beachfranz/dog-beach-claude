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
        return ScopeCheck(None, None, None, 0, [], "no_agency")

    # Try bare-name canonical (per tenet 4 — bare name, not "X, City of")
    agency_name_candidates = [jc.name]
    if jc.name.startswith("City of "):
        agency_name_candidates.append(jc.name.replace("City of ", ""))
    elif jc.name.endswith(" County") and expected_type == "county":
        agency_name_candidates.append(jc.name)  # already bare-name-like

    agency_row = None
    for cand in agency_name_candidates:
        rows = supa("/rest/v1/agency", params={
            "select": "id,name,type",
            "name":   f"eq.{cand}",
            "type":   f"eq.{expected_type}",
            "limit":  "1",
        })
        if rows:
            agency_row = rows[0]
            break

    if not agency_row:
        return ScopeCheck(None, None, expected_type, 0, [], "no_agency")

    # Count existing policy_source rows issued by this agency
    ps_rows = supa("/rest/v1/policy_source", params={
        "select": "id,citation",
        "issuing_agency_id": f"eq.{agency_row['id']}",
        "limit":  "50",
    })

    if ps_rows:
        return ScopeCheck(
            agency_id=agency_row["id"],
            agency_name=agency_row["name"],
            agency_type=agency_row["type"],
            existing_ps_count=len(ps_rows),
            existing_ps_ids=[r["id"] for r in ps_rows],
            decision="supplement_existing",
        )

    return ScopeCheck(
        agency_id=agency_row["id"],
        agency_name=agency_row["name"],
        agency_type=agency_row["type"],
        existing_ps_count=0,
        existing_ps_ids=[],
        decision="create_new",
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
    "municode":       {"mode": "playwright", "wait_seconds": 12.0, "selector": ".codes-chunks-pg"},
    "ecode360":       {"mode": "playwright", "wait_seconds": 8.0,  "selector": None},
    "codepublishing": {"mode": "urllib",     "wait_seconds": 0,    "selector": None},
    "amlegal":        {"mode": "urllib",     "wait_seconds": 0,    "selector": ".section-content"},
    "qcode":          {"mode": "urllib",     "wait_seconds": 0,    "selector": None},
    "county_codes":   {"mode": "urllib",     "wait_seconds": 0,    "selector": None},
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


# ─── Step 4 — Fetch verbatim text (STUBBED) ────────────────────────────

def step_4_fetch_verbatim(jc: JurisdictionClassification,
                          chapter: ChapterNavigation) -> dict:
    """STUB. Next: Playwright deep-fetch of the operative section text."""
    return {"status": "not_implemented", "step": "4_fetch_verbatim"}


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

    # Step 4 — stubbed
    text = step_4_fetch_verbatim(jc, chap)

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
