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
        # Federal codify path enabled 2026-05-18 per #1 encapsulation.
        # Federal units codify via:
        #   (a) Federal CFR baselines via --state-agency-rule mode (one-shot per
        #       agency: NPS 36 CFR §2.15 baseline shipped 2026-05-18 for all
        #       NPS-managed beaches in CA/OR/WA → 79 beaches)
        #   (b) Per-unit Compendium / unit-policy via --state-agency-rule with
        #       --pad-unit-name-ilike for the specific unit
        #   (c) --list-federal-coverage helper enumerates units + suggests
        #       per-unit codify commands per state
        # If --jurisdiction "Olympic National Park" is invoked, proceed
        # through the standard pipeline; federal Phase A discovers nps.gov/
        # USFS/etc URL patterns via web_search Step 6.8.
        return TriageDecision(
            action="proceed",
            reason="Federal codify path enabled (per #1 encapsulation 2026-05-18)",
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


# ─── Step 1.5 — State-baseline coverage check ───────────────────────────
#
# Added 2026-05-18 per #10 encapsulation: in state-baseline-dominant regimes
# (e.g., OR via OAR Ocean Shore Rules + ORS §609), the state-level policy_source
# rows already cover most or all city/county beaches. Codifying per-city/per-
# county in those regimes wastes Phase A → Step 6.8 cycles with low marginal
# value. Marginal value IS still real (city-specific overrides like off-leash
# designations), but should be opt-in rather than default.
#
# Mechanism: query beaches in the jurisdiction's polygon → count how many
# already have consensus_confidence ≥ baseline-conf AND have ≥1 state-issued
# ps row in their beach_policy_source links → if ratio ≥ threshold, defer.
#
# Override: --ignore-state-baseline disables the check entirely.

@dataclass
class StateBaselineCoverage:
    n_beaches_total:   int
    n_beaches_covered: int   # have ≥1 state-issued ps at conf ≥ baseline_conf
    coverage_pct:      float # 0.0 to 1.0
    state_ps_ids:      list[int]  # which state ps rows cover these beaches
    skip_decision:     bool
    reason:            str


def step_1_5_state_baseline_coverage(
    jc: JurisdictionClassification,
    threshold: float = 0.9,
    baseline_conf: float = 0.7,
) -> StateBaselineCoverage:
    """Check if state-level ps rows already cover beaches in this jurisdiction.

    Returns (skip_decision=True, reason) when:
      - jurisdiction polygon intersects ≥1 beach (else N/A)
      - ≥ threshold fraction of those beaches have:
          * consensus_confidence ≥ baseline_conf in beach_dog_policy, AND
          * ≥1 beach_policy_source link to a ps row whose issuing_agency.type
            is in ('state','state_department') OR subtype is state-class

    Skip == True signals "state baseline covers this; per-jurisdiction codify
    is low marginal value." Override with --ignore-state-baseline.

    Idempotent — no writes; safe to call any time.
    """
    if jc.polygon_table is None or jc.polygon_key is None:
        return StateBaselineCoverage(0, 0, 0.0, [], False,
                                     "no polygon — cannot evaluate baseline coverage")

    conn = _connect_pg()
    try:
        with conn, conn.cursor() as cur:
            # All active beaches whose geom intersects the jurisdiction polygon
            cur.execute(
                f"""
                SELECT b.fid, COALESCE(bdp.consensus_confidence, 0) AS conf,
                       COALESCE(
                         (SELECT array_agg(DISTINCT ps.id)
                          FROM public.beach_policy_source bps
                          JOIN public.policy_source ps ON ps.id = bps.policy_source_id
                          LEFT JOIN public.agency a ON a.id = ps.issuing_agency_id
                          WHERE bps.beach_fid = b.fid
                            AND (a.type IN ('state','state_department')
                                 OR ps.subtype IN ('state_regulation','state_statute','federal_regulation'))),
                         '{{}}'::bigint[]
                       ) AS state_ps_ids
                FROM public.beaches_gold b
                LEFT JOIN public.beach_dog_policy bdp ON bdp.arena_group_id = b.fid
                JOIN public.{jc.polygon_table} p ON ST_Intersects(b.geom, p.geom)
                WHERE b.is_active
                  AND b.state = %s
                  AND p.{'id' if jc.polygon_table != 'counties' else 'geoid'} = %s
                """,
                (jc.state, jc.polygon_key),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    n_total = len(rows)
    if n_total == 0:
        return StateBaselineCoverage(0, 0, 0.0, [], False,
                                     "no beaches in polygon — pre-filter should have caught this")

    covered_ps_ids: set[int] = set()
    n_covered = 0
    for _fid, conf, ps_ids in rows:
        if conf >= baseline_conf and ps_ids and len(ps_ids) > 0:
            n_covered += 1
            for psid in ps_ids:
                covered_ps_ids.add(psid)

    pct = n_covered / n_total if n_total else 0.0
    skip = pct >= threshold
    reason = (
        f"state baseline covers {n_covered}/{n_total} beaches "
        f"({pct:.0%}) at conf ≥ {baseline_conf:.0%} (threshold {threshold:.0%})"
    )
    return StateBaselineCoverage(
        n_beaches_total=n_total,
        n_beaches_covered=n_covered,
        coverage_pct=pct,
        state_ps_ids=sorted(covered_ps_ids),
        skip_decision=skip,
        reason=reason,
    )


# ─── Step 2 — Platform discovery ───────────────────────────────────────

# Per-state platform priority. WA = codepublishing dominant (per the WA
# codify audit); CA = Municode dominant; OR = Municode-then-codepublishing.
# Inline config for v1; migrate to DB table if it grows. See
# docs/codify_pip_resolver_architecture.md.
STATE_PLATFORM_PRIORITY = {
    # Every state must list ALL platforms (omitting any means a city on the
    # missing platform is silently classified NONE). Order = preference;
    # priority-ones win first, long-tail ones catch the rest.
    "WA": ["codepublishing", "municode", "amlegal", "county_codes", "ecode360", "qcode"],
    "OR": ["municode", "codepublishing", "amlegal", "ecode360", "qcode", "county_codes"],
    "CA": ["municode", "amlegal", "qcode", "ecode360", "codepublishing", "county_codes"],
    "MI": ["municode", "amlegal", "ecode360", "codepublishing", "qcode", "county_codes"],
    "MA": ["ecode360", "amlegal", "municode", "codepublishing", "qcode", "county_codes"],
    "_default": ["municode", "amlegal", "qcode", "ecode360", "codepublishing", "county_codes"],
}

# Per-platform fetch config. JS-rendered platforms route through Playwright
# (via scripts/fetch/fetch_html.py). Static-HTML platforms use urllib.
# `selector` is the CSS selector for the meaningful content area (per
# [[municode-fetchable]] for Municode).
PLATFORM_FETCH_CONFIG = {
    # Bumped Municode wait from 12→20s per the WA Phase C reliability issues
    # (Bingen + Black Diamond Title 6 found in TOC but content returned empty
    # — likely the chunks-pg selector hadn't fully rendered).
    "municode":         {"mode": "playwright", "wait_seconds": 20.0, "selector": ".codes-chunks-pg"},
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


# In-process cache for Municode state catalog. Keyed by state. Per
# [[municode-silent-redirect]]: probe the state landing once, parse rendered
# jurisdiction names, skip all per-jurisdiction probes for cities/counties
# not in the catalog. Saves 4× wasted Playwright calls per non-Municode
# jurisdiction. Fail-open: empty set on fetch failure = don't block discovery.
_MUNICODE_CATALOG_CACHE: dict[str, set[str]] = {}
_MUNICODE_PRECHECK_DISABLED: bool = False  # toggled by --no-municode-precheck


def municode_state_catalog(state: str) -> set[str]:
    """Return set of jurisdiction names known to be in Municode's catalog
    for `state` (lowercased state code). Cached in-process. Fail-open."""
    state_lc = state.lower()
    if state_lc in _MUNICODE_CATALOG_CACHE:
        return _MUNICODE_CATALOG_CACHE[state_lc]
    catalog: set[str] = set()
    if not PLAYWRIGHT_AVAILABLE:
        _MUNICODE_CATALOG_CACHE[state_lc] = catalog
        return catalog
    try:
        text = playwright_fetch(
            f"https://library.municode.com/{state_lc}",
            selector=None, raw_html=False, wait_seconds=15.0, timeout_ms=30000,
        ) or ""
        # Rendered listing contains jurisdiction names, sometimes line-separated,
        # sometimes with "County" suffix. Extract anything that looks like a
        # municipal/county name. Permissive regex — false positives are harmless
        # (they just mean we attempt a probe that will fail validation).
        import re as _re
        for m in _re.finditer(r"([A-Z][a-zA-Z'\-\.]+(?:\s+[A-Z][a-zA-Z'\-\.]+){0,4})\s+County", text):
            catalog.add(m.group(1).strip() + " County")
            catalog.add(m.group(1).strip())  # bare-name variant
        # Also lines that look like standalone city names (each on own line
        # or comma-separated). Conservative: only words starting with uppercase
        # not already added as county-prefix.
        for line in text.splitlines():
            line = line.strip()
            if 2 <= len(line) <= 40 and line[0].isupper() and not line.endswith(":"):
                # Strip trailing whitespace/punctuation
                line = line.rstrip(",;.")
                if line and " " not in line[:1]:
                    catalog.add(line)
    except Exception as e:
        print(f"  [municode-precheck] catalog fetch failed for {state_lc}: {type(e).__name__}")
    _MUNICODE_CATALOG_CACHE[state_lc] = catalog
    return catalog


def _candidates_municode(jc: JurisdictionClassification, state: str) -> list[PlatformCandidate]:
    """Municode: library.municode.com/<state>/<slug>/codes/<doc>

    Per [[ca-codify-v1-lessons]]:
    - 4 doc_slugs in priority order cover 100% of CA Municode URLs:
        code_of_ordinances (83%) → municipal_code → ordinance_code → code
    - Counties ALWAYS use `_county`-suffixed slug (CA: 35/35 = 100%);
      cities use bare slug. No need to try both.

    Per [[municode-silent-redirect]]: pre-check the state catalog
    (library.municode.com/<state>) so we don't burn 4× Playwright calls
    on jurisdictions not in Municode's catalog at all.

    Net: 1 slug × 4 docs = 4 candidates. Step 2 returns on first valid
    hit so most cases terminate after candidate 1 (~12s Playwright)."""

    # Pre-check: skip if jurisdiction not in Municode's state catalog.
    # Disabled when --no-municode-precheck or catalog fetch failed (fail-open).
    if not _MUNICODE_PRECHECK_DISABLED:
        catalog = municode_state_catalog(state)
        if catalog:  # non-empty = we have a catalog to check against
            bare_name = jc.name.replace("City of ", "").strip()
            name_with_county = bare_name if bare_name.endswith(" County") else (bare_name + " County" if jc.governance_class == "county" else None)
            in_catalog = (
                bare_name in catalog
                or jc.name in catalog
                or (name_with_county and name_with_county in catalog)
            )
            if not in_catalog:
                print(f"  [2] municode catalog skip — {jc.name!r} not in library.municode.com/{state.lower()} listing")
                return []

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


# ─── #7 PDF text extraction ─────────────────────────────────────────────
# Many county/city codes are published only as PDFs (Tillamook County OR
# Ordinance 64, Lincoln County OR Chapter 2 PDF, Lane Code LC07.pdf, etc.).
# urllib + Playwright return the binary blob; we need actual text. Per
# [[promote-ad-hoc-tools-to-process]] today's manual workaround (curator
# downloads PDF + pastes text) is encapsulated here.

try:
    import pdfplumber  # type: ignore
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False


def _fetch_pdf_text(url: str, timeout: int = 30) -> tuple[str | None, str]:
    """Download a PDF + extract text via pdfplumber.

    Returns (text, why). text is None on failure; lowercased on success
    for downstream containment checks (matches _smart_fetch contract).

    Caches by URL to disk under tmp/_pdf_cache/<sha>.pdf so we don't
    re-download on retries within the same run.
    """
    if not PDFPLUMBER_AVAILABLE:
        return None, "pdfplumber_unavailable"

    import hashlib, io
    cache_dir = ROOT / "tmp" / "_pdf_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    sha = hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]
    cache_path = cache_dir / f"{sha}.pdf"

    pdf_bytes: bytes | None = None
    if cache_path.exists() and cache_path.stat().st_size > 0:
        pdf_bytes = cache_path.read_bytes()
    else:
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/124.0.0.0 Safari/537.36"),
                "Accept": "application/pdf,*/*",
            })
            with urllib.request.urlopen(req, timeout=timeout) as r:
                if r.status != 200:
                    return None, f"http_{r.status}"
                pdf_bytes = r.read()
                cache_path.write_bytes(pdf_bytes)
        except urllib.error.HTTPError as e:
            return None, f"http_{e.code}"
        except Exception as e:
            return None, f"pdf_download_error:{type(e).__name__}"

    if not pdf_bytes or len(pdf_bytes) < 100:
        return None, "pdf_empty"

    # Quick sniff — confirm we got a PDF, not an HTML error page
    if not pdf_bytes[:8].startswith(b"%PDF"):
        return None, "pdf_signature_missing"

    try:
        text_parts: list[str] = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            # Cap pages (huge ordinance compilations can be hundreds of pages)
            for page in pdf.pages[:80]:
                t = page.extract_text() or ""
                if t:
                    text_parts.append(t)
        text = "\n".join(text_parts)
        return text.lower(), "ok_pdf"
    except Exception as e:
        return None, f"pdf_parse_error:{type(e).__name__}"


def _is_pdf_url(url: str) -> bool:
    """Heuristic: URL has .pdf extension or looks like a DocumentCenter PDF
    endpoint (common pattern on county.gov sites — civicengage CMS)."""
    lc = url.lower()
    return (
        lc.endswith(".pdf")
        or "/documentcenter/view/" in lc  # civicengage PDF endpoint
        or "/cms/one.aspx" in lc and ".pdf" in lc
    )


def _smart_fetch(url: str, platform: str | None = None,
                 timeout: int = 15) -> tuple[str | None, str]:
    """Fetch HTML via the right transport for the platform.

    Returns (body_text, why). body_text is None on failure; lowercased on
    success for downstream containment checks.
    """
    # PDF route — sniff by URL pattern, applied to ALL platforms (county.gov
    # sites often host code chapters as PDFs regardless of which platform
    # generated the candidate URL).
    if _is_pdf_url(url):
        return _fetch_pdf_text(url, timeout=max(timeout, 30))

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
        # Bumped parks/beaches from 50→85 per CA Municode pattern audit:
        # LA Title 17 (PARKS BEACHES), OC Title 2 (Public Facilities),
        # Manhattan Beach Title 12 (Beaches Parks Rec) all host the
        # operative dog rule in non-Animals titles. The Animals/Parks
        # distinction was an oversimplification.
        (95, r"\b(animal|dog|pet|leash)s?\b",            "subject-animal"),
        (85, r"\b(park|beach|recreation)s?\b",           "subject-parks-beaches"),
        (80, r"\b(public peace|nuisance|misdemeanor)\b", "subject-peace"),
        (75, r"\b(public facilit(y|ies))\b",             "subject-public-facilities"),
        (70, r"\b(health|sanitation)\b",                 "subject-health"),
        (60, r"\b(streets?|traffic|parking|public way)\b", "subject-streets"),
    ],
    "county": [
        # County codes can host the rule in either Animals or Parks/Beaches
        # titles — keep both at high score; LLM picks the operative one.
        (95, r"\b(animal|dog|pet|leash)s?\b",            "subject-animal"),
        (90, r"\b(park|beach|recreation)s?\b",           "subject-parks-beaches"),
        (75, r"\b(public works?|public property|public facilit(y|ies))\b", "subject-public"),
        (70, r"\b(health|sanitation|welfare)\b",         "subject-health"),
        (60, r"\b(licensing|control)\b",                 "subject-licensing"),
    ],
    "special_district": [
        (95, r"\b(animal|dog|pet|leash)s?\b",            "subject-animal"),
        (85, r"\b(park|beach|recreation)s?\b",           "subject-parks-beaches"),
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
    # PDF route — applies to ALL platforms (county.gov-hosted PDFs are common)
    if _is_pdf_url(url):
        pdf_text, _ = _fetch_pdf_text(url)
        return pdf_text, None  # no HTML to return for PDFs (no further drill-down)

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
            # Extract text. AVOID a second Playwright call (slow + hard to
            # tune wait_seconds for each platform). Instead extract the
            # selector element from the already-fetched raw_html via regex.
            if selector and selector.startswith(("#", ".")):
                attr = "id" if selector.startswith("#") else "class"
                value = selector[1:]
                m = re.search(rf'<[^>]+\b{attr}="[^"]*\b{re.escape(value)}\b[^"]*"[^>]*>(.*?)</',
                              raw_html, re.DOTALL)
                if m:
                    text = re.sub(r"<[^>]+>", " ", m.group(1))
                else:
                    # selector not found in HTML — fall back to full-page
                    text = re.sub(r"<script[^>]*>.*?</script>", " ", raw_html, flags=re.DOTALL)
                    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL)
                    text = re.sub(r"<[^>]+>", " ", text)
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
  "penalty_summary": null | "<1-2 sentence summary of the enforcement clause for violating WHATEVER rule the section establishes. Applies to both presence violations ('no dogs at all' / 'dogs prohibited') AND leash violations ('off-leash where leash required'). Classification (infraction|misdemeanor|civil) + fine amount(s) if specified. Examples: 'Infraction; fine up to $250 for first offense.' (leash violation); 'Misdemeanor punishable by up to $500 or 30 days.' (no-dogs violation); 'Civil penalty $100.' (either). Null when source text doesn't mention enforcement.>",
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

INFERRING BEACH-APPLICABILITY FROM CITYWIDE RULES (LOAD-BEARING):
A city/county's GENERAL leash or at-large rule applies to its beaches by default.
You should NOT require the text to say "beach" explicitly to find a rule. If the
text establishes "no dog shall run at large in any park, school ground, or public
place" OR "dogs shall be on a leash within the city" OR similar territorial scope,
that IS the operative beach rule.

Confidence calibration:
- 0.95+ when the text explicitly mentions beaches/the specific beach
- 0.80-0.90 when the text is a citywide leash/at-large rule (inferred beach applies)
- 0.40-0.70 when the rule is ambiguous or arguably scoped to a non-beach area
- < 0.40 when there is genuinely no rule and no citywide leash provision in the text

Return `rule: "no_rule_found"` ONLY when:
(a) the text has NO citywide leash/at-large/animal-control rule that could be inferred
(b) AND the text has NO beach-specific dog rule
Not when "the text doesn't mention beaches" alone — that's normal.

EXCEPTIONS that override the city default (handle these as explicit text says):
- Federal land within the city (NPS, USFWS NWR) → that overlay's rule wins
- State park within the city (CA DPR, WSPRC) → state-park rule wins
- Sub-area carve-out (Del Mar off-leash strip) → see Phase I sub-area handling

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

EXAMPLE 2 (Bainbridge Island — citywide leash rule, INFER beach applies):
Input: Title 6 Ch 6.04 §6.04.010 "Dogs running at large prohibited. It is unlawful for any owner of a dog to suffer or permit such dog to run at large within any park, school grounds, or public place."
NOTE: doesn't mention beach explicitly, but "any public place" + territorial scope = beaches included by default.
Output: {
  "rule": "on_leash",
  "subtype": "municipal_code",
  "citation": "Bainbridge Island Municipal Code §6.04.010 (Dogs Running at Large)",
  "evidence_quote": "It is unlawful for any owner of a dog to suffer or permit such dog to run at large within any park, school grounds, or public place.",
  "full_text": "<full verbatim of §6.04.010>",
  "deep_link_url": "<the chapter URL from the input>",
  "status_note": null,
  "confidence": 0.85,
  "notes": "Inferred beach-applicability from citywide leash rule; text doesn't mention beach explicitly but scope is territorial"
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
    # Penalty summary (added 2026-05-18) — enforcement clause for violating
    # the rule (whatever the rule is: presence OR leash). 1-2 sentence summary.
    penalty_summary: str | None = None
    # Populated by Step 6 when the LLM identifies a likely-operative chapter
    # not yet drilled. Triggers Step 6.5 re-drill if confidence < 0.7.
    suggested_next_title:   int | None = None
    suggested_next_chapter: int | None = None
    suggested_next_reason:  str | None = None


def _anthropic_rule_call(jurisdiction_blob: dict, enable_web_search: bool = False) -> tuple[dict, int, int, float]:
    """Call Sonnet with the rule-decision prompt. Returns (parsed, in_tokens, out_tokens, cost).

    When enable_web_search=True, adds Anthropic's web_search tool so Sonnet
    can route around Cloudflare-walled URLs / thin fetch content by searching
    the web for the rule text directly. Adds cost (~$10/1000 searches +
    token usage), so only enabled when the deterministic fetch produced
    thin / blocked content. Per [[ca-codify-v1-lessons]] and the 15 CA ps
    rows already documenting this bypass pattern."""
    body = {
        "model": MODEL,
        "max_tokens": 4000,
        "system": [
            {"type": "text", "text": RULE_DECISION_PROMPT, "cache_control": {"type": "ephemeral"}},
        ],
        "messages": [
            {"role": "user", "content": json.dumps(jurisdiction_blob, ensure_ascii=False)},
        ],
    }
    if enable_web_search:
        body["tools"] = [{
            "type": "web_search_20250305",
            "name": "web_search",
            "max_uses": 4,
        }]
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode("utf-8"), method="POST",
    )
    req.add_header("x-api-key", ANTHROPIC_KEY)
    req.add_header("anthropic-version", "2023-06-01")
    req.add_header("content-type", "application/json")
    with urllib.request.urlopen(req, timeout=180) as r:
        resp = json.loads(r.read().decode("utf-8"))
    # With web_search enabled, response contains tool_use blocks +
    # tool_result blocks + final text. We want the LAST text block (the
    # model's synthesis after tool calls), then extract JSON from it.
    text_blocks = [b.get("text", "") for b in resp.get("content", []) if b.get("type") == "text"]
    text = text_blocks[-1] if text_blocks else ""
    stripped = text.strip()
    # Strip prose preamble (e.g., "Based on my search:\n\n{...}")
    if stripped.startswith("```"):
        parts = stripped.split("```", 2)
        if len(parts) >= 2:
            inner = parts[1]
            if inner.lower().startswith("json"):
                inner = inner[4:].lstrip()
            stripped = inner.rsplit("```", 1)[0].strip()
    # Find the first { and last } (JSON envelope) if prose surrounds
    if not stripped.startswith("{"):
        i, j = stripped.find("{"), stripped.rfind("}")
        if i >= 0 and j > i:
            stripped = stripped[i:j+1]
    parsed = json.loads(stripped)
    usage = resp.get("usage", {})
    in_t  = usage.get("input_tokens", 0)
    out_t = usage.get("output_tokens", 0)
    cache_r = usage.get("cache_read_input_tokens", 0)
    cost = (in_t * 3.0 + out_t * 15.0 + cache_r * 0.30) / 1_000_000
    return parsed, in_t, out_t, cost


def step_6_decide_rule(jc: JurisdictionClassification, platform: PlatformDiscovery,
                       fetched: VerbatimFetch,
                       enable_web_search: bool = False) -> CodifiedRule | None:
    """Read fetched chapter+section text, ask Sonnet to identify the operative
    rule. Returns one CodifiedRule (or None if no readable input).

    enable_web_search=True enables Anthropic's web_search tool, letting Sonnet
    route around Cloudflare-walled URLs by searching for the rule text. Use
    when the deterministic fetch produced thin content (< 500 chars / matches
    Cloudflare challenge keywords)."""
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
    if enable_web_search:
        blob["instructions"] = (
            "The fetched content looks thin or blocked (Cloudflare challenge / "
            "JS-rendered SPA / wrong chapter). Use the web_search tool to find "
            "the actual verbatim rule text for this jurisdiction's dog/leash "
            "ordinance. Cross-check across 1-3 sources. Return the original URL "
            "(if provided) as deep_link_url and the web-searched verbatim quote "
            "as full_text + evidence_quote."
        )

    try:
        parsed, in_t, out_t, cost = _anthropic_rule_call(blob, enable_web_search=enable_web_search)
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
        penalty_summary=parsed.get("penalty_summary"),
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


# ─── Step 6.8 — URL-inference + web-search fallback ────────────────────

# Common patterns for US city / county / agency homepages. Probed in
# order; first 200 OK with sane body length is treated as the agency_web_url
# and handed to Step 6.7's nav-scan + Step 6 LLM eval logic.
def _infer_city_url_candidates(jc: JurisdictionClassification) -> list[str]:
    """Generate plausible city/county website URL patterns."""
    name_lower = jc.name.lower()
    slug_no_space = _slug_no_separator(jc.name).lower()
    slug_dashes = name_lower.replace("city of ", "").replace(" county", "").replace(" ", "-")
    slug_under = name_lower.replace("city of ", "").replace(" county", "").replace(" ", "_")
    state_lc = jc.state.lower()
    candidates: list[str] = []
    if jc.governance_class == "incorporated_city":
        candidates += [
            f"https://www.cityof{slug_no_space}.gov",
            f"https://www.cityof{slug_no_space}.org",
            f"https://www.cityof{slug_no_space}.com",
            f"https://cityof{slug_no_space}.gov",
            f"https://cityof{slug_no_space}.org",
            f"https://www.{slug_no_space}wa.gov" if state_lc == "wa" else None,
            f"https://www.{slug_no_space}or.gov" if state_lc == "or" else None,
            f"https://www.{slug_no_space}.gov",
            f"https://www.{slug_no_space}.{state_lc}.gov",
            f"https://www.{slug_no_space}.{state_lc}.us",
            f"https://{slug_no_space}.{state_lc}.us",
            f"https://www.ci.{slug_dashes}.{state_lc}.us",
            f"https://www.ci.{slug_under}.{state_lc}.us",
        ]
    elif jc.governance_class == "county":
        cty = name_lower.replace(" county", "").replace(" ", "")
        candidates += [
            f"https://www.co.{cty}.{state_lc}.us",
            f"https://www.{cty}county{state_lc}.gov",
            f"https://www.{cty}countygov.com",
            f"https://www.{cty}county.us",
            f"https://{cty}county.{state_lc}.gov",
        ]
    return [c for c in candidates if c]


def step_6_8_url_inference_fallback(jc: JurisdictionClassification,
                                    sc: ScopeCheck,
                                    prior_rule: CodifiedRule | None) -> CodifiedRule | None:
    """Last-resort: when codified-source paths defer AND no agency.web_url:
    (a) infer the city/county homepage URL from name patterns and hand to
        Step 6.7's nav-scan; if that produces nothing, then
    (b) escalate to Sonnet + web_search tool (deep half).

    Per [[codify-step-6-8-web-search-fallback]] design + [[promote-ad-hoc-tools-to-process]]
    web_search promotion. URL inference is the cheap first half (works for
    cities with a discoverable .gov homepage); web_search is the deeper
    second half (works when no platform was found AND no homepage inferred,
    or homepage exists but has no nav-discoverable code link)."""
    if prior_rule and prior_rule.confidence >= 0.7:
        return None
    if sc.agency_web_url:
        return None  # Step 6.7 already handled

    # ── Half 1: URL inference + Step 6.7 nav-scan ─────────────────────
    candidates = _infer_city_url_candidates(jc)
    found_url: str | None = None
    tried_log: list[tuple[str, str]] = []
    for url in candidates[:12]:  # cap to keep wall time bounded
        body, why = _smart_fetch(url, platform="agency_homepage")
        tried_log.append((url, why or "fetch_failed"))
        if body and len(body) > 800:
            # Sanity check: body should mention the jurisdiction name
            bare = jc.name.replace("City of ", "").replace(" County", "").lower()
            if bare in body:
                found_url = url
                break
        time.sleep(0.5)

    if found_url:
        # Hand off to Step 6.7 with the inferred URL as agency_web_url
        fake_sc = ScopeCheck(
            agency_id=sc.agency_id, agency_name=sc.agency_name,
            agency_type=sc.agency_type,
            agency_web_url=found_url,   # ← the inferred URL
            existing_ps_count=sc.existing_ps_count,
            existing_ps_ids=sc.existing_ps_ids,
            decision=sc.decision,
        )
        result = step_6_7_agency_policy_fallback(jc, fake_sc, prior_rule)
        if result and result.confidence >= 0.7:
            return result
        # else fall through to web_search half

    # ── Half 2: Sonnet + web_search escalation ────────────────────────
    # Same pattern as codify_from_manual_url's Cloudflare-thin branch.
    # Sonnet routes around platform-discovery gaps + Cloudflare walls
    # via web_search_20250305 tool. Promoted from ad-hoc to first-class
    # per [[promote-ad-hoc-tools-to-process]] 2026-05-18.
    print(f"  [6.8] URL-inference exhausted → escalating to web_search")
    blob_hint = (
        f"No municipal code platform found for {jc.name}, {jc.state} via standard "
        f"discovery (Municode, ecode360, codepublishing, amlegal, qcode, "
        f"municipal.codes). URL-inference tried {len(candidates)} city-homepage "
        f"patterns without success. Use web_search to locate the operative "
        f"dog/leash/animal-control ordinance for this jurisdiction. "
        f"Examples of where to look: a municipal code platform we may have missed, "
        f"the city's own code page, county code if city defers to county, state "
        f"code if no local ordinance exists. Return the deep-link URL + verbatim "
        f"section text + confidence."
    )
    fake_fetch = VerbatimFetch(
        fetched=[FetchedChapter(
            url=f"https://example.invalid/{jc.state}/{jc.name.replace(' ', '_')}",
            link_text=f"{jc.name} (no platform — use web_search)",
            text_excerpt=blob_hint,
            text_length=len(blob_hint),
            has_dog_terms=True,
            drilled=[],
        )],
        fetch_mode="step_6_8_web_search",
        notes="Step 6.8 web_search escalation; no platform discovered, no inferred homepage panned out",
    )
    fake_platform = PlatformDiscovery(
        valid_url="",
        platform="web_search_fallback",
        tried=[],
        notes=f"Step 6.8 — {len(candidates)} URL-inference candidates tried, none valid",
    )
    return step_6_decide_rule(jc, fake_platform, fake_fetch, enable_web_search=True)


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


# ─── Step 7 — Migration emit ───────────────────────────────────────────

# Per playbook §7 template. Emits ps INSERT (with NOT EXISTS guard +
# canonical citation prefix). When agency row is missing, ALSO emit an
# agency INSERT per tenet 4 (bare-name canonical). No bps INSERT — that's
# the resolver's job in the new architecture (per
# docs/codify_pip_resolver_architecture.md).
def emit_migration_sql(jc: JurisdictionClassification, sc: ScopeCheck,
                       cr: CodifiedRule, platform_used: str | None) -> str:
    """Generate the SQL INSERT block for one codified rule.

    Caller is responsible for confidence-gating (only call for conf >= 0.7).
    Quality-gate the URL via is_url_deep_enough() before calling.
    """
    import datetime
    today = datetime.date.today().isoformat()

    # SQL-escape single quotes by doubling them
    def esc(s: str | None) -> str:
        if s is None:
            return "NULL"
        return "'" + s.replace("'", "''") + "'"

    # Canonical citation prefix for NOT EXISTS guard — first 30 chars
    citation_prefix = cr.citation[:30].replace("'", "''")

    # Verbatim quote with attribution footer (per playbook §7)
    full_text_with_footer = cr.full_text
    if platform_used:
        full_text_with_footer += f"\n\n[Hosted on {platform_used}. Fetched via codify v1 {today}.]"

    # Agency type for lookup (per scope_check)
    agency_type = sc.agency_type or GOVERNANCE_TO_AGENCY_TYPE.get(jc.governance_class, "city")
    agency_name = sc.agency_name or jc.name

    parts = []
    parts.append(
        f"-- {jc.name} ({jc.state}) — {cr.subtype}\n"
        f"-- confidence={cr.confidence:.2f}, rule={cr.rule}\n"
        f"-- citation: {cr.citation}\n"
    )

    # Agency INSERT (only if no existing agency row)
    if sc.decision == "no_agency":
        parts.append(
            f"INSERT INTO public.agency (name, type)\n"
            f"SELECT {esc(agency_name)}, {esc(agency_type)}\n"
            f"WHERE NOT EXISTS (\n"
            f"  SELECT 1 FROM public.agency\n"
            f"   WHERE name = {esc(agency_name)} AND type = {esc(agency_type)}\n"
            f");\n"
        )

    # policy_source INSERT
    parts.append(
        f"INSERT INTO public.policy_source\n"
        f"  (subtype, citation, issuing_agency_id, scope, source_url, full_text, penalty_summary)\n"
        f"SELECT {esc(cr.subtype)},\n"
        f"       {esc(cr.citation)},\n"
        f"       (SELECT id FROM public.agency\n"
        f"         WHERE name = {esc(agency_name)} AND type = {esc(agency_type)}),\n"
        f"       ARRAY['dog_policy']::text[],\n"
        f"       {esc(cr.source_url)},\n"
        f"       {esc(full_text_with_footer)},\n"
        f"       {esc(cr.penalty_summary)}\n"
        f"WHERE NOT EXISTS (\n"
        f"  SELECT 1 FROM public.policy_source\n"
        f"   WHERE citation LIKE {esc(citation_prefix + '%')}\n"
        f");\n"
    )

    # beach_policy_source INSERT (critical fix 2026-05-18 — auto-emitter was
    # producing ORPHANED ps rows w/ no beach links → no cascade → zero coverage
    # gain. The hand-built OAR migration this morning had bps inserts because
    # I knew tenet #5; the auto-emitter didn't.)
    #
    # Spatial-join pattern: attach the rule to all active beaches whose geom
    # intersects the jurisdiction's polygon (city → jurisdictions table, county
    # → counties table, etc.). region_name defaults to NULL; curator can
    # supersede per-zone with a separate migration.
    if jc.polygon_table and jc.polygon_key is not None:
        polygon_key_column = "id" if jc.polygon_table != "counties" else "geoid"
        evidence = (cr.evidence_quote or cr.full_text or "")[:600]
        parts.append(
            f"-- beach_policy_source: spatial-join attach to all beaches in jurisdiction polygon\n"
            f"INSERT INTO public.beach_policy_source\n"
            f"  (beach_fid, policy_source_id, section, rule, operative_status,\n"
            f"   evidence_verbatim, evidence_url, region_name, extracted_at, last_verified)\n"
            f"SELECT b.fid, ps.id, 'sand', {esc(cr.rule)},\n"
            f"       'operative'::operative_status,\n"
            f"       {esc(evidence)},\n"
            f"       ps.source_url, NULL, NOW(), NOW()\n"
            f"FROM public.beaches_gold b\n"
            f"CROSS JOIN public.policy_source ps\n"
            f"JOIN public.{jc.polygon_table} p ON ST_Intersects(b.geom, p.geom)\n"
            f"WHERE ps.citation LIKE {esc(citation_prefix + '%')}\n"
            f"  AND b.is_active AND b.state = {esc(jc.state)}\n"
            f"  AND p.{polygon_key_column} = {esc(str(jc.polygon_key))}\n"
            f"ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__')), rule) DO NOTHING;\n"
        )
    else:
        parts.append(
            f"-- WARNING: no polygon for {jc.name}; bps rows NOT auto-generated.\n"
            f"-- Curator must hand-write beach_policy_source INSERT for this jurisdiction.\n"
        )
    return "\n".join(parts) + "\n"


# ─── #12 — URL-gate review queue helper ─────────────────────────────────
# Wraps a successful-but-URL-gate-failed (or borderline-confidence) INSERT
# block with a curator-review header. The main() emitter writes these to a
# separate _REVIEW.sql file wrapped in BEGIN/ROLLBACK so accidental apply
# is safe.

def _wrap_review_sql_block(insert_sql: str, *,
                            jurisdiction: str, state: str, reason: str) -> str:
    """Annotate one INSERT block for the curator-review queue. Caller (main())
    wraps the full set of blocks in a single BEGIN/ROLLBACK transaction."""
    header = (
        f"-- ──── REVIEW: {jurisdiction}, {state} ────\n"
        f"-- Reason: {reason}\n"
        f"-- Curator action: verify URL deep-link discipline + citation accuracy,\n"
        f"--   then either (a) approve as-is by changing the file's ROLLBACK to\n"
        f"--   COMMIT, or (b) replace source_url + re-run with --manual-url to\n"
        f"--   regenerate a clean auto-commit migration.\n"
        f"--\n"
    )
    return header + insert_sql


# ─── Stubborn-tracker / per-jurisdiction outcome ───────────────────────

# Outcome enum for the JSONL log. Per the diminishing-returns analysis:
# ~88% should land in success_auto_commit; the rest are deferred for one
# of these reasons.
OUTCOME_SUCCESS_AUTO_COMMIT    = "success_auto_commit"     # conf >= 0.7
OUTCOME_SUCCESS_HUMAN_REVIEW   = "success_human_review"    # 0.4 <= conf < 0.7
OUTCOME_DEFER_STUBBORN         = "defer_stubborn"          # tried all paths, < 0.4
OUTCOME_DEFER_TRIBAL           = "defer_tribal"
OUTCOME_DEFER_FEDERAL_BRANCH   = "defer_federal_branch"
OUTCOME_DEFER_CDP_COVERED      = "defer_cdp_covered_by_parent"
OUTCOME_DEFER_UNKNOWN_CLASS    = "defer_unknown_class"
OUTCOME_DEFER_STATE_BASELINE   = "defer_state_baseline_covers"  # state-level ps already covers all beaches
OUTCOME_ERROR                  = "error"


@dataclass
class JurisdictionOutcome:
    """Persisted per-jurisdiction record. Written to JSONL log for the
    stubborn re-pass workflow + dashboarding."""
    name:            str
    state:           str
    outcome:         str
    governance_class: str | None
    polygon_table:    str | None
    polygon_key:      str | None
    agency_id:        int | None
    decision:         str | None       # create_new | supplement_existing | no_agency
    attempts:         list[str]        # ['step_2', 'step_3', 'step_4', 'step_6', 'step_6_5', 'step_6_7']
    platform_chosen:  str | None
    rule:             str | None
    subtype:          str | None
    citation:         str | None
    source_url:       str | None
    confidence:       float | None
    notes:            str | None
    sql_emitted:      bool
    timestamp_iso:    str


# ─── Per-jurisdiction orchestration ────────────────────────────────────

def _make_outcome(name: str, state: str, jc, sc, attempts: list, cr, platform_used,
                  outcome: str, notes: str | None, sql_emitted: bool) -> JurisdictionOutcome:
    """Build a JurisdictionOutcome record from the per-step state."""
    import datetime
    return JurisdictionOutcome(
        name=name, state=state, outcome=outcome,
        governance_class=jc.governance_class if jc else None,
        polygon_table=jc.polygon_table if jc else None,
        polygon_key=jc.polygon_key if jc else None,
        agency_id=sc.agency_id if sc else None,
        decision=sc.decision if sc else None,
        attempts=attempts,
        platform_chosen=platform_used,
        rule=cr.rule if cr else None,
        subtype=cr.subtype if cr else None,
        citation=cr.citation if cr else None,
        source_url=cr.source_url if cr else None,
        confidence=cr.confidence if cr else None,
        notes=notes,
        sql_emitted=sql_emitted,
        timestamp_iso=datetime.datetime.now().isoformat(timespec="seconds"),
    )


# ─── Manual URL override (Franz-curated mode) ─────────────────────────

def _detect_platform_from_url(url: str) -> str | None:
    """Map a URL host to the platform name (drives selector + fetch mode)."""
    lc = url.lower()
    if "library.municode.com" in lc: return "municode"
    if "codepublishing.com" in lc:   return "codepublishing"
    if "ecode360.com" in lc:         return "ecode360"
    if "codelibrary.amlegal.com" in lc or "amlegal.com" in lc: return "amlegal"
    if "qcode.us" in lc or ".qcode." in lc: return "qcode"
    if ".municipal.codes" in lc:     return "codepublishing"  # similar shape
    if "ecfr.gov" in lc or "federalregister.gov" in lc: return None
    return None  # unknown — _fetch_text defaults to urllib + full-page


def codify_from_manual_url(jc: JurisdictionClassification,
                           sc: ScopeCheck,
                           url: str) -> CodifiedRule | None:
    """Bypass Steps 2/3/4 — fetch the given URL directly, pass to Step 6.

    Use case: user (Franz) provides the deep-link URL when our discovery
    heuristics failed. Common for big cities whose codes live in
    non-obvious titles (Seattle Title 18 Parks vs Title 6 Animals).

    Uses FULL-PAGE text extraction (no selector) — regex can't balance
    nested tags, and Municode's .codes-chunks-pg div spans the entire
    chapter. Full-page gives the LLM enough to find the operative
    section identified by the URL fragment."""
    platform = _detect_platform_from_url(url)
    import re

    # PDF route — applies to ANY URL ending in .pdf or matching civicengage
    # DocumentCenter pattern. Playwright can't render PDFs; use pdfplumber.
    if _is_pdf_url(url):
        text, why = _fetch_pdf_text(url, timeout=45)
        if text is None:
            print(f"      [manual] PDF fetch failed: {why}")
            return None
        platform = platform or "pdf"
    else:
        # Manual URLs are one-offs; always use Playwright. Handles Cloudflare
        # protection (municipal.codes, codepublishing-w-Cloudflare) + JS-rendered
        # platforms (Municode, ecode360) uniformly. Per-platform wait_seconds
        # if available, else default 8s.
        if not PLAYWRIGHT_AVAILABLE:
            print(f"      [manual] Playwright unavailable")
            return None
        cfg = PLATFORM_FETCH_CONFIG.get(platform or "", {"wait_seconds": 8.0})
        wait_s = cfg.get("wait_seconds", 8.0) or 8.0
        try:
            raw_html = playwright_fetch(url, selector=None, raw_html=True,
                                        wait_seconds=wait_s, timeout_ms=45000)
            text = re.sub(r"<script[^>]*>.*?</script>", " ", raw_html, flags=re.DOTALL)
            text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL)
            text = re.sub(r"<[^>]+>", " ", text)
            text = re.sub(r"\s+", " ", text).strip()
        except Exception as e:
            print(f"      [manual] Playwright fetch failed: {e}")
            return None

    # Detect Cloudflare challenge / thin fetch — trigger web_search retry
    cf_signal = any(s in (text or "")[:2000].lower() for s in [
        "performing security verification",
        "checking your browser",
        "cloudflare",
        "just a moment",
        "ray id",
    ])
    thin_or_blocked = (not text) or (len(text) < 500) or cf_signal

    if thin_or_blocked:
        # Web_search bypass — pass the URL hint + a minimal blob to Sonnet
        # with web_search enabled. Sonnet routes around the block.
        print(f"      [manual] fetch thin/blocked ({len(text or '')} chars, cf={cf_signal}) — escalating to web_search")
        fake_fetch = VerbatimFetch(
            fetched=[FetchedChapter(
                url=url,
                link_text=f"{jc.name} (manual URL — fetch blocked)",
                text_excerpt=(text or "")[:500] + " [BLOCKED — use web_search]",
                text_length=len(text or ""),
                has_dog_terms=False,
                drilled=[],
            )],
            fetch_mode="manual_url_blocked",
            notes="manual URL fetch was Cloudflare-blocked or thin; escalated to web_search",
        )
        fake_platform = PlatformDiscovery(
            valid_url=url, platform=platform or "manual_url",
            tried=[], notes="manual URL — fetch blocked",
        )
        return step_6_decide_rule(jc, fake_platform, fake_fetch, enable_web_search=True)

    fake_fetch = VerbatimFetch(
        fetched=[FetchedChapter(
            url=url,
            link_text=f"{jc.name} (manual URL)",
            text_excerpt=text[:5000],
            text_length=len(text),
            has_dog_terms=_has_dog_terms(text),
            drilled=[],
        )],
        fetch_mode="manual_url",
        notes=f"manual URL override; platform={platform or 'unknown'}",
    )
    fake_platform = PlatformDiscovery(
        valid_url=url, platform=platform or "manual_url",
        tried=[], notes="manual URL override",
    )
    return step_6_decide_rule(jc, fake_platform, fake_fetch)


def process_jurisdiction(name: str, state: str,
                         emit_sql_to: list | None = None,
                         emit_sql_review_to: list | None = None,
                         discover_only: bool = False,
                         manual_url: str | None = None,
                         ignore_state_baseline: bool = False,
                         state_baseline_threshold: float = 0.9) -> JurisdictionOutcome:
    """Run the full pipeline on one jurisdiction. Returns a structured
    outcome. When emit_sql_to is a list, appends generated SQL blocks to it.

    discover_only=True early-exits after Step 2 (platform discovery only) —
    useful for two-phase per-platform agent partitioning.

    ignore_state_baseline=True disables the §1.5 state-baseline coverage check;
    use when the curator wants to force codify in a state-baseline-dominant
    regime (e.g., looking for sub-region overrides)."""
    print(f"\n=== {name} ({state}) ===")
    attempts: list[str] = []

    # Step 0 — Classify
    jc = classify_jurisdiction(name, state)
    print(f"  [0] classify → {jc.governance_class}  (polygon_table={jc.polygon_table} polygon_key={jc.polygon_key})")
    if jc.classify_notes:
        print(f"       notes: {jc.classify_notes}")
    attempts.append("step_0_classify")

    # Step 0.5 — Triage
    td = triage(jc)
    print(f"  [0.5] triage → {td.action}  ({td.reason})")
    attempts.append("step_0_5_triage")
    if td.action != "proceed":
        outcome_map = {
            "skip_covered_by_parent": OUTCOME_DEFER_CDP_COVERED,
            "defer":                  OUTCOME_DEFER_TRIBAL if jc.governance_class == "tribal" else OUTCOME_DEFER_UNKNOWN_CLASS,
            "branch_federal":         OUTCOME_DEFER_FEDERAL_BRANCH,
        }
        return _make_outcome(name, state, jc, None, attempts, None, None,
                             outcome=outcome_map.get(td.action, OUTCOME_DEFER_UNKNOWN_CLASS),
                             notes=td.reason, sql_emitted=False)

    # Step 1 — Scope check
    sc = scope_check(jc)
    print(f"  [1] scope → {sc.decision}  (agency_id={sc.agency_id} "
          f"name={sc.agency_name!r} type={sc.agency_type})")
    print(f"       existing_ps_count={sc.existing_ps_count}")
    attempts.append("step_1_scope")

    # Step 1.5 — State-baseline coverage check
    # Skip if state-level ps rows already cover ≥ threshold of this jurisdiction's
    # beaches at conf ≥ 0.7. Manual URL override bypasses this (curator intent).
    if not manual_url and not ignore_state_baseline:
        baseline = step_1_5_state_baseline_coverage(jc, threshold=state_baseline_threshold)
        attempts.append("step_1_5_state_baseline")
        if baseline.n_beaches_total > 0:
            print(f"  [1.5] state-baseline → {baseline.reason}")
            if baseline.skip_decision:
                ps_ids_repr = ",".join(str(i) for i in baseline.state_ps_ids[:5])
                print(f"  → outcome: defer_state_baseline_covers  "
                      f"(state_ps_ids=[{ps_ids_repr}{'...' if len(baseline.state_ps_ids)>5 else ''}])")
                return _make_outcome(name, state, jc, sc, attempts, None, None,
                                     outcome=OUTCOME_DEFER_STATE_BASELINE,
                                     notes=baseline.reason + " — use --ignore-state-baseline to force codify",
                                     sql_emitted=False)

    # MANUAL URL OVERRIDE — bypass Steps 2-6.7, fetch the given URL directly
    if manual_url:
        attempts.append("manual_url_override")
        print(f"  [manual] URL override → {manual_url}")
        cr = codify_from_manual_url(jc, sc, manual_url)
        if cr:
            gate_ok, gate_why = is_url_deep_enough(cr.subtype, cr.source_url or manual_url)
            conf_marker = "✅" if cr.confidence >= 0.7 else ("⚠️ review" if cr.confidence >= 0.4 else "❌ defer")
            print(f"  [6] rule → {cr.rule}  conf={cr.confidence:.2f} {conf_marker}")
            print(f"       subtype={cr.subtype}  citation={cr.citation!r}")
            print(f"       url={cr.source_url}  url_gate={'✅' if gate_ok else '❌ '+gate_why}")
            if cr.evidence_quote:
                print(f"       evidence: {cr.evidence_quote[:140]!r}")
        # Skip Steps 2-6.7 entirely; jump to outcome + SQL emit at function end
        platform_used = _detect_platform_from_url(manual_url) or "manual_url"
        sql_emitted = False
        notes_summary = None
        if cr is None or cr.rule == "no_rule_found":
            outcome = OUTCOME_DEFER_STUBBORN
            notes_summary = "manual URL fetched but LLM found no operative rule"
        elif cr.confidence >= 0.7:
            gate_ok, gate_why = is_url_deep_enough(cr.subtype, cr.source_url or manual_url)
            if gate_ok:
                outcome = OUTCOME_SUCCESS_AUTO_COMMIT
                if emit_sql_to is not None:
                    sql = emit_migration_sql(jc, sc, cr, platform_used)
                    emit_sql_to.append(sql)
                    sql_emitted = True
                    print(f"  [7] SQL emitted (auto-commit ready)")
            else:
                outcome = OUTCOME_SUCCESS_HUMAN_REVIEW
                notes_summary = f"high confidence ({cr.confidence:.2f}) but URL gate failed: {gate_why}"
                if emit_sql_review_to is not None:
                    sql = _wrap_review_sql_block(
                        emit_migration_sql(jc, sc, cr, platform_used),
                        jurisdiction=name, state=state, reason=notes_summary,
                    )
                    emit_sql_review_to.append(sql)
                    print(f"  [7] SQL emitted to REVIEW queue (URL gate failed)")
        elif cr.confidence >= 0.4:
            outcome = OUTCOME_SUCCESS_HUMAN_REVIEW
            notes_summary = f"borderline confidence ({cr.confidence:.2f}); human review"
            if emit_sql_review_to is not None and cr.subtype:
                sql = _wrap_review_sql_block(
                    emit_migration_sql(jc, sc, cr, platform_used),
                    jurisdiction=name, state=state, reason=notes_summary,
                )
                emit_sql_review_to.append(sql)
                print(f"  [7] SQL emitted to REVIEW queue (borderline confidence)")
        else:
            outcome = OUTCOME_DEFER_STUBBORN
            notes_summary = cr.notes
        print(f"  → outcome: {outcome}")
        return _make_outcome(name, state, jc, sc, attempts, cr, platform_used,
                             outcome=outcome, notes=notes_summary, sql_emitted=sql_emitted)

    # Step 2 — Platform discovery
    attempts.append("step_2_platform")
    plat = step_2_discover_platform(jc, sc)

    # Phase A early exit — discovery-only mode for two-phase per-platform
    # partitioning workflow. Caller (Phase B partitioner) consumes the JSONL
    # outcomes' platform_chosen field to dispatch per-platform workers.
    if discover_only:
        platform_used = plat.platform if plat else None
        if platform_used:
            outcome = "discover_only_platform_found"
            notes = f"platform={platform_used} url={plat.valid_url}"
        else:
            outcome = "discover_only_no_platform"
            notes = f"no validity hit after {len(plat.tried)} attempt(s); needs Step 6.7/6.8"
        print(f"  → discover_only: {outcome}")
        return _make_outcome(name, state, jc, sc, attempts, None, platform_used,
                             outcome=outcome, notes=notes, sql_emitted=False)
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
    attempts.append("step_3_chapter")
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
    attempts.append("step_4_verbatim")
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
    attempts.append("step_6_llm_decide")
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
            attempts.append("step_6_5_redrill")
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
            attempts.append("step_6_7_agency_policy")
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
            attempts.append("step_6_8_url_inference")
            print(f"  [6.8] URL-inference fallback (no agency.web_url) → trying inferred city URLs")
            cr4 = step_6_8_url_inference_fallback(jc, sc, cr)
            if cr4:
                gate4_ok, gate4_why = is_url_deep_enough(cr4.subtype, cr4.source_url)
                conf4_marker = "✅" if cr4.confidence >= 0.7 else ("⚠️ review" if cr4.confidence >= 0.4 else "❌ defer")
                print(f"  [6.8] re-decide → {cr4.rule}  conf={cr4.confidence:.2f} {conf4_marker}")
                print(f"       subtype={cr4.subtype}  citation={cr4.citation!r}")
                print(f"       url={cr4.source_url}  url_gate={'✅' if gate4_ok else '❌ '+gate4_why}")
                if cr4.evidence_quote:
                    print(f"       evidence: {cr4.evidence_quote[:140]!r}")
                cr = cr4
            else:
                print(f"  [6.8] URL-inference found nothing valid")
    else:
        print(f"  [6] rule → SKIPPED (no fetched content or no ANTHROPIC_API_KEY)")
        # If Step 6 didn't even run AND we have an agency.web_url, jump
        # straight to the agency-policy fallback. This is the common case
        # when no platform validated (Step 2 returned NONE FOUND).
        if sc.agency_web_url:
            attempts.append("step_6_7_agency_policy")
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
        else:
            # No agency.web_url known — try Step 6.8 URL-inference
            attempts.append("step_6_8_url_inference")
            print(f"  [6.8] URL-inference fallback (no agency.web_url) → trying inferred city URLs")
            cr4 = step_6_8_url_inference_fallback(jc, sc, None)
            if cr4:
                gate4_ok, gate4_why = is_url_deep_enough(cr4.subtype, cr4.source_url)
                conf4_marker = "✅" if cr4.confidence >= 0.7 else ("⚠️ review" if cr4.confidence >= 0.4 else "❌ defer")
                print(f"  [6.8] re-decide → {cr4.rule}  conf={cr4.confidence:.2f} {conf4_marker}")
                print(f"       subtype={cr4.subtype}  citation={cr4.citation!r}")
                print(f"       url={cr4.source_url}  url_gate={'✅' if gate4_ok else '❌ '+gate4_why}")
                if cr4.evidence_quote:
                    print(f"       evidence: {cr4.evidence_quote[:140]!r}")
                cr = cr4
            else:
                print(f"  [6.8] URL-inference found nothing valid")

    # Determine outcome + emit SQL if confident enough
    platform_used = plat.platform if plat else None
    sql_emitted = False
    notes_summary = None
    if cr is None or cr.rule == "no_rule_found":
        outcome = OUTCOME_DEFER_STUBBORN
        notes_summary = (cr.notes if cr else None) or "no rule decided after all attempts"
    elif cr.confidence >= 0.7:
        gate_ok, gate_why = is_url_deep_enough(cr.subtype, cr.source_url)
        if not gate_ok:
            outcome = OUTCOME_SUCCESS_HUMAN_REVIEW
            notes_summary = f"high confidence ({cr.confidence:.2f}) but URL gate failed: {gate_why}"
            if emit_sql_review_to is not None:
                sql = _wrap_review_sql_block(
                    emit_migration_sql(jc, sc, cr, platform_used),
                    jurisdiction=name, state=state, reason=notes_summary,
                )
                emit_sql_review_to.append(sql)
                print(f"  [7] SQL emitted to REVIEW queue (URL gate failed)")
        else:
            outcome = OUTCOME_SUCCESS_AUTO_COMMIT
            if emit_sql_to is not None:
                sql = emit_migration_sql(jc, sc, cr, platform_used)
                emit_sql_to.append(sql)
                sql_emitted = True
                print(f"  [7] SQL emitted (auto-commit ready)")
    elif cr.confidence >= 0.4:
        outcome = OUTCOME_SUCCESS_HUMAN_REVIEW
        notes_summary = f"borderline confidence ({cr.confidence:.2f}); human review queue"
        if emit_sql_review_to is not None and cr.subtype:
            sql = _wrap_review_sql_block(
                emit_migration_sql(jc, sc, cr, platform_used),
                jurisdiction=name, state=state, reason=notes_summary,
            )
            emit_sql_review_to.append(sql)
            print(f"  [7] SQL emitted to REVIEW queue (borderline confidence)")
    else:
        outcome = OUTCOME_DEFER_STUBBORN
        notes_summary = cr.notes or "all paths returned low confidence"

    print(f"  → outcome: {outcome}")
    return _make_outcome(name, state, jc, sc, attempts, cr, platform_used,
                         outcome=outcome, notes=notes_summary, sql_emitted=sql_emitted)


# ─── Main ──────────────────────────────────────────────────────────────

def list_state_jurisdictions(state: str, pilot: int | None = None,
                              offset: int = 0,
                              require_beach: bool = True) -> list[tuple[str, str]]:
    """List incorporated places in a state with AT LEAST ONE beach in
    their polygon — per Franz 2026-05-18. Don't waste codify cycles on
    jurisdictions with no beaches; codified rules aren't user-surfaced
    for non-beach contexts in the product.

    NOTE: dog parks are intentionally EXCLUDED from this filter (Franz
    2026-05-18: dog parks are definitionally dog-allowed + off-leash;
    codified leash rules don't add user-facing value for dog-park-only
    cities. Operator-posted dog-park-specific rules are a different
    layer, not codify's concern).

    Set require_beach=False to skip the pre-filter (returns all
    incorporated places; useful for upstream-authority codify e.g. when
    a city's code governs coastal county beaches via MOU).

    Uses psycopg2 directly because PostgREST can't easily do the spatial
    EXISTS sub-query.
    """
    import psycopg2
    pooler = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
    pp = urllib.parse.urlparse(pooler)
    conn = psycopg2.connect(
        host=pp.hostname, port=pp.port or 5432, user=pp.username,
        password=os.environ["SUPABASE_DB_PASSWORD"],
        dbname=(pp.path or "/postgres").lstrip("/"), sslmode="require",
    )
    sql = """
        SELECT j.name
        FROM public.jurisdictions j
        WHERE j.state = %s
          AND j.place_type LIKE 'C%%'
    """
    params: list = [state]
    if require_beach:
        sql += """
          AND EXISTS (
            SELECT 1 FROM public.beaches_gold g
            WHERE g.state = %s AND g.is_active
              AND ST_Intersects(g.geom, j.geom)
          )
        """
        params.append(state)
    sql += " ORDER BY j.name"
    if pilot:
        sql += " LIMIT %s"
        params.append(pilot)
    if offset:
        sql += " OFFSET %s"
        params.append(offset)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
    finally:
        conn.close()
    return [(r[0], state) for r in rows]


# ─── #1 — Federal coverage discovery helper ──────────────────────────
#
# Lists federal-managed beaches per state, grouped by managing agency +
# unit_name. Prints actionable codify commands per opportunity using the
# existing --state-agency-rule mode.
#
# Per [[wa-codify-layered-roadmap]] Layer 2 ("federal land managers = BIGGEST gap")
# + Walkthroughs #3 (Fort Funston/GGNRA) + #4 (Crown Memorial MPA).

# PAD-US mng_name → (agency_id, canonical_name, federal_baseline_cfr)
FEDERAL_AGENCY_MAP = {
    "NPS":   (307, "National Park Service",                  "36 CFR §2.15 (Pets)"),
    "FWS":   (308, "United States Fish and Wildlife Service","50 CFR Part 26 (Public Entry and Use)"),
    "USFS":  (292, "United States Forest Service",           "36 CFR Part 261 (Prohibitions)"),
    "BLM":   (288, "United States Bureau of Land Management","43 CFR Part 8360 (Visitor Services)"),
    "USACE": (311, "United States Army Corps of Engineers",  "36 CFR Part 327 (Rules and Regulations Governing Public Use)"),
    "USBR":  (289, "United States Bureau of Reclamation",    "43 CFR Part 423 (Public Conduct on BoR Facilities)"),
    "NOAA":  (None, "National Oceanic and Atmospheric Administration", "15 CFR Part 922 (NMS Regulations)"),  # agency not yet in DB
    "DOD":   (291, "United States Department of Defense",    "(installation-specific)"),
}


def list_federal_coverage(states: list[str]) -> dict:
    """Enumerate federal-managed beach coverage opportunities. Returns
    structured dict: { mng_name: { unit_name: [(fid, beach_name), ...] } }.

    Caller (run_list_federal_coverage) prints actionable codify commands.
    """
    conn = _connect_pg()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                SELECT b.state, pad.mng_name, pad.unit_name, pad.des_tp, b.fid, b.name
                FROM public.beach_relevant_pad_us pad
                JOIN public.beaches_gold b ON ST_Intersects(b.geom, pad.geom)
                WHERE b.is_active AND b.state = ANY(%s)
                  AND pad.mng_type = 'FED'
                ORDER BY b.state, pad.mng_name, pad.unit_name, b.name
            """, (list(states),))
            rows = cur.fetchall()
    finally:
        conn.close()

    coverage: dict = {}
    for state, mng_name, unit_name, des_tp, fid, beach_name in rows:
        unit = coverage.setdefault(mng_name, {}).setdefault(unit_name, {
            "state": state, "des_tp": des_tp, "beaches": [], "_seen_fids": set()
        })
        if fid not in unit["_seen_fids"]:
            unit["beaches"].append((fid, beach_name))
            unit["_seen_fids"].add(fid)
    # Strip the dedup helper before returning
    for units in coverage.values():
        for info in units.values():
            info.pop("_seen_fids", None)
    return coverage


def run_list_federal_coverage(args) -> int:
    """Subcommand: --list-federal-coverage."""
    states = [s.strip().upper() for s in args.states.split(",") if s.strip()]
    coverage = list_federal_coverage(states)

    if not coverage:
        print(f"No federal-managed beaches found in {states}.")
        return 0

    # Tally
    total_units = sum(len(units) for units in coverage.values())
    total_beaches = sum(
        len(info["beaches"])
        for units in coverage.values()
        for info in units.values()
    )
    print(f"\n=== FEDERAL COVERAGE — {','.join(states)} ===")
    print(f"  {len(coverage)} agencies × {total_units} units × {total_beaches} beaches")
    print()

    # Sort agencies by total beach count (biggest opportunity first)
    agency_totals = sorted(
        ((mng, sum(len(u["beaches"]) for u in units.values()))
         for mng, units in coverage.items()),
        key=lambda x: -x[1]
    )

    for mng, agency_total in agency_totals:
        agency_id, agency_canonical, baseline_cfr = FEDERAL_AGENCY_MAP.get(
            mng, (None, mng, "(no baseline pattern known)")
        )
        print(f"━━ {mng}  ({agency_canonical}, agency_id={agency_id}) — {agency_total} beaches")
        print(f"   Baseline CFR: {baseline_cfr}")
        if agency_id is None:
            print(f"   ⚠️  Agency not in DB; create canonical agency row first")

        units = coverage[mng]
        for unit_name in sorted(units, key=lambda u: -len(units[u]["beaches"])):
            info = units[unit_name]
            n = len(info["beaches"])
            print(f"   • {info['state']} {unit_name!r}  ({info['des_tp']}) — {n} beach{'es' if n != 1 else ''}")
            for fid, name in info["beaches"][:3]:
                print(f"        fid {fid:<12} {name}")
            if n > 3:
                print(f"        … and {n-3} more")

        print()
        # Suggested codify command (uses existing --state-agency-rule mode)
        if agency_id:
            print(f"   ↳ Per-unit codify pattern (substitute <unit-keyword> + URL + verbatim):")
            print(f'     python scripts/derive_policy_source_for_jurisdiction.py \\')
            print(f'       --state-agency-rule \\')
            print(f'       --agency-name "{agency_canonical}" --agency-type federal_department \\')
            print(f'       --states {",".join(states)} \\')
            print(f'       --pad-mng-type FED --pad-mng-name {mng} \\')
            print(f'       --pad-unit-name-ilike "%<unit-keyword>%" \\')
            print(f'       --subtype superintendents_compendium \\')
            print(f'       --citation "<unit name> Superintendent\\\'s Compendium §X.Y (Pets)" \\')
            print(f'       --rule-url "<deep-link to pets policy>" \\')
            print(f'       --rule <on_leash|off_leash|not_allowed|off_leash_voice_control> --section sand \\')
            print(f'       --evidence-verbatim "<quote>" \\')
            print(f'       --full-text-file <path-to-verbatim-text> \\')
            print(f'       --label federal_codify_<state>_<unit-slug>')
        print()

    print("=" * 60)
    print("Federal baselines already shipped 2026-05-18:")
    print("  • NPS 36 CFR §2.15 (Pets)  → 79 beaches across CA/OR/WA (ps id=1)")
    print()
    print("Per-unit overrides (Compendiums / refuge plans / FR rules) are still")
    print("manual via the --state-agency-rule commands above. Each per-unit emit")
    print("uses subtype=superintendents_compendium for NPS or agency_administrative_policy")
    print("for USFWS/USFS/BLM (per playbook §6 layered authority).")
    return 0


# ─── #8 — Rescue-mode orchestration ──────────────────────────────────
#
# Reads a prior-run outcomes JSONL, generates per-jurisdiction agent
# prompts (using the playbook's STUBBORN JURISDICTION URL DISCOVERY
# template) + a starter CSV for batch URL collection. The curator
# dispatches one Claude Code agent per prompt (parallel-safe since
# prompts are independent), collects URLs into the CSV, then re-runs
# this script with --from-csv to ship.
#
# This encapsulates today's manual agent-dispatch workflow (7 OR coastal
# counties → 7 Explore agents in parallel) into a reproducible artifact.

STUBBORN_RESCUE_PROMPT_TEMPLATE = """\
# Stubborn jurisdiction URL discovery — {name}, {state}

## Context

Codify v1 pipeline ran on this jurisdiction with outcome `{outcome}` and
notes: `{notes}`.

Phase A platform discovery already tried: Municode (catalog-checked first),
codepublishing, amlegal, qcode, ecode360, county.codes.
Step 6.8 web_search escalation already ran for stubborn defers; for
human_review outcomes the URL gate failed.

The script can re-run via `--manual-url` or `--from-csv` (with source_url
column) once a verified chapter-level deep-link URL is found.

## Your task

Use WebSearch + WebFetch to find the operative chapter-level deep-link URL
for {name}, {state}'s dog/leash/animal-control ordinance.

## Acceptable URL forms

- Deep-linked chapter on a county/city code site (anchors like #ch-6, /Chapter-6/, /Title6/)
- Page-level deep-link to specific ordinance section
- Codified deep-link to specific section
- PDF of a SPECIFIC chapter (e.g., LC07.pdf for Lane Code Chapter 7) — the
  pipeline now handles PDFs end-to-end via pdfplumber

## NOT acceptable

- Wikipedia, news articles, blog posts, .org summary sites
- Statewide ORS/RCW/etc pages (state baseline is captured separately)
- Agency homepage / sheriff's office landing / parks dept landing
- Top-of-code landing page or "all county code" PDF blob
- PDFs that just RESTATE state law (e.g., "Oregon-Dog-Laws-PDF") — those
  aren't this jurisdiction's OWN ordinance

## Verify

Open the URL via WebFetch; confirm body contains: (1) jurisdiction name +
state, (2) "leash" OR "dog" OR "animal" in operative text, (3) section/
chapter number visible (e.g., "§6.04", "Chapter 7", "Article 4"). If
Cloudflare/403, use WebSearch to verify content exists.

## Return

Append exactly ONE line to this file's CSV (`../input.csv`):

  {name},{state},<verified_chapter_url>,<one-sentence-citation>

Or if no acceptable URL found:

  {name},{state},DEFER,<why>

Do NOT dispatch scripts. Do NOT modify files outside `../input.csv`.

## Related pins

- [[script-defers-dispatch-agents]] — this rescue path is the escalation
- [[page-level-over-agency-level]] — URL discipline tenet
- [[municode-silent-redirect]] — Municode pre-check already filtered this one
- `docs/jurisdiction_policy_source_playbook.md` — STUBBORN JURISDICTION URL DISCOVERY variant
"""


def run_rescue_mode(args) -> int:
    """Subcommand for --rescue mode.
    Reads outcomes JSONL → emits per-jurisdiction agent prompts + starter CSV."""
    in_path = Path(args.from_jsonl)
    if not in_path.exists():
        print(f"ERROR: --from-jsonl path not found: {in_path}", file=sys.stderr)
        return 2

    # Parse outcomes
    eligible_outcomes = {OUTCOME_DEFER_STUBBORN}
    if args.include_review:
        eligible_outcomes.add(OUTCOME_SUCCESS_HUMAN_REVIEW)

    eligible: list[dict] = []
    with open(in_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                o = json.loads(line)
            except json.JSONDecodeError:
                continue
            if o.get("outcome") in eligible_outcomes:
                eligible.append(o)

    if not eligible:
        print(f"No eligible entries in {in_path} (outcomes filter: {sorted(eligible_outcomes)})")
        return 0

    # Output directory
    label = args.label or in_path.stem.replace("_outcomes", "")
    out_dir = Path(args.out_dir) / f"rescue_{label}"
    prompts_dir = out_dir / "prompts"
    prompts_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / "input.csv"
    readme_path = out_dir / "README.md"

    # CSV starter with source_url column
    with open(csv_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("jurisdiction,state,source_url,citation\n")
        for o in eligible:
            # Quote name if it contains comma
            n = o["name"]
            if "," in n:
                n = f'"{n}"'
            f.write(f"{n},{o['state']},,\n")

    # Per-jurisdiction prompt files
    for o in eligible:
        slug = o["name"].lower().replace(" ", "_").replace(",", "").replace("'", "")
        prompt_path = prompts_dir / f"{o['state']}_{slug}.md"
        prompt_path.write_text(
            STUBBORN_RESCUE_PROMPT_TEMPLATE.format(
                name=o["name"],
                state=o["state"],
                outcome=o["outcome"],
                notes=(o.get("notes") or "(no notes)")[:300],
            ),
            encoding="utf-8",
        )

    # README with next-step instructions
    readme = f"""\
# Rescue queue — {label}

Generated by `scripts/derive_policy_source_for_jurisdiction.py --rescue`
from `{in_path}` on {__import__("datetime").date.today().isoformat()}.

## Contents

- **prompts/**: one agent prompt per stubborn jurisdiction ({len(eligible)} files)
- **input.csv**: starter CSV with `jurisdiction, state, source_url, citation`
  columns; `source_url` blank for agents/curator to fill

## Next steps (per playbook + [[script-defers-dispatch-agents]])

1. **Dispatch one Claude Code Explore agent per prompt** (parallel-safe — each
   prompt is independent). Each agent uses WebSearch + WebFetch to find the
   chapter-level deep-link URL and appends a CSV line to `input.csv`.
2. **Review the filled CSV** for DEFER rows + any URLs that look agency-level
   (the URL gate will catch most, but spot-check before re-run).
3. **Re-run the script** in batch mode against the filled CSV:

   ```bash
   python scripts/derive_policy_source_for_jurisdiction.py \\
     --from-csv {csv_path} \\
     --out-dir tmp \\
     --label {label}_rescue_run
   ```

4. **Apply the resulting SQL** with explicit per-batch approval (see playbook §9):

   ```bash
   PGPASSWORD=... PGCLIENTENCODING=UTF8 psql "<pooler_url>" -v ON_ERROR_STOP=1 \\
     -f tmp/codify_{label}_rescue_run_*.sql
   ```

   For URL-gate-failed entries (`_REVIEW.sql`), curator must verify each block
   + change `ROLLBACK` to `COMMIT` before applying.

## Eligible-outcomes filter

This rescue queue included: {sorted(eligible_outcomes)}
(Use `--include-review` to add `success_human_review` next time.)

## Affected jurisdictions ({len(eligible)})

| Jurisdiction | State | Outcome | Notes |
|---|---|---|---|
"""
    for o in eligible:
        notes_short = (o.get("notes") or "")[:60].replace("|", " ")
        readme += f"| {o['name']} | {o['state']} | `{o['outcome']}` | {notes_short} |\n"
    readme_path.write_text(readme, encoding="utf-8")

    print(f"Rescue queue prepared: {out_dir}")
    print(f"  Prompts: {len(eligible)} files in {prompts_dir}")
    print(f"  Starter CSV: {csv_path}")
    print(f"  README: {readme_path}")
    print(f"\nNext: dispatch one Claude Code Explore agent per prompt; collect URLs into {csv_path};")
    print(f"      re-run with --from-csv to ship.")
    return 0


# ─── State-agency rule mode (#6 encapsulation 2026-05-18) ──────────────
#
# Generalizes the OPRD OAR 736-010-0030 pattern: a single state-agency rule
# that applies to all beaches managed by that agency via PAD-US spatial join.
# Replaces hand-SQL with `--state-agency-rule` CLI flag.

def _connect_pg():
    """Open a psycopg2 connection using the same pooler URL pattern as
    list_state_jurisdictions."""
    import psycopg2
    pooler = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
    pp = urllib.parse.urlparse(pooler)
    return psycopg2.connect(
        host=pp.hostname, port=pp.port or 5432, user=pp.username,
        password=os.environ["SUPABASE_DB_PASSWORD"],
        dbname=(pp.path or "/postgres").lstrip("/"), sslmode="require",
    )


def lookup_agency_id(name: str, agency_type: str) -> int | None:
    """Look up agency by canonical bare-name + type per playbook tenet #4.
    Returns None if not found (caller must error or create)."""
    conn = _connect_pg()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM public.agency WHERE name = %s AND type = %s LIMIT 1",
                (name, agency_type),
            )
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def query_pad_us_managed_beaches(
    states: list[str],
    pad_mng_type: str | None = None,
    pad_mng_name: str | None = None,
    pad_des_tp: str | None = None,
    pad_unit_name_ilike: str | None = None,
) -> list[tuple[int, str]]:
    """Find beaches managed by a state/federal agency via PAD-US spatial join.

    Filters beach_relevant_pad_us by (mng_type, mng_name, des_tp, unit_name);
    intersects with active beaches in the given states. Returns deduplicated
    [(fid, name)] list ordered by name.

    When NO PAD-US filter is provided (mng_type/mng_name/des_tp/unit_name all
    None), returns ALL active beaches in the listed states. Use for regulatory
    regimes that don't correspond to a discrete polygon — e.g., OR Ocean Shore
    State Recreation Area governed by OAR 736-021-0070, which applies across
    the entire OR coastal beach inventory.

    Per playbook step 5 federal/state pattern; today's OAR work used this
    spatial join as hand-SQL → now encapsulated as a reusable function.
    """
    has_pad_filter = any([pad_mng_type, pad_mng_name, pad_des_tp, pad_unit_name_ilike])

    if not has_pad_filter:
        # No-PAD-filter mode: all active beaches in the listed states
        sql = """
            SELECT b.fid, b.name
            FROM public.beaches_gold b
            WHERE b.state = ANY(%s) AND b.is_active
            ORDER BY b.name
        """
        params: list = [list(states)]
    else:
        sql = """
            SELECT DISTINCT b.fid, b.name
            FROM public.beach_relevant_pad_us pad
            JOIN public.beaches_gold b ON ST_Intersects(b.geom, pad.geom)
            WHERE b.state = ANY(%s) AND b.is_active
        """
        params = [list(states)]
        if pad_mng_type:
            sql += " AND pad.mng_type = %s"
            params.append(pad_mng_type)
        if pad_mng_name:
            sql += " AND pad.mng_name = %s"
            params.append(pad_mng_name)
        if pad_des_tp:
            sql += " AND pad.des_tp = %s"
            params.append(pad_des_tp)
        if pad_unit_name_ilike:
            sql += " AND pad.unit_name ILIKE %s"
            params.append(pad_unit_name_ilike)
        sql += " ORDER BY b.name"

    conn = _connect_pg()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            return [(r[0], r[1]) for r in cur.fetchall()]
    finally:
        conn.close()


def emit_state_agency_rule_migration(
    *,
    agency_id: int,
    agency_name: str,           # for migration header only
    subtype: str,
    citation: str,
    source_url: str,
    full_text: str,
    rule: str,
    section: str,
    evidence_verbatim: str,
    affected_beaches: list[tuple[int, str]],
    effective_date: str | None,
    metadata: dict | None,
    label: str,
    states: list[str],
    pad_filter_repr: str,
) -> str:
    """Build idempotent migration SQL per playbook §7 template.

    Returns the SQL text (caller writes to file). Does NOT apply.
    """
    def esc(s: str | None) -> str:
        if s is None:
            return "NULL"
        return "'" + s.replace("'", "''") + "'"

    import datetime
    today = datetime.date.today().isoformat()

    # Header
    header = [
        f"-- State-agency rule codify: {citation}",
        f"-- Generated by scripts/derive_policy_source_for_jurisdiction.py --state-agency-rule {today}",
        f"--",
        f"-- Agency: {agency_name} (id={agency_id})",
        f"-- Subtype: {subtype}",
        f"-- Rule:    {rule}  (section={section})",
        f"-- States:  {','.join(states)}",
        f"-- PAD-US filter: {pad_filter_repr}",
        f"-- Affected beaches: {len(affected_beaches)}",
        f"--",
    ]
    for fid, name in affected_beaches:
        header.append(f"--   fid {fid}  {name}")
    header.append("--")
    header.append("-- Per playbook tenet #5: bps INSERT auto-fires trigger cascade.")
    header.append("-- NO manual two-pass refire needed (data-only migration, not function-body change).")
    header.append("")

    full_text_footer = full_text
    if not full_text_footer.endswith("\n"):
        full_text_footer += "\n"
    full_text_footer += f"\n[Codified via codify v1 --state-agency-rule {today}.]"

    metadata_sql = "NULL"
    if metadata:
        metadata_sql = "jsonb_build_object(" + ", ".join(
            f"{esc(k)}, {esc(str(v))}" for k, v in metadata.items()
        ) + ")"

    eff_date_sql = f"{esc(effective_date)}::date" if effective_date else "NULL::date"

    # Build the VALUES list of beach fids
    if not affected_beaches:
        raise ValueError("No affected beaches — refusing to emit empty migration.")
    values_list = ",\n             ".join(
        f"({fid}::bigint)  /* {name.replace('*/', '*-/')} */"
        for fid, name in affected_beaches
    )

    body = f"""BEGIN;

-- 1. policy_source insert (idempotent via source_url uniqueness check)
INSERT INTO public.policy_source (
    subtype, citation, issuing_agency_id, scope, source_url, full_text,
    effective_date, last_verified, metadata
)
SELECT {esc(subtype)},
       {esc(citation)},
       {agency_id},
       ARRAY['dog_policy']::text[],
       {esc(source_url)},
       {esc(full_text_footer)},
       {eff_date_sql},
       NOW(),
       {metadata_sql}
WHERE NOT EXISTS (
    SELECT 1 FROM public.policy_source
    WHERE source_url = {esc(source_url)}
);

-- 2. beach_policy_source links (idempotent via ON CONFLICT)
INSERT INTO public.beach_policy_source (
    beach_fid, policy_source_id, section, rule, operative_status,
    evidence_verbatim, evidence_url, extracted_at, last_verified
)
SELECT v.fid, ps.id, {esc(section)}, {esc(rule)},
       'operative'::operative_status,
       {esc(evidence_verbatim)},
       ps.source_url, NOW(), NOW()
FROM (VALUES {values_list}) AS v(fid)
CROSS JOIN public.policy_source ps
WHERE ps.source_url = {esc(source_url)}
ON CONFLICT (beach_fid, policy_source_id, section, (COALESCE(region_name, '__default__')), rule) DO NOTHING;

COMMIT;
"""
    return "\n".join(header) + body


def run_state_agency_rule(args) -> int:
    """Subcommand for --state-agency-rule mode.
    Emits one migration SQL file. Does NOT apply (per playbook §9)."""
    # 1) Validate agency exists
    agency_id = lookup_agency_id(args.agency_name, args.agency_type)
    if agency_id is None:
        print(f"ERROR: no agency found for name={args.agency_name!r} type={args.agency_type!r}",
              file=sys.stderr)
        print(f"  Verify via: SELECT id, name, type FROM public.agency WHERE name ILIKE '%{args.agency_name}%';",
              file=sys.stderr)
        return 3
    print(f"agency_id={agency_id}  name={args.agency_name!r}  type={args.agency_type!r}")

    # 2) Spatial query for affected beaches
    states = [s.strip().upper() for s in args.states.split(",") if s.strip()]
    beaches = query_pad_us_managed_beaches(
        states=states,
        pad_mng_type=args.pad_mng_type,
        pad_mng_name=args.pad_mng_name,
        pad_des_tp=args.pad_des_tp,
        pad_unit_name_ilike=args.pad_unit_name_ilike,
    )
    pad_filter_parts: list[str] = []
    if args.pad_mng_type:
        pad_filter_parts.append(f"mng_type={args.pad_mng_type}")
    if args.pad_mng_name:
        pad_filter_parts.append(f"mng_name={args.pad_mng_name}")
    if args.pad_des_tp:
        pad_filter_parts.append(f"des_tp={args.pad_des_tp}")
    if args.pad_unit_name_ilike:
        pad_filter_parts.append(f"unit_name~{args.pad_unit_name_ilike!r}")
    pad_filter_repr = " ".join(pad_filter_parts) if pad_filter_parts else "(none — all active beaches in listed states)"

    print(f"PAD-US filter: {pad_filter_repr}")
    print(f"Affected beaches: {len(beaches)}")
    for fid, name in beaches:
        print(f"  fid {fid:<12}  {name}")
    if not beaches:
        print("ERROR: zero beaches matched the PAD-US filter. Refusing to emit empty migration.",
              file=sys.stderr)
        return 4

    # 3) Read full_text from file (long verbatim doesn't fit on CLI)
    if not args.full_text_file:
        print("ERROR: --full-text-file required (path to verbatim quote of the rule)",
              file=sys.stderr)
        return 5
    full_text = Path(args.full_text_file).read_text(encoding="utf-8")

    # 4) Optional metadata kv pairs
    metadata = {}
    if args.metadata_kv:
        for kv in args.metadata_kv:
            if "=" in kv:
                k, _, v = kv.partition("=")
                metadata[k.strip()] = v.strip()

    # 5) Emit migration SQL
    sql = emit_state_agency_rule_migration(
        agency_id=agency_id,
        agency_name=args.agency_name,
        subtype=args.subtype,
        citation=args.citation,
        source_url=args.rule_url,
        full_text=full_text,
        rule=args.rule,
        section=args.section,
        evidence_verbatim=args.evidence_verbatim,
        affected_beaches=beaches,
        effective_date=args.effective_date,
        metadata=metadata or None,
        label=args.label or "state_agency_rule",
        states=states,
        pad_filter_repr=pad_filter_repr,
    )

    # 6) Write to supabase/migrations/ per playbook §7
    import datetime
    ts = datetime.date.today().strftime("%Y%m%d")
    out_path = ROOT / "supabase" / "migrations" / f"{ts}_{args.label or 'state_agency_rule'}.sql"
    if out_path.exists() and not args.overwrite:
        print(f"ERROR: {out_path} already exists. Use --overwrite to replace.",
              file=sys.stderr)
        return 6
    out_path.write_text(sql, encoding="utf-8")
    print(f"\nMigration written: {out_path}")
    print("\nNext steps (per playbook §9):")
    print(f"  1. Review the SQL: cat {out_path}")
    print(f"  2. Run quality gates (§8) — read-only audits")
    print(f"  3. git add + commit the migration")
    print(f"  4. Apply via psql ONLY with explicit per-batch approval")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Codify v1 — derive policy_source per jurisdiction")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--jurisdiction", help="Single jurisdiction name (e.g. 'Skagit County')")
    g.add_argument("--state", help="Process all jurisdictions in this state")
    g.add_argument("--from-csv", help="CSV with jurisdiction,state columns")
    g.add_argument("--state-agency-rule", action="store_true",
                   help="Mode: codify ONE state-agency rule that applies to many "
                        "beaches via PAD-US spatial join (e.g. OAR 736-010-0030 → "
                        "OPRD-managed beaches). Requires --agency-name, --agency-type, "
                        "--states, --pad-mng-type, --subtype, --citation, --rule-url, "
                        "--rule, --evidence-verbatim, --full-text-file, --label. "
                        "Emits supabase/migrations/<date>_<label>.sql; does NOT apply.")
    g.add_argument("--rescue", action="store_true",
                   help="Mode: read an outcomes JSONL from a prior pipeline run + "
                        "emit per-jurisdiction agent prompts + starter CSV for the "
                        "stubborn-rescue workflow. Requires --from-jsonl. Optional "
                        "--include-review adds success_human_review entries.")
    g.add_argument("--list-federal-coverage", action="store_true",
                   help="Mode: enumerate federal-managed beach coverage opportunities "
                        "per state. Lists each (agency, unit, beaches) triple + suggests "
                        "actionable --state-agency-rule commands for per-unit codify. "
                        "Requires --states.")

    # rescue mode arguments
    rg = ap.add_argument_group("rescue mode arguments")
    rg.add_argument("--from-jsonl", help="Path to prior-run outcomes JSONL")
    rg.add_argument("--include-review", action="store_true",
                    help="Include success_human_review outcomes (URL-gate-failed) "
                         "in the rescue queue (default: defer_stubborn only)")

    # state-agency-rule mode arguments (only used with --state-agency-rule)
    ag = ap.add_argument_group("state-agency-rule mode arguments")
    ag.add_argument("--agency-name", help="Agency canonical name (e.g. 'Oregon Parks and Recreation Department')")
    ag.add_argument("--agency-type", default="state_department",
                    help="Agency type (default: state_department)")
    ag.add_argument("--states", help="Comma-separated state codes to search for affected beaches (e.g. OR,WA)")
    ag.add_argument("--pad-mng-type", help="PAD-US mng_type filter (e.g. STAT, FED, LOC)")
    ag.add_argument("--pad-mng-name", help="PAD-US mng_name filter (e.g. SPR for State Parks Recreation)")
    ag.add_argument("--pad-des-tp", help="PAD-US des_tp filter (e.g. SP for State Park)")
    ag.add_argument("--pad-unit-name-ilike", help="PAD-US unit_name ILIKE filter (SQL pattern)")
    ag.add_argument("--subtype", help="policy_source subtype (e.g. state_regulation)")
    ag.add_argument("--citation", help="Canonical citation string")
    ag.add_argument("--rule-url", help="Deep-linked source URL")
    ag.add_argument("--rule", choices=["on_leash", "off_leash", "off_leash_voice_control",
                                       "not_allowed", "no_rule_found"],
                    help="Operative rule per beach")
    ag.add_argument("--section", default="sand", help="bps section (default: sand)")
    ag.add_argument("--evidence-verbatim", help="Per-beach evidence quote")
    ag.add_argument("--full-text-file", help="Path to file with verbatim rule text (full_text)")
    ag.add_argument("--effective-date", help="Rule effective date YYYY-MM-DD (optional)")
    ag.add_argument("--metadata-kv", action="append",
                    help="Optional metadata pairs key=value (repeatable)")
    ag.add_argument("--overwrite", action="store_true",
                    help="Overwrite the migration file if it already exists")
    ap.add_argument("--state-of", dest="state_of",
                    help="State (with --jurisdiction). e.g. WA")
    ap.add_argument("--pilot", type=int, help="Cap to first N jurisdictions (with --state)")
    ap.add_argument("--out-dir", default="tmp", help="Output directory for SQL + JSONL log")
    ap.add_argument("--label", help="Optional label for output filenames (e.g. 'wa_pilot_5')")
    ap.add_argument("--discover-only", action="store_true",
                    help="Phase A: run Steps 0-2 only; output platform_chosen per jurisdiction "
                         "for downstream per-platform partitioning. No LLM cost.")
    ap.add_argument("--only-platform", choices=list(PLATFORM_BUILDERS.keys()),
                    help="Phase C: restrict Step 2 to ONE platform (override "
                         "STATE_PLATFORM_PRIORITY). For per-platform parallel workers.")
    ap.add_argument("--include-no-beach", action="store_true",
                    help="Disable beach pre-filter (default skips jurisdictions "
                         "with NO beaches in their polygon).")
    ap.add_argument("--manual-url",
                    help="Single-jurisdiction manual-URL override (with --jurisdiction). "
                         "Bypasses Steps 2-6.7; fetches the URL directly + runs Step 6 LLM. "
                         "For batch mode use --from-csv with a source_url column.")
    ap.add_argument("--ignore-state-baseline", action="store_true",
                    help="Disable Step 1.5 state-baseline coverage check. By default, "
                         "if state-level ps rows already cover ≥ threshold fraction of "
                         "this jurisdiction's beaches at conf ≥ 0.7, codify defers "
                         "with defer_state_baseline_covers outcome. Set this flag to "
                         "force codify anyway (e.g., hunting for sub-region overrides).")
    ap.add_argument("--state-baseline-threshold", type=float, default=0.9,
                    help="Step 1.5 coverage threshold (default 0.9 = 90 percent of "
                         "jurisdiction beaches must already be state-covered to defer).")
    ap.add_argument("--no-municode-precheck", action="store_true",
                    help="Disable Municode state-catalog pre-check (Step 2 will always "
                         "try Municode URLs even for jurisdictions known to be absent). "
                         "For debugging.")
    args = ap.parse_args()

    # ── --rescue mode: separate code path, exits early ──
    if args.rescue:
        if not args.from_jsonl:
            print("ERROR: --rescue requires --from-jsonl <path>", file=sys.stderr)
            return 2
        return run_rescue_mode(args)

    # ── --list-federal-coverage mode: separate code path, exits early ──
    if args.list_federal_coverage:
        if not args.states:
            print("ERROR: --list-federal-coverage requires --states <code,code>", file=sys.stderr)
            return 2
        return run_list_federal_coverage(args)

    # ── --state-agency-rule mode: separate code path, exits early ──
    if args.state_agency_rule:
        # pad_mng_type now OPTIONAL — see query_pad_us_managed_beaches no-filter mode
        required = ("agency_name", "states", "subtype",
                    "citation", "rule_url", "rule", "evidence_verbatim",
                    "full_text_file", "label")
        missing = [k for k in required if not getattr(args, k)]
        if missing:
            print(f"ERROR: --state-agency-rule mode requires: {', '.join('--'+m.replace('_','-') for m in missing)}",
                  file=sys.stderr)
            return 2
        return run_state_agency_rule(args)

    # Build the list of (name, state) tuples to process
    queue: list[tuple[str, str]] = []
    if args.jurisdiction:
        if not args.state_of:
            print("ERROR: --jurisdiction requires --state-of", file=sys.stderr)
            return 2
        queue.append((args.jurisdiction, args.state_of, args.manual_url))
    elif args.state:
        queue = [(n, s, None) for n, s in list_state_jurisdictions(
                  args.state, pilot=args.pilot,
                  require_beach=not args.include_no_beach)]
    elif args.from_csv:
        with open(args.from_csv, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                # Optional source_url column triggers manual-URL override
                url = (row.get("source_url") or "").strip() or None
                queue.append((row["jurisdiction"], row["state"], url))
    # Normalize queue to 3-tuples (name, state, manual_url|None)
    queue = [(t[0], t[1], t[2] if len(t) > 2 else None) for t in queue]

    if not queue:
        print("No jurisdictions to process.", file=sys.stderr)
        return 0

    print(f"Queue: {len(queue)} jurisdiction(s)")

    # Output paths
    import datetime
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    label = args.label or (args.state.lower() if args.state else "single")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    sql_path   = out_dir / f"codify_{label}_{ts}.sql"
    jsonl_path = out_dir / f"codify_{label}_{ts}_outcomes.jsonl"

    outcomes: list[JurisdictionOutcome] = []
    sql_blocks: list[str] = []
    sql_review_blocks: list[str] = []   # #12: human_review queue (URL gate fails + borderline conf)
    tally: dict[str, int] = {}

    # Municode pre-check toggle (must mutate before any process_jurisdiction call)
    if args.no_municode_precheck:
        global _MUNICODE_PRECHECK_DISABLED
        _MUNICODE_PRECHECK_DISABLED = True
        print("MUNICODE pre-check DISABLED (--no-municode-precheck)")

    # Phase C: override STATE_PLATFORM_PRIORITY for per-platform workers
    if args.only_platform:
        # Mutate the in-memory priority dict so step_2 only tries one platform
        for k in list(STATE_PLATFORM_PRIORITY.keys()):
            STATE_PLATFORM_PRIORITY[k] = [args.only_platform]
        print(f"PHASE C: --only-platform={args.only_platform} (overriding priority for all states)")

    for name, state, manual_url in queue:
        try:
            o = process_jurisdiction(name, state, emit_sql_to=sql_blocks,
                                     emit_sql_review_to=sql_review_blocks,
                                     discover_only=args.discover_only,
                                     manual_url=manual_url,
                                     ignore_state_baseline=args.ignore_state_baseline,
                                     state_baseline_threshold=args.state_baseline_threshold)
            outcomes.append(o)
            tally[o.outcome] = tally.get(o.outcome, 0) + 1
        except Exception as e:
            print(f"  ERROR on {name}: {e}", file=sys.stderr)
            err_o = JurisdictionOutcome(
                name=name, state=state, outcome=OUTCOME_ERROR,
                governance_class=None, polygon_table=None, polygon_key=None,
                agency_id=None, decision=None, attempts=[],
                platform_chosen=None, rule=None, subtype=None, citation=None,
                source_url=None, confidence=None, notes=str(e),
                sql_emitted=False,
                timestamp_iso=datetime.datetime.now().isoformat(timespec="seconds"),
            )
            outcomes.append(err_o)
            tally[OUTCOME_ERROR] = tally.get(OUTCOME_ERROR, 0) + 1
        time.sleep(0.15)

    # Write SQL (only the high-confidence rows that emitted)
    if sql_blocks:
        with open(sql_path, "w", encoding="utf-8") as f:
            f.write(f"-- Codify v1 emit — label={label} ts={ts}\n")
            f.write(f"-- {len(sql_blocks)} jurisdiction(s) auto-committed; review before applying.\n")
            f.write("BEGIN;\n\n")
            for block in sql_blocks:
                f.write(block)
                f.write("\n")
            f.write("COMMIT;\n")
        print(f"\nSQL written: {sql_path}  ({len(sql_blocks)} block(s))")

        # #2: post-apply companion — runs the temporal extractor on the new ps
        # rows after the curator applies the migration. Per playbook §9.5:
        # "Extract temporal patterns from NEW ps rows" — was orphaned step
        # because the auto-emitter didn't surface the ps_ids.
        post_apply_path = out_dir / f"codify_{label}_{ts}_post_apply.sh"
        # Collect distinct source_urls from outcomes for the SQL-based ps_id lookup
        committed_urls = sorted({
            o.source_url for o in outcomes
            if o.outcome == OUTCOME_SUCCESS_AUTO_COMMIT and o.source_url
        })
        url_list_quoted = ",\n    ".join(f"'{u}'" for u in committed_urls)
        # Build a python URL list for embedding into the post_apply script
        py_url_list = ",\n        ".join(repr(u) for u in committed_urls)
        post_apply = f"""#!/usr/bin/env bash
# Codify v1 post-apply — auto-generated alongside {sql_path.name}
# Per playbook §9.5 ([[two-pass-refire-pattern]] does NOT apply for bps INSERTs;
# the trigger cascade is automatic — but the temporal extractor IS NOT.).
#
# Usage: after applying the migration, run this script to extract temporal
# patterns (seasonal carve-outs, daily windows) from the newly-committed ps rows.
#
#   bash {post_apply_path.name}
#
# Prereqs: ANTHROPIC_API_KEY + SUPABASE_DB_PASSWORD in scripts/pipeline/.env
# (python-dotenv loads them — no bash env-source step needed, which is
# brittle when env values contain |, &, (), etc.)
# Cost: ~$0.005-0.01 per ps row via Sonnet 4.5
set -euo pipefail

cd "$(dirname "$0")/.."   # script lives in tmp/; .. → repo root

# Resolve new ps_ids via python (uses python-dotenv to load env cleanly)
NEW_PS_IDS=$(python -c "
import sys, os, urllib.parse
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
load_dotenv(Path('scripts/pipeline/.env'))
pooler = Path('supabase/.temp/pooler-url').read_text().strip()
pp = urllib.parse.urlparse(pooler)
conn = psycopg2.connect(host=pp.hostname, port=pp.port or 5432, user=pp.username,
                       password=os.environ['SUPABASE_DB_PASSWORD'],
                       dbname=(pp.path or '/postgres').lstrip('/'), sslmode='require')
urls = [
        {py_url_list}
]
with conn, conn.cursor() as cur:
    cur.execute('SELECT id FROM public.policy_source WHERE source_url = ANY(%s)', (urls,))
    print(','.join(str(r[0]) for r in cur.fetchall()))
")

if [ -z "$NEW_PS_IDS" ]; then
  echo "No ps rows found matching the {len(committed_urls)} source_urls from this run."
  echo "Did you apply {sql_path.name} yet? If not, apply it first via psql."
  exit 1
fi

echo "Running temporal extractor on ps_ids: $NEW_PS_IDS"
python scripts/extract_temporal_from_policy_source.py --ps-ids "$NEW_PS_IDS"
"""
        post_apply_path.write_text(post_apply, encoding="utf-8")
        # chmod +x where supported (no-op on Windows)
        try:
            import stat as _stat
            post_apply_path.chmod(post_apply_path.stat().st_mode | _stat.S_IEXEC | _stat.S_IXGRP | _stat.S_IXOTH)
        except Exception:
            pass
        print(f"Post-apply script: {post_apply_path}  (extracts temporal patterns from new ps rows)")

    # #12: write REVIEW SQL (URL-gate-failed + borderline-confidence cases)
    # Wrapped in BEGIN/ROLLBACK so accidental apply is safe — curator changes
    # to COMMIT after verifying each entry.
    if sql_review_blocks:
        review_path = out_dir / f"codify_{label}_{ts}_REVIEW.sql"
        with open(review_path, "w", encoding="utf-8") as f:
            f.write(f"-- ───────────────────────────────────────────────────────────────\n")
            f.write(f"-- CURATOR REVIEW REQUIRED — Codify v1 emit — label={label} ts={ts}\n")
            f.write(f"-- ───────────────────────────────────────────────────────────────\n")
            f.write(f"-- {len(sql_review_blocks)} jurisdiction(s) reached conf >= 0.4 but did NOT meet\n")
            f.write(f"-- auto-commit criteria (URL gate failed OR borderline confidence).\n")
            f.write(f"--\n")
            f.write(f"-- This file is wrapped in BEGIN/ROLLBACK so accidental apply is\n")
            f.write(f"-- safe. After verifying each entry, change ROLLBACK to COMMIT and\n")
            f.write(f"-- apply via psql.\n")
            f.write(f"--\n")
            f.write(f"-- Alternative: replace source_url with a deep-link + re-run with\n")
            f.write(f"-- --manual-url to regenerate a clean auto-commit migration.\n")
            f.write(f"--\n")
            f.write("BEGIN;\n\n")
            for block in sql_review_blocks:
                f.write(block)
                f.write("\n")
            f.write("ROLLBACK;  -- ← change to COMMIT after curator review\n")
        print(f"REVIEW SQL written: {review_path}  ({len(sql_review_blocks)} block(s); BEGIN/ROLLBACK wrapped)")

    # Write JSONL outcomes (always, for stubborn-tracker)
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for o in outcomes:
            f.write(json.dumps(asdict(o), ensure_ascii=False) + "\n")
    print(f"Outcomes log: {jsonl_path}  ({len(outcomes)} record(s))")

    print("\n=== OUTCOMES ===")
    for k in sorted(tally.keys()):
        print(f"  {k:<32} {tally[k]}")
    print(f"  total processed:                 {len(outcomes)}")
    # Surface the stubborn count prominently for re-pass workflow
    stubborn_n = tally.get(OUTCOME_DEFER_STUBBORN, 0)
    if stubborn_n:
        print(f"\n  STUBBORN: {stubborn_n} jurisdiction(s) need Step 6.8 re-pass "
              f"(see [[codify-step-6-8-web-search-fallback]])")
    return 0


if __name__ == "__main__":
    sys.exit(main())
