"""
County code platform discovery — Phase A of the all-CA-counties sweep.

For each CA county that has beaches in beaches_gold:
1. Try several Municode URL patterns
2. Identify whether the county is on Municode and which doc slug works
3. (Future) try ecode360 / other platforms for un-found counties
4. Output a CSV: county, beach_count, platform, doc_slug, dog_chapter_hint

Idempotent: appends to scripts/one_off/county_code_discovery_status.csv;
skips counties already discovered.

Honest scope: this only DISCOVERS. The per-county dog-policy capture
(Phase B/C/D) is a separate per-county effort that consumes this CSV.
"""
from __future__ import annotations

import csv
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(__file__) + "/../fetch")
import fetch_html  # type: ignore
import fetch_municode  # type: ignore

STATUS_CSV = os.path.join(os.path.dirname(__file__), "county_code_discovery_status.csv")

# Municode jurisdiction-slug variations to try for each county.
# Pattern: ca/<county_slug>_county or ca/<county_slug>-county
MUNICODE_DOC_SLUGS = ["code_of_ordinances", "municipal_code", "code", "ordinance_code"]

# amlegal jurisdiction-slug variations to try for each county.
# Pattern: codelibrary.amlegal.com/codes/<slug>/
# Precedents (2026-05-16):
#   - SD County:  `san_diego`           (snake, no suffix)
#   - SB County:  `sanbernardino`       (concat, no suffix) — Franz pointer
#   - LA City:    `los_angeles`         (snake)
#   - Reedley:    `reedleyca`           (concat + `ca`)
# Try bare-snake first (SD/LA precedent), then concat-no-separator
# (SB precedent), then suffixed variants.
AMLEGAL_SLUG_VARIANTS = [
    "{slug}",          # san_diego  (SD)
    "{concat}",        # sanbernardino  (SB)
    "{slug}_county",
    "{concat}county",
    "{slug}_ca",
    "{concat}ca",
]

# qcode.us slug variations.
# Pattern: qcode.us/codes/<slug>/
# Placer precedent (2026-05-16): slug is concatenated (`placercounty`, no
# separator). Note: some qcode jurisdictions pass-through to amlegal (e.g.
# Sacramento returns amlegal-style chrome), so we re-use the strict CA-county
# validity check.
QCODE_SLUG_VARIANTS = ["{slug}county", "{slug}", "{slug}_county", "{slug}-county"]


def get_counties() -> list[tuple[str, int]]:
    sql = """
    select county_name, count(*) as beaches
      from public.beaches_gold
     where state='CA' and is_active=true and county_name is not null
     group by county_name order by 2 desc;
    """
    env = os.environ.copy()
    env["PGPASSWORD"] = "BBVbup6ipvhTCOJ2"
    env["PGCLIENTENCODING"] = "UTF8"
    result = subprocess.run(
        ["psql",
         "postgres://postgres.ehlzbwtrsxaaukurekau@aws-1-us-east-1.pooler.supabase.com:5432/postgres",
         "-t", "-A", "-F", "\t", "-c", sql],
        capture_output=True, env=env, check=True,
    )
    out: list[tuple[str, int]] = []
    for line in result.stdout.decode("utf-8").splitlines():
        if not line.strip():
            continue
        name, beaches = line.split("\t", 1)
        out.append((name.strip(), int(beaches)))
    return out


def county_to_slug(county_name: str) -> str:
    """'Los Angeles' -> 'los_angeles', 'San Luis Obispo' -> 'san_luis_obispo'"""
    return county_name.lower().replace(" ", "_").replace("'", "")


def try_municode(county_slug: str) -> tuple[str, str] | None:
    """Returns (doc_slug, working_url) or None if not found.

    Tries each doc_slug variant; returns first that returns valid content
    (not the 'not authorized' SPA page).
    """
    for doc_slug in MUNICODE_DOC_SLUGS:
        url = f"https://library.municode.com/ca/{county_slug}_county/codes/{doc_slug}"
        try:
            text = fetch_html.fetch(url, selector="h1", wait_seconds=8, timeout_ms=30000)
        except Exception:
            continue
        if text and "Municode Codification Search" not in text and "requested content cannot be found" not in text:
            return (doc_slug, url)
        time.sleep(0.5)
    # Try with hyphen instead of underscore
    for doc_slug in MUNICODE_DOC_SLUGS:
        url = f"https://library.municode.com/ca/{county_slug.replace('_', '-')}_county/codes/{doc_slug}"
        try:
            text = fetch_html.fetch(url, selector="h1", wait_seconds=8, timeout_ms=30000)
        except Exception:
            continue
        if text and "Municode Codification Search" not in text and "requested content cannot be found" not in text:
            return (doc_slug, url)
        time.sleep(0.5)
    return None


# amlegal jurisdictions that are consolidated city-counties — their
# index page reads as a city (no "X County" string) but the slug is
# the right one for county-level beach work.
AMLEGAL_CONSOLIDATED_CITY_COUNTIES = {
    "San Francisco": "san_francisco",
}


def _is_valid_ca_county_page(text: str, county_name: str) -> bool:
    """Strict check: amlegal page must be the CA county we asked for.

    Empirical false positives caught during Phase A.2 (2026-05-16):
      - `humboldt` → Humboldt, IOWA (city) — same name, different state
      - `sacramentoca` → City of Sacramento, CA (not Sacramento County)

    The robust signal is the exact phrase "{county} County" in inner_text
    plus a California state indicator. Cities and same-named jurisdictions
    in other states won't carry the "<name> County" string in their amlegal
    chrome.
    """
    if not text:
        return False
    tl = text.lower()
    if "page not found" in tl:
        return False
    if f"{county_name.lower()} county" not in tl:
        return False
    if "california" not in tl and ", ca" not in tl:
        return False
    return True


def try_amlegal(county_name: str, county_slug: str) -> tuple[str, str] | None:
    """Returns (slug_variant, working_url) or None if not found.

    SD County's slug is bare (`san_diego`, no suffix); discovery missed
    it in Phase A because only Municode was probed. We try bare-slug
    first to bias toward that pattern, then `_county` / hyphenated /
    `_ca` variants.

    SF is whitelisted as a consolidated city-county: its amlegal index
    reads as a city, so the strict "<name> County" check would reject
    the correct slug.
    """
    if county_name in AMLEGAL_CONSOLIDATED_CITY_COUNTIES:
        jurisdiction_slug = AMLEGAL_CONSOLIDATED_CITY_COUNTIES[county_name]
        url = f"https://codelibrary.amlegal.com/codes/{jurisdiction_slug}/"
        try:
            text = fetch_html.fetch(url, wait_seconds=4, timeout_ms=30000)
        except Exception:
            return None
        if text and "page not found" not in text.lower() and county_name.lower() in text.lower():
            return (jurisdiction_slug, url)
        return None

    concat_slug = county_slug.replace("_", "")  # san_bernardino -> sanbernardino
    hyphen_slug = county_slug.replace("_", "-")
    slug_candidates: list[str] = []
    seen: set[str] = set()
    for variant in AMLEGAL_SLUG_VARIANTS:
        cand = variant.format(slug=county_slug, concat=concat_slug)
        if cand not in seen:
            slug_candidates.append(cand)
            seen.add(cand)
    if hyphen_slug != county_slug:
        for variant in AMLEGAL_SLUG_VARIANTS:
            cand = variant.format(slug=hyphen_slug, concat=concat_slug)
            if cand not in seen:
                slug_candidates.append(cand)
                seen.add(cand)

    for jurisdiction_slug in slug_candidates:
        url = f"https://codelibrary.amlegal.com/codes/{jurisdiction_slug}/"
        try:
            text = fetch_html.fetch(url, wait_seconds=4, timeout_ms=30000)
        except Exception:
            continue
        if _is_valid_ca_county_page(text, county_name):
            return (jurisdiction_slug, url)
        time.sleep(0.5)
    return None


def try_qcode(county_name: str, county_slug: str) -> tuple[str, str] | None:
    """Returns (slug_variant, working_url) or None if not found.

    qcode.us is a regional codification platform; some entries are native
    qcode UI (Placer), others pass through to amlegal (Sacramento). Both
    forms satisfy the strict CA-county validity check.

    Failure mode: page renders "Could not find <slug>" prominently.
    """
    slug_candidates: list[str] = []
    for variant in QCODE_SLUG_VARIANTS:
        slug_candidates.append(variant.format(slug=county_slug))
    hyphen_slug = county_slug.replace("_", "-")
    if hyphen_slug != county_slug:
        for variant in QCODE_SLUG_VARIANTS:
            slug_candidates.append(variant.format(slug=hyphen_slug))

    for jurisdiction_slug in slug_candidates:
        url = f"https://qcode.us/codes/{jurisdiction_slug}/"
        try:
            text = fetch_html.fetch(url, wait_seconds=4, timeout_ms=30000)
        except Exception:
            continue
        if not text:
            continue
        if f"could not find {jurisdiction_slug.lower()}" in text.lower():
            time.sleep(0.5)
            continue
        if _is_valid_ca_county_page(text, county_name):
            return (jurisdiction_slug, url)
        time.sleep(0.5)
    return None


def find_animal_chapter(url: str) -> str | None:
    """Look in the Municode TOC for an animal-regulation chapter title."""
    try:
        html = fetch_html.fetch(url, selector="#content", raw_html=False, wait_seconds=12, timeout_ms=30000)
    except Exception:
        return None
    # Look for chapter/title labels mentioning animals
    for line in html.splitlines():
        ll = line.lower()
        if ("animal" in ll or "dogs" in ll) and ("chapter" in ll or "title" in ll or "division" in ll):
            return line.strip()[:200]
    return None


def load_existing_status() -> dict[str, dict]:
    if not os.path.exists(STATUS_CSV):
        return {}
    out: dict[str, dict] = {}
    with open(STATUS_CSV, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            out[row["county"]] = row
    return out


def write_status(rows: list[dict]):
    with open(STATUS_CSV, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["county", "beaches", "platform", "doc_slug", "url", "animal_chapter_hint"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def main() -> int:
    counties = get_counties()
    print(f"Discovered {len(counties)} CA counties with beaches", file=sys.stderr)

    existing = load_existing_status()
    rows: list[dict] = list(existing.values())
    rows_by_county = {r["county"]: r for r in rows}

    for name, beaches in counties:
        if name in rows_by_county and rows_by_county[name].get("platform") not in (None, "", "unknown"):
            print(f"  {name}: already discovered ({rows_by_county[name].get('platform')}), skipping", file=sys.stderr)
            continue

        county_slug = county_to_slug(name)
        print(f"  {name} ({beaches} beaches, slug={county_slug}) ... ", end="", file=sys.stderr, flush=True)

        muni = try_municode(county_slug)
        row = {"county": name, "beaches": beaches}

        if muni:
            doc_slug, url = muni
            row["platform"] = "municode"
            row["doc_slug"] = doc_slug
            row["url"] = url
            chapter = find_animal_chapter(url)
            row["animal_chapter_hint"] = chapter or ""
            print(f"municode/{doc_slug}", file=sys.stderr)
        else:
            # amlegal fallback (added 2026-05-16 after SD precedent).
            # SD's bare slug `san_diego` was missed by Municode-only Phase A.
            am = try_amlegal(name, county_slug)
            if am:
                jurisdiction_slug, url = am
                row["platform"] = "amlegal"
                row["doc_slug"] = jurisdiction_slug
                row["url"] = url
                row["animal_chapter_hint"] = ""
                print(f"amlegal/{jurisdiction_slug}", file=sys.stderr)
            else:
                # qcode.us fallback (added 2026-05-16 after Placer precedent).
                qc = try_qcode(name, county_slug)
                if qc:
                    jurisdiction_slug, url = qc
                    row["platform"] = "qcode"
                    row["doc_slug"] = jurisdiction_slug
                    row["url"] = url
                    row["animal_chapter_hint"] = ""
                    print(f"qcode/{jurisdiction_slug}", file=sys.stderr)
                else:
                    row["platform"] = "unknown"
                    row["doc_slug"] = ""
                    row["url"] = ""
                    row["animal_chapter_hint"] = ""
                    print("not on municode/amlegal/qcode", file=sys.stderr)

        rows_by_county[name] = row
        # Write after each county so partial progress survives interrupt
        write_status(list(rows_by_county.values()))

    print(f"\nDone. Status written to {STATUS_CSV}", file=sys.stderr)
    print(f"Summary:", file=sys.stderr)
    by_platform: dict[str, int] = {}
    for r in rows_by_county.values():
        p = r.get("platform") or "unknown"
        by_platform[p] = by_platform.get(p, 0) + 1
    for p, n in sorted(by_platform.items(), key=lambda x: -x[1]):
        print(f"  {p}: {n}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
