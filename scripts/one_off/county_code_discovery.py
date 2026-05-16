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
MUNICODE_DOC_SLUGS = ["code_of_ordinances", "municipal_code", "code"]


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
            row["platform"] = "unknown"
            row["doc_slug"] = ""
            row["url"] = ""
            row["animal_chapter_hint"] = ""
            print("not on municode", file=sys.stderr)

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
