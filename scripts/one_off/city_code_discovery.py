"""
City code platform discovery — Wave 3 of the law-as-primary-source-CA work.

For each incorporated CA city (place_type='C1') that bounds at least one
active beach in beaches_gold:
1. Try several Municode / amlegal / qcode URL patterns
2. Identify which platform (if any) hosts the municipal code
3. Output a CSV: city, county, beach_count, platform, doc_slug, url

Idempotent: reads existing scripts/one_off/city_code_discovery_status.csv
and skips cities already discovered.

Adapted from county_code_discovery.py — same Playwright fetch + strict
validity check (city-shaped: requires city name + California state
indicator + absence of "<name> county" string to avoid mis-matching the
same-named county).
"""
from __future__ import annotations

import csv
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(__file__) + "/../fetch")
import fetch_html  # type: ignore

STATUS_CSV = os.path.join(os.path.dirname(__file__), "city_code_discovery_status.csv")

# Municode doc-slug variants. Cities tend to use `code_of_ordinances` and
# `municipal_code` most often; `ordinance_code` was found on Contra Costa
# County so worth keeping in the list.
MUNICODE_DOC_SLUGS = ["code_of_ordinances", "municipal_code", "code", "ordinance_code"]

# amlegal city-slug variants. Precedents:
#   - LA City:    `los_angeles`         (snake)
#   - Long Beach: `longbeach`           (concat) per Rosie's walkthrough
#   - Reedley:    `reedleyca`           (concat + `ca`)
#   - Eureka:     `eureka`              (bare)
# Cities much more often use concat-no-separator and `ca` suffix than counties.
AMLEGAL_CITY_SLUG_VARIANTS = [
    "{slug}",          # los_angeles  (LA)
    "{concat}",        # longbeach    (LB)
    "{concat}ca",      # reedleyca    (Reedley)
    "{slug}_ca",       # manhattan_beach_ca
    "{slug}ca",        # manhattanbeachca
]

# Municode city-slug variants. Most CA cities use `<city>_ca` on Municode.
MUNICODE_CITY_SLUG_VARIANTS = [
    "{slug}_ca",       # manhattan_beach_ca
    "{slug}",          # manhattan_beach
    "{concat}",        # manhattanbeach
    "{concat}_ca",
]

# qcode.us city-slug variants. Pattern is generally bare city slug.
QCODE_CITY_SLUG_VARIANTS = ["{slug}", "{concat}", "{slug}ca", "{concat}ca"]


def get_cities() -> list[tuple[str, int]]:
    """Returns [(city_name, beach_count), ...] descending by beach count."""
    sql = """
    select j.name as city, count(distinct g.fid) as beaches
      from public.jurisdictions j
      join public.beaches_gold g on st_intersects(j.geom, g.geom)
     where j.state in ('California','CA') and j.place_type='C1'
       and g.state='CA' and g.is_active=true
     group by 1
     order by 2 desc;
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


def city_to_slug(city_name: str) -> str:
    """'Manhattan Beach' -> 'manhattan_beach', 'San Buenaventura (Ventura)' -> 'san_buenaventura'"""
    # Strip parenthetical (e.g., "San Buenaventura (Ventura)" → "San Buenaventura")
    name = city_name.split("(")[0].strip()
    return name.lower().replace(" ", "_").replace("'", "")


def _is_valid_ca_city_page(text: str, city_name: str) -> bool:
    """Strict CA-city validity check.

    Requirements:
      - non-empty text and no "page not found"
      - city name appears (lowercased)
      - California state indicator ("california" or ", ca")
      - "<city> county" does NOT appear (would indicate same-named county)
    """
    if not text:
        return False
    tl = text.lower()
    if "page not found" in tl or "could not find" in tl:
        return False
    cl = city_name.lower()
    if cl not in tl:
        return False
    if "california" not in tl and ", ca" not in tl:
        return False
    if f"{cl} county" in tl:
        return False
    return True


def try_municode(city_name: str, slug: str, concat: str) -> tuple[str, str] | None:
    seen: set[str] = set()
    for slug_var in MUNICODE_CITY_SLUG_VARIANTS:
        jurisdiction = slug_var.format(slug=slug, concat=concat)
        if jurisdiction in seen:
            continue
        seen.add(jurisdiction)
        for doc_slug in MUNICODE_DOC_SLUGS:
            url = f"https://library.municode.com/ca/{jurisdiction}/codes/{doc_slug}"
            try:
                text = fetch_html.fetch(url, selector="h1", wait_seconds=8, timeout_ms=30000)
            except Exception:
                continue
            if text and "Municode Codification Search" not in text and "requested content cannot be found" not in text:
                # Fall back to body validity check since `h1` selector may
                # not include state indicator
                try:
                    body = fetch_html.fetch(url, wait_seconds=6, timeout_ms=30000)
                except Exception:
                    body = ""
                if _is_valid_ca_city_page(body, city_name):
                    return (f"{jurisdiction}/{doc_slug}", url)
            time.sleep(0.3)
    return None


def try_amlegal(city_name: str, slug: str, concat: str) -> tuple[str, str] | None:
    seen: set[str] = set()
    for variant in AMLEGAL_CITY_SLUG_VARIANTS:
        jurisdiction = variant.format(slug=slug, concat=concat)
        if jurisdiction in seen:
            continue
        seen.add(jurisdiction)
        url = f"https://codelibrary.amlegal.com/codes/{jurisdiction}/"
        try:
            text = fetch_html.fetch(url, wait_seconds=4, timeout_ms=30000)
        except Exception:
            continue
        if _is_valid_ca_city_page(text, city_name):
            return (jurisdiction, url)
        time.sleep(0.3)
    return None


def try_qcode(city_name: str, slug: str, concat: str) -> tuple[str, str] | None:
    seen: set[str] = set()
    for variant in QCODE_CITY_SLUG_VARIANTS:
        jurisdiction = variant.format(slug=slug, concat=concat)
        if jurisdiction in seen:
            continue
        seen.add(jurisdiction)
        url = f"https://qcode.us/codes/{jurisdiction}/"
        try:
            text = fetch_html.fetch(url, wait_seconds=4, timeout_ms=30000)
        except Exception:
            continue
        if _is_valid_ca_city_page(text, city_name):
            return (jurisdiction, url)
        time.sleep(0.3)
    return None


def load_existing_status() -> dict[str, dict]:
    if not os.path.exists(STATUS_CSV):
        return {}
    out: dict[str, dict] = {}
    with open(STATUS_CSV, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            out[row["city"]] = row
    return out


def write_status(rows: list[dict]):
    with open(STATUS_CSV, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["city", "beaches", "platform", "doc_slug", "url"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def main() -> int:
    cities = get_cities()
    print(f"Discovered {len(cities)} CA incorporated cities with beaches", file=sys.stderr)

    existing = load_existing_status()
    rows_by_city = {c["city"]: c for c in existing.values()}

    for name, beaches in cities:
        if name in rows_by_city and rows_by_city[name].get("platform") not in (None, "", "unknown"):
            continue

        slug = city_to_slug(name)
        concat = slug.replace("_", "")
        print(f"  {name} ({beaches} beaches, slug={slug}) ... ", end="", file=sys.stderr, flush=True)

        row = {"city": name, "beaches": beaches}

        muni = try_municode(name, slug, concat)
        if muni:
            doc_slug, url = muni
            row["platform"] = "municode"
            row["doc_slug"] = doc_slug
            row["url"] = url
            print(f"municode/{doc_slug}", file=sys.stderr)
        else:
            am = try_amlegal(name, slug, concat)
            if am:
                jurisdiction, url = am
                row["platform"] = "amlegal"
                row["doc_slug"] = jurisdiction
                row["url"] = url
                print(f"amlegal/{jurisdiction}", file=sys.stderr)
            else:
                qc = try_qcode(name, slug, concat)
                if qc:
                    jurisdiction, url = qc
                    row["platform"] = "qcode"
                    row["doc_slug"] = jurisdiction
                    row["url"] = url
                    print(f"qcode/{jurisdiction}", file=sys.stderr)
                else:
                    row["platform"] = "unknown"
                    row["doc_slug"] = ""
                    row["url"] = ""
                    print("unknown", file=sys.stderr)

        rows_by_city[name] = row
        write_status(list(rows_by_city.values()))

    by_platform: dict[str, int] = {}
    for r in rows_by_city.values():
        p = r.get("platform") or "unknown"
        by_platform[p] = by_platform.get(p, 0) + 1
    print(f"\nSummary:", file=sys.stderr)
    for p, n in sorted(by_platform.items(), key=lambda x: -x[1]):
        print(f"  {p}: {n}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
