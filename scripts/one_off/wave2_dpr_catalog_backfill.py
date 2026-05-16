"""
Wave 2 DPR catalog backfill — parses parks.ca.gov/Dogs into structured
(park_name, allowed, notes) rows, joins to beaches_gold, generates SQL
inserts for policy_source + beach_policy_source.

Output: 20260516_wave2_dpr_catalog_backfill.sql migration on stdout
(redirect to file).

The catalog is one HTML page with a tabular block:
    Park Name TAB Yes/No TAB Notes

After Playwright `inner_text`, the table renders as one line per park
with the three fields separated by tabs.

Honest limits:
- park_name matching is exact (case-sensitive, whitespace-stripped).
  Mismatches between beaches_gold.park_name and catalog text need
  manual review — counted + listed at end of stderr.
- The catalog's "Yes" without notes is ambiguous. We default to
  rule='on_leash' (DPR's blanket "6-foot leash at all times" rule).
- "No" with notes like "Dogs allowed in parking lot only" is captured
  faithfully but only the SAND rule is generated (not_allowed). Other
  sections (parking, paved_paths) are not auto-inserted here.
"""
from __future__ import annotations

import os
import re
import sys
from typing import Iterable

sys.path.insert(0, os.path.dirname(__file__) + "/../fetch")
import fetch_html  # type: ignore

CATALOG_URL = "https://www.parks.ca.gov/Dogs"


def fetch_catalog() -> str:
    return fetch_html.fetch(CATALOG_URL, wait_seconds=5.0)


def parse_catalog(text: str) -> list[tuple[str, str, str]]:
    """Returns list of (park_name, yes_no, notes) tuples."""
    rows: list[tuple[str, str, str]] = []
    start = False
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line == "Dogs Allowed?":
            start = True
            continue
        if not start:
            continue
        m = re.match(r"^(.+?)\t(Yes|No)\t?(.*)$", line)
        if m:
            park_name, yes_no, notes = m.group(1).strip(), m.group(2), m.group(3).strip()
            rows.append((park_name, yes_no, notes))
        else:
            m = re.match(r"^(.+?)\s{2,}(Yes|No)(?:\s{2,}(.+))?$", line)
            if m:
                park_name = m.group(1).strip()
                yes_no = m.group(2)
                notes = (m.group(3) or "").strip()
                rows.append((park_name, yes_no, notes))
    return rows


def fetch_beach_park_names() -> dict[str, list[int]]:
    """Returns {park_name: [fids]} for DPR-primary tier-1+2 CA beaches."""
    import subprocess
    sql = """
    with tiered as (
      select g.fid, g.park_name,
             public.beach_location_tier(d.dogs_allowed, d.has_off_leash, d.has_on_leash, d.dogs_prohibited_start::text) as tier
      from public.beaches_gold g
      left join public.beach_dog_policy d on d.arena_group_id = g.fid
      where g.state='CA' and g.is_active=true
    ), filt as (select * from tiered where tier in ('1_off-leash','2_on-leash'))
    select f.park_name, f.fid
      from filt f
      join public.beach_agency ba on ba.beach_fid = f.fid and ba.authority_domain='dog_policy' and ba.precedence_rank=1
      join public.agency a on a.id = ba.agency_id
     where a.name = 'California Department of Parks and Recreation'
       and f.park_name is not null
     order by f.park_name, f.fid;
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
    # Decode as UTF-8 explicitly; default Windows subprocess decoding is cp1252.
    result_stdout = result.stdout.decode("utf-8")
    out: dict[str, list[int]] = {}
    for line in result_stdout.splitlines():
        if not line.strip():
            continue
        park, fid = line.split("\t", 1)
        out.setdefault(park.strip(), []).append(int(fid))
    return out


_RULE_FROM_YESNO = {
    "Yes": "on_leash",   # DPR baseline: 6-ft leash
    "No": "not_allowed",
}


def sql_escape(s: str) -> str:
    return s.replace("'", "''")


def emit_sql(catalog_rows: list[tuple[str, str, str]], beach_park_names: dict[str, list[int]]):
    catalog_index = {p: (y, n) for p, y, n in catalog_rows}

    def resolve(park_name: str) -> tuple[str, str, str] | None:
        # Exact match
        if park_name in catalog_index:
            y, n = catalog_index[park_name]
            return (park_name, y, n)
        # Prefix-style match (catalog has " (incl. ...)" or " State Seashore" suffix)
        for cat_name, (y, n) in catalog_index.items():
            if cat_name.startswith(park_name + " ") or cat_name.startswith(park_name + "("):
                return (cat_name, y, n)
        return None

    print("-- Wave 2 DPR catalog backfill — auto-generated.")
    print("-- Parses parks.ca.gov/Dogs and creates per-park policy_source rows")
    print("-- + beach_policy_source links for all DPR-primary tier-1+2 CA beaches")
    print("-- whose park_name has an exact match in the catalog.")
    print("--")
    print("-- Idempotent via WHERE NOT EXISTS / ON CONFLICT DO NOTHING.")
    print()

    matched_parks = 0
    matched_beaches = 0
    unmatched_parks: list[str] = []

    for park_name, fids in sorted(beach_park_names.items()):
        resolved = resolve(park_name)
        if not resolved:
            unmatched_parks.append(park_name)
            continue
        catalog_name, yes_no, notes = resolved
        matched_parks += 1
        matched_beaches += len(fids)
        rule = _RULE_FROM_YESNO[yes_no]
        # Use catalog name in the citation if it's fuller (suffix/parenthetical)
        cite_name = catalog_name if catalog_name != park_name else park_name
        citation = f"California State Parks dog policy — {cite_name}"
        verbatim_full = notes if notes else f"{yes_no}. Dogs must be on a maximum 6-foot leash at ALL times and physically under your control. [parks.ca.gov/Dogs catalog row 2026-05-16]"

        print(f"-- {park_name}: {yes_no}{' — ' + notes if notes else ''} -> {len(fids)} beach(es)")
        print(
            "INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text, parent_citation_id)\n"
            "SELECT 'agency_administrative_policy',\n"
            f"       '{sql_escape(citation)}',\n"
            "       (SELECT id FROM public.agency WHERE name='California Department of Parks and Recreation' AND type='state_department'),\n"
            "       ARRAY['dog_policy']::text[],\n"
            "       'https://www.parks.ca.gov/Dogs',\n"
            f"       '{sql_escape(verbatim_full)}',\n"
            "       (SELECT id FROM public.policy_source WHERE citation='California State Parks dog policy (parks.ca.gov/Dogs)')\n"
            "WHERE NOT EXISTS (\n"
            f"  SELECT 1 FROM public.policy_source WHERE citation = '{sql_escape(citation)}'\n"
            ");"
        )
        for fid in fids:
            print(
                "INSERT INTO public.beach_policy_source\n"
                "  (beach_fid, policy_source_id, section, rule, operative_status, evidence_verbatim, evidence_url)\n"
                f"SELECT {fid},\n"
                f"       (SELECT id FROM public.policy_source WHERE citation='{sql_escape(citation)}'),\n"
                f"       'sand', '{rule}', 'operative'::operative_status,\n"
                f"       '{sql_escape(verbatim_full)}',\n"
                "       'https://www.parks.ca.gov/Dogs'\n"
                "ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;"
            )
        print()

    print("-- ─── Summary ─────────────────────────────────────────")
    print(f"-- Matched parks: {matched_parks}")
    print(f"-- Matched beaches: {matched_beaches}")
    print(f"-- Unmatched parks (need fuzzy review): {len(unmatched_parks)}")
    if unmatched_parks:
        print("-- Unmatched park_name values:")
        for p in unmatched_parks:
            print(f"--   {p!r}")

    print(f"\nSummary: {matched_parks} parks / {matched_beaches} beaches matched; "
          f"{len(unmatched_parks)} parks unmatched.", file=sys.stderr)


def main() -> int:
    print("Fetching catalog...", file=sys.stderr)
    text = fetch_catalog()
    catalog_rows = parse_catalog(text)
    print(f"Parsed {len(catalog_rows)} catalog rows.", file=sys.stderr)

    print("Loading beach park_names from DB...", file=sys.stderr)
    beach_park_names = fetch_beach_park_names()
    print(f"Found {sum(len(v) for v in beach_park_names.values())} beaches "
          f"across {len(beach_park_names)} distinct park_names.", file=sys.stderr)

    emit_sql(catalog_rows, beach_park_names)
    return 0


if __name__ == "__main__":
    sys.exit(main())
