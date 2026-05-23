"""
arena_audit.py — durable arena-pipeline state audit.

Self-contained alternative to the in-Claude-session scheduled task.
Compares current arena state against the 2026-05-01 baseline (commit 29cf51c).

Reports a punch-list to stdout. Exits 0 always (informational).
Run via Windows Task Scheduler / Linux cron / on demand.

Usage:
    python scripts/one_off/arena_audit.py

Optionally redirect:
    python scripts/one_off/arena_audit.py >> ~/arena_audit.log 2>&1
"""
from __future__ import annotations
import os
import sys
import urllib.parse
from datetime import datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(
    host=p.hostname, port=p.port or 5432,
    user=p.username, password=os.environ["SUPABASE_DB_PASSWORD"],
    dbname=(p.path or "/postgres").lstrip("/"), sslmode="require",
)

# Baseline (2026-05-01, commit 29cf51c)
BASELINE = {
    "active_total": 1248,
    "active_osm": 565,
    "active_poi": 683,
    "distinct_groups": 802,
    "polygon_only_groups": 124,
    "multi_poi_groups_max": 4,  # dog_beach-guarded
    "known_inactive_reasons": {
        "outside_ca", "osm_unnamed_lt_1000_sqm", "name_has_digit",
        "no_polygon_geom", "inland_unnamed", "isolated_unnamed",
        "no_name", "business_keyword", "misgeocoded", "not_a_beach",
        # Prefixed reasons (counted via prefix match)
        "subsegment_of/", "prohibits_dogs", "likely_dup_of/",
        "secondary_in_cpad/", "ai_hail_mary/", "outside_ca",
    },
}

DRIFT_PCT = 5.0


def diag(label: str, actual: int, baseline: int) -> str:
    delta = actual - baseline
    pct = (100 * delta / baseline) if baseline else 0
    flag = " ⚠ DRIFT" if abs(pct) > DRIFT_PCT else ""
    return f"  {label:30} actual={actual:>6}  baseline={baseline:>6}  Δ={delta:+}  ({pct:+.1f}%){flag}"


def main() -> int:
    print(f"Arena audit — {datetime.now().isoformat(timespec='seconds')}\n")

    conn = psycopg2.connect(**PG)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("set statement_timeout = '120s';")

    # 1. Active counts vs baseline
    print("─── Active counts ───")
    cur.execute("""
      select count(*) filter (where is_active) total,
             count(*) filter (where is_active and source_code='osm') osm,
             count(*) filter (where is_active and source_code='poi') poi
        from public.arena;
    """)
    total, osm, poi = cur.fetchone()
    print(diag("active total", total, BASELINE["active_total"]))
    print(diag("active OSM",   osm,   BASELINE["active_osm"]))
    print(diag("active POI",   poi,   BASELINE["active_poi"]))

    # 2. Distinct groups
    cur.execute("select count(distinct group_id) from public.arena where is_active=true;")
    n_groups = cur.fetchone()[0]
    print(diag("distinct beach groups", n_groups, BASELINE["distinct_groups"]))

    # 3. Multi-POI group count
    print("\n─── Group structure ───")
    cur.execute("""
      with grp as (
        select group_id, count(*) filter (where source_code='poi') n
          from public.arena where is_active=true group by group_id
      )
      select n, count(*) groups from grp group by 1 order by 1;
    """)
    multi = 0
    poly_only = 0
    for n, c in cur.fetchall():
        if n == 0:
            poly_only = c
        elif n >= 2:
            multi += c
        print(f"  {n} POI(s) per group: {c} groups")
    flag = " ⚠" if multi > BASELINE["multi_poi_groups_max"] else ""
    print(f"\n  multi-POI groups: {multi} (baseline ≤ {BASELINE['multi_poi_groups_max']}){flag}")
    print(diag("polygon-only groups", poly_only, BASELINE["polygon_only_groups"]))

    # 4. Unknown inactive_reason values
    print("\n─── inactive_reason values ───")
    cur.execute("""
      select inactive_reason, count(*)
        from public.arena
       where inactive_reason is not null
       group by 1 order by 2 desc;
    """)
    unknown = []
    for reason, count in cur.fetchall():
        is_known = (
            reason in BASELINE["known_inactive_reasons"]
            or any(reason.startswith(prefix) for prefix in BASELINE["known_inactive_reasons"]
                   if prefix.endswith("/"))
        )
        marker = "" if is_known else " ⚠ UNKNOWN"
        print(f"  {reason[:40]:40} {count:>5}{marker}")
        if not is_known:
            unknown.append(reason)

    # 5. Landing-table sync gap
    print("\n─── Landing-table sync gap ───")
    cur.execute("""
      select count(*)
        from public.arena a
        join public.poi_landing pl on 'poi/' || pl.fid::text = a.source_id
       where a.source_code='poi' and a.is_active=true and pl.is_active=false;
    """)
    poi_gap = cur.fetchone()[0]
    flag = " ⚠" if poi_gap > 0 else " ✓"
    print(f"  POI active in arena but inactive in poi_landing: {poi_gap}{flag}")

    cur.execute("""
      with latest as (
        select distinct on (type, id) type, id, is_active
          from public.osm_landing
         order by type, id, fetched_at desc
      )
      select count(*)
        from public.arena a
        join latest l on a.source_id = 'osm/' || l.type || '/' || l.id::text
       where a.source_code='osm' and a.is_active=true and l.is_active=false;
    """)
    osm_gap = cur.fetchone()[0]
    flag = " ⚠" if osm_gap > 0 else " ✓"
    print(f"  OSM active in arena but inactive in osm_landing (latest): {osm_gap}{flag}")

    # 6. Materialized view freshness
    print("\n─── Materialized view ───")
    cur.execute("select count(*) from public.arena_group_polys;")
    matview_n = cur.fetchone()[0]
    cur.execute("select count(distinct group_id) from public.arena where is_active=true and source_code='osm';")
    osm_groups_now = cur.fetchone()[0]
    flag = " ⚠ stale" if abs(matview_n - osm_groups_now) > 5 else " ✓"
    print(f"  arena_group_polys rows: {matview_n}, current OSM-having groups: {osm_groups_now}{flag}")

    # Summary
    print("\n─── Summary ───")
    issues = []
    if abs(total - BASELINE["active_total"]) / BASELINE["active_total"] * 100 > DRIFT_PCT:
        issues.append(f"active total drift {((total - BASELINE['active_total'])/BASELINE['active_total']*100):+.1f}%")
    if multi > BASELINE["multi_poi_groups_max"]:
        issues.append(f"{multi} multi-POI groups (expected ≤ {BASELINE['multi_poi_groups_max']})")
    if unknown:
        issues.append(f"{len(unknown)} undocumented inactive_reason value(s)")
    if poi_gap or osm_gap:
        issues.append(f"landing-sync gap: poi={poi_gap}, osm={osm_gap}")

    if not issues:
        print("  ✓ all checks pass — arena state matches 2026-05-01 baseline")
    else:
        print(f"  ⚠ {len(issues)} issue(s) flagged:")
        for i in issues:
            print(f"    - {i}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
