"""
test_ca_points_in_cpad.py
-------------------------
Takes the 955 rows labeled CA in US_beaches_with_state.csv, sends their
fid/lat/lng to PostGIS, and reports:
  1. How many fall inside >=1 CPAD polygon (via ST_Contains).
  2. Distribution of polygons-per-point.
  3. Count of points by polygon agency-level bucket
     (federal / state / county / local / other / easement).

Easement is N/A because the CPAD Units layer rolls easements into their
owning agency's row — easement-as-such lives in the Holdings layer, not
loaded yet.
"""

import csv
import json
import re
import subprocess
from pathlib import Path

CSV_PATH = Path(r"C:\Users\beach\Documents\dog-beach-claude\share\Dog_Beaches\US_beaches_with_state.csv")


def load_ca_points():
    pts = []
    with open(CSV_PATH, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("STATE") != "CA":
                continue
            wkt = row.get("WKT", "")
            m = re.match(r"POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)", wkt, re.I)
            if not m:
                continue
            lon, lat = float(m.group(1)), float(m.group(2))
            fid = int(row.get("fid", "0"))
            pts.append((fid, lat, lon))
    return pts


def values_clause(pts):
    # Inline VALUES clause — 955 points × ~40 chars ≈ 40KB of SQL, fine.
    rows = [f"({fid}, {lat}, {lon})" for fid, lat, lon in pts]
    return "VALUES " + ",\n".join(rows)


TMP_SQL = Path(r"C:\Users\beach\Documents\dog-beach-claude\supabase\.temp\q.sql")


def run_sql(sql):
    TMP_SQL.parent.mkdir(parents=True, exist_ok=True)
    TMP_SQL.write_text(sql, encoding="utf-8")
    r = subprocess.run(
        ["supabase", "db", "query", "--linked", "-f", str(TMP_SQL)],
        capture_output=True, text=True, timeout=300,
    )
    if r.returncode != 0:
        raise RuntimeError(r.stderr)
    start = r.stdout.find("{")
    end   = r.stdout.rfind("}")
    return json.loads(r.stdout[start:end+1])["rows"]


def main():
    pts = load_ca_points()
    print(f"Loaded {len(pts):,} CA points from CSV\n")
    pts_sql = values_clause(pts)

    # Q1: how many points fall inside at least one CPAD polygon
    q1 = f"""
      with points(fid, lat, lon) as ({pts_sql}),
      hits as (
        select distinct p.fid
        from points p
        join public.cpad_units c on ST_Contains(c.geom, ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326))
      )
      select
        (select count(*) from points) as total,
        (select count(*) from hits)   as inside_any,
        ((select count(*) from hits)::float / (select count(*) from points) * 100)::numeric(5,1) as pct
    """
    print("=== Q1: points inside >=1 CPAD polygon ===")
    for r in run_sql(q1):
        print(f"  {r['inside_any']:,} of {r['total']:,} ({r['pct']}%)")

    # Q2: distribution of polygons per point (including 0)
    q2 = f"""
      with points(fid, lat, lon) as ({pts_sql}),
      per_point as (
        select p.fid, count(c.objectid) as n
        from points p
        left join public.cpad_units c on ST_Contains(c.geom, ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326))
        group by p.fid
      )
      select n as polygons_per_point, count(*) as points
      from per_point group by n order by n
    """
    print("\n=== Q2: distribution — polygons per point ===")
    for r in run_sql(q2):
        print(f"  {r['polygons_per_point']:>3} polygon(s): {r['points']:,} points")

    # Q3: bucket breakdown (a point can be in multiple polygons across buckets)
    q3 = f"""
      with points(fid, lat, lon) as ({pts_sql}),
      hits as (
        select distinct p.fid, case c.agncy_lev
          when 'Federal'          then 'federal'
          when 'State'            then 'state'
          when 'County'           then 'county'
          when 'City'             then 'local'
          else                         'other'
        end as bucket
        from points p
        join public.cpad_units c on ST_Contains(c.geom, ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326))
      )
      select bucket, count(distinct fid) as points
      from hits group by bucket order by points desc
    """
    print("\n=== Q3: points by polygon agency-level ===")
    for r in run_sql(q3):
        print(f"  {r['bucket']:<10s} {r['points']:,} points")
    print(f"  {'easement':<10s} N/A — not in CPAD Units layer (lives in Holdings, not loaded)")


if __name__ == "__main__":
    main()
