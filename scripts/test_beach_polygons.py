"""
test_beach_polygons.py
----------------------
Flip side of the CA-points-in-CPAD test: for each CPAD polygon, how
many of the 955 CA beach points fall inside it (with 100m buffer)?
Surface which polygons are beach-associated, broken down by agency
level and compared against "Beach" in UNIT_NAME.
"""

import csv
import json
import re
import subprocess
from pathlib import Path

CSV_PATH = Path(r"C:\Users\beach\Documents\dog-beach-claude\share\Dog_Beaches\US_beaches_with_state.csv")
TMP_SQL  = Path(r"C:\Users\beach\Documents\dog-beach-claude\supabase\.temp\q.sql")


def load_ca_points():
    pts = []
    with open(CSV_PATH, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r.get("STATE") != "CA": continue
            m = re.match(r"POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)", r.get("WKT", ""), re.I)
            if not m: continue
            lon, lat = float(m.group(1)), float(m.group(2))
            pts.append((int(r.get("fid", "0")), lat, lon))
    return pts


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
    print(f"Loaded {len(pts):,} CA points\n")

    values = "values " + ",\n".join(f"({f}, {la}, {lo})" for f, la, lo in pts)

    # Q1: polygons that contain at least one CA beach point (with 100m buffer)
    q1 = f"""
      with points(fid, lat, lon) as ({values}),
      beach_polys as (
        select
          c.objectid,
          c.unit_name,
          c.agncy_lev,
          c.mng_ag_lev,
          c.county,
          c.acres,
          count(distinct p.fid) as beach_count
        from points p
        join public.cpad_units c
          on c.geom && ST_Expand(ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326), 0.001)
         and ST_DWithin(c.geom, ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326), 0.001)
        group by c.objectid, c.unit_name, c.agncy_lev, c.mng_ag_lev, c.county, c.acres
      )
      select
        (select count(*) from beach_polys)       as beach_polygons,
        (select count(*) from public.cpad_units) as total_polygons,
        ((select count(*) from beach_polys)::float / (select count(*) from public.cpad_units) * 100)::numeric(5,2) as pct
    """

    q2 = f"""
      with points(fid, lat, lon) as ({values}),
      beach_polys as (
        select distinct c.objectid, c.agncy_lev
        from points p
        join public.cpad_units c
          on c.geom && ST_Expand(ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326), 0.001)
         and ST_DWithin(c.geom, ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326), 0.001)
      )
      select agncy_lev, count(*) as polygons
      from beach_polys group by agncy_lev order by polygons desc
    """

    q3 = f"""
      with points(fid, lat, lon) as ({values}),
      beach_polys as (
        select c.objectid, c.unit_name, c.agncy_lev, c.county, c.acres, count(distinct p.fid) as n
        from points p
        join public.cpad_units c
          on c.geom && ST_Expand(ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326), 0.001)
         and ST_DWithin(c.geom, ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326), 0.001)
        group by 1, 2, 3, 4, 5
      )
      select unit_name, agncy_lev, county, round(acres::numeric, 0) as acres, n as beach_count
      from beach_polys order by n desc, acres desc limit 20
    """

    q4 = f"""
      with points(fid, lat, lon) as ({values}),
      spatial as (
        select distinct c.objectid
        from points p
        join public.cpad_units c
          on c.geom && ST_Expand(ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326), 0.001)
         and ST_DWithin(c.geom, ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326), 0.001)
      ),
      nominal as (
        select objectid from public.cpad_units where unit_name ilike '%beach%'
      )
      select
        (select count(*) from spatial) as spatial_only_hits,
        (select count(*) from nominal) as nominal_only_hits,
        (select count(*) from spatial s join nominal n using (objectid)) as both,
        (select count(*) from spatial where objectid not in (select objectid from nominal)) as spatial_not_named_beach,
        (select count(*) from nominal where objectid not in (select objectid from spatial)) as named_beach_no_hit
    """

    print("=== Q1: CPAD polygons associated with beaches (100m buffer) ===")
    for r in run_sql(q1):
        print(f"  {r['beach_polygons']:,} of {r['total_polygons']:,} polygons touch a beach point ({r['pct']}%)")

    print("\n=== Q2: beach-associated polygons by agency level ===")
    for r in run_sql(q2):
        print(f"  {r['agncy_lev']:<25s} {r['polygons']:>5d} polygons")

    print("\n=== Q3: top 20 polygons by # of beach points ===")
    print(f"  {'unit_name':<50s} {'level':<8} {'county':<20} {'acres':>10}  beaches")
    print(f"  {'-'*50} {'-'*8} {'-'*20} {'-'*10}  -------")
    for r in run_sql(q3):
        name = (r['unit_name'] or '')[:50]
        print(f"  {name:<50s} {r['agncy_lev']:<8} {(r['county'] or ''):<20} {str(r['acres']):>10}  {r['beach_count']:>5}")

    print("\n=== Q4: spatial-hit polygons vs UNIT_NAME containing 'Beach' ===")
    for r in run_sql(q4):
        print(f"  spatial hit (buffered):               {r['spatial_only_hits']:,} polygons")
        print(f"  UNIT_NAME contains 'Beach':           {r['nominal_only_hits']:,} polygons")
        print(f"  both (spatial AND named 'Beach'):     {r['both']:,}")
        print(f"  spatial-hit polygons NOT beach-named: {r['spatial_not_named_beach']:,}  (e.g. 'Crystal Cove State Park' — has beach points but name doesn't say 'Beach')")
        print(f"  beach-named polygons with NO hit:     {r['named_beach_no_hit']:,}  (beaches in CPAD that have no CSV point nearby)")


if __name__ == "__main__":
    main()
