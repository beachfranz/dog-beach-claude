"""
test_ca_cpad_vs_staging.py
--------------------------
For the 955 CA points: join to beaches_staging_new on display_name and
compare CPAD classification (from ST_Contains on cpad_units) with
staging's governing_jurisdiction. Produces a contingency table.
"""

import csv
import json
import re
import subprocess
from pathlib import Path

CSV_PATH = Path(r"C:\Users\beach\Documents\dog-beach-claude\share\Dog_Beaches\US_beaches_with_state.csv")
TMP_SQL  = Path(r"C:\Users\beach\Documents\dog-beach-claude\supabase\.temp\q.sql")


def load_ca_rows():
    rows = []
    with open(CSV_PATH, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r.get("STATE") != "CA": continue
            wkt = r.get("WKT", "")
            m = re.match(r"POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)", wkt, re.I)
            if not m: continue
            name = (r.get("NAME") or "").strip()
            if not name: continue
            fid  = int(r.get("fid", "0"))
            lon, lat = float(m.group(1)), float(m.group(2))
            rows.append((fid, name.replace("'", "''"), lat, lon))
    return rows


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
    rows = load_ca_rows()
    print(f"Loaded {len(rows):,} CA rows\n")

    values = "values " + ",\n".join(
        f"({fid}, '{name}', {lat}, {lon})" for fid, name, lat, lon in rows
    )

    # Contingency: staging governing_jurisdiction × CPAD bucket, now
    # with a 100m buffer (ST_DWithin) so coastal points that sit just
    # offshore of a polygon edge are still counted. Buffer introduces
    # multi-polygon overlap → pick one CPAD row per point by priority:
    # Federal > State > County > City > Other.
    buffer_m = 100
    sql_contingency = f"""
      with points(fid, name, lat, lon) as ({values}),
      staging_pick as (
        select distinct on (p.fid)
          p.fid,
          s.governing_jurisdiction as staging_jur,
          s.id is not null         as has_staging_row
        from points p
        left join public.beaches_staging_new s
          on s.display_name = p.name
          and (s.state = 'California' or s.state is null)
        order by p.fid, (s.state = 'California') desc nulls last, s.id desc
      ),
      cpad_hits as (
        select
          p.fid,
          c.agncy_lev,
          case c.agncy_lev
            when 'Federal' then 1
            when 'State'   then 2
            when 'County'  then 3
            when 'City'    then 4
            else                5
          end as priority
        from points p
        join public.cpad_units c
          on c.geom && ST_Expand(ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326), 0.001)
         and ST_DWithin(c.geom, ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326), 0.001)
      ),
      cpad_pick as (
        select distinct on (fid) fid, agncy_lev
        from cpad_hits
        order by fid, priority asc, agncy_lev asc
      ),
      cpad_pick_full as (
        select p.fid, cp.agncy_lev
        from points p
        left join cpad_pick cp using (fid)
      )
      select
        case
          when not sp.has_staging_row then '(no staging row)'
          when sp.staging_jur is null then '(jurisdiction null)'
          else sp.staging_jur
        end as staging_jur,
        case
          when cp.agncy_lev is null  then 'none'
          when cp.agncy_lev = 'Federal' then 'federal'
          when cp.agncy_lev = 'State'   then 'state'
          when cp.agncy_lev = 'County'  then 'county'
          when cp.agncy_lev = 'City'    then 'local'
          else                               'other'
        end as cpad_bucket,
        count(*) as n
      from points p
      join staging_pick    sp using (fid)
      join cpad_pick_full  cp using (fid)
      group by 1, 2
      order by 1, 2
    """

    # Summary: how many inside-any + distribution of polygons/point
    sql_summary = f"""
      with points(fid, name, lat, lon) as ({values}),
      per_point as (
        select p.fid, count(c.objectid) as n
        from points p
        left join public.cpad_units c
          on ST_DWithin(
               c.geom,
               ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326),
               0.001  -- ~100m at CA latitude; geom-level so GIST index is used
             )
        group by p.fid
      )
      select n as polygons_per_point, count(*) as points
      from per_point group by n order by n
    """

    sql_match_rate = f"""
      with points(fid, name, lat, lon) as ({values}),
      matched as (
        select distinct p.fid
        from points p
        join public.beaches_staging_new s on s.display_name = p.name
      )
      select
        (select count(*) from points) as total,
        (select count(*) from matched) as with_staging_match,
        ((select count(*) from matched)::float / (select count(*) from points) * 100)::numeric(5,1) as pct
    """

    # Staging match rate
    print("=== CSV name match rate against beaches_staging_new ===")
    for r in run_sql(sql_match_rate):
        print(f"  {r['with_staging_match']:,} of {r['total']:,} CSV rows match a staging row by display_name ({r['pct']}%)")

    # Polygons-per-point distribution with buffer
    print(f"\n=== Polygons-per-point distribution (with 100m buffer) ===")
    summary = run_sql(sql_summary)
    inside_any = sum(r["points"] for r in summary if r["polygons_per_point"] > 0)
    print(f"  {inside_any:,} of 955 inside >=1 polygon ({inside_any/9.55:.1f}%)")
    for r in summary:
        print(f"  {r['polygons_per_point']:>3} polygon(s): {r['points']:,} points")

    # Contingency
    print("\n=== Contingency: staging governing_jurisdiction × CPAD bucket (100m buffer, priority pick) ===")
    data = run_sql(sql_contingency)
    buckets = ["federal", "state", "county", "local", "other", "none"]
    jurs = sorted({r["staging_jur"] for r in data})

    # Header
    print(f"\n  {'staging_jurisdiction':<28s} | " + " | ".join(f"{b:>8s}" for b in buckets) + " | total")
    print("  " + "-" * (28 + 3 + 11 * len(buckets) + 8))
    totals = {b: 0 for b in buckets}
    for j in jurs:
        cells = {b: 0 for b in buckets}
        for r in data:
            if r["staging_jur"] == j:
                cells[r["cpad_bucket"]] = r["n"]
        row_total = sum(cells.values())
        print(f"  {j:<28s} | " + " | ".join(f"{cells[b]:>8d}" for b in buckets) + f" | {row_total:>5d}")
        for b in buckets: totals[b] += cells[b]
    print("  " + "-" * (28 + 3 + 11 * len(buckets) + 8))
    print(f"  {'column totals':<28s} | " + " | ".join(f"{totals[b]:>8d}" for b in buckets) + f" | {sum(totals.values()):>5d}")


if __name__ == "__main__":
    main()
