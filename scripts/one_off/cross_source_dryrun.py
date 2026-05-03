"""Dry-run cross-source POI→OSM matching passes A, B, C."""
import os, urllib.parse, psycopg2
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")

conn = psycopg2.connect(**PG); cur = conn.cursor()
conn.autocommit = True
cur.execute("set statement_timeout = '300s';")
cur.execute("refresh materialized view public.arena_group_polys;")

cur.execute("""
  select count(*) from public.arena
   where source_code='poi' and is_active=true and group_id = fid;
""")
n_singleton = cur.fetchone()[0]
print(f"Baseline: singleton active POIs = {n_singleton}\n")

# Pass A
print("─── PASS A — 250m buffer + (same cpad OR name-sim >= 0.7) ───")
cur.execute("""
  with cands as (
    select poi.fid as poi_fid, gleader.fid as poly_fid,
           ST_Distance(poi.geom::geography, g.poly::geography)::int m,
           similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))) sim
      from public.arena poi
      join public.arena_group_polys g
        on poi.source_code='poi' and poi.is_active=true and poi.group_id = poi.fid
       and ST_DWithin(g.poly::geography, poi.geom::geography, 250)
      join public.arena gleader on gleader.fid = g.group_id
     where (poi.cpad_unit_id is not null and poi.cpad_unit_id = gleader.cpad_unit_id)
        or similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))) >= 0.7
  ),
  pick as (select distinct on (poi_fid) * from cands order by poi_fid, sim desc, m asc)
  select count(*) from pick;
""")
n_a = cur.fetchone()[0]
print(f"  Pass A: {n_a} POIs\n")

cur.execute("""
  with cands as (
    select poi.fid as poi_fid, poi.name as poi_name,
           gleader.fid as poly_fid, gleader.name as poly_name,
           ST_Distance(poi.geom::geography, g.poly::geography)::int m,
           similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,'')))::numeric(3,2) sim,
           (poi.cpad_unit_id = gleader.cpad_unit_id) same_cpad
      from public.arena poi
      join public.arena_group_polys g
        on poi.source_code='poi' and poi.is_active=true and poi.group_id = poi.fid
       and ST_DWithin(g.poly::geography, poi.geom::geography, 250)
      join public.arena gleader on gleader.fid = g.group_id
     where (poi.cpad_unit_id is not null and poi.cpad_unit_id = gleader.cpad_unit_id)
        or similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))) >= 0.7
  ),
  pick as (select distinct on (poi_fid) * from cands order by poi_fid, sim desc, m asc)
  select * from pick order by sim desc, m asc limit 12;
""")
print("  Top 12 samples:")
for r in cur.fetchall():
    print(f"    fid={r[0]:<6} '{(r[1] or '')[:28]:28}' -> poly={r[2]} '{(r[3] or '')[:28]:28}'  sim={r[5]} m={r[4]:>4} same_cpad={r[6]}")

# Pass B
print("\n─── PASS B — name-sim >= 0.85 + same county (no spatial) ───")
cur.execute("""
  with cands as (
    select poi.fid as poi_fid,
           gleader.fid as poly_fid,
           similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))) sim,
           ST_Distance(poi.geom::geography, gleader.geom::geography)::int m
      from public.arena poi
      join public.arena gleader
        on poi.source_code='poi' and poi.is_active=true and poi.group_id = poi.fid
       and gleader.source_code='osm' and gleader.is_active=true and gleader.fid = gleader.group_id
       and poi.county_fips = gleader.county_fips
       and similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))) >= 0.85
  ),
  pick as (select distinct on (poi_fid) * from cands order by poi_fid, sim desc, m asc)
  select count(*) from pick;
""")
n_b = cur.fetchone()[0]
print(f"  Pass B: {n_b} POIs\n")

cur.execute("""
  with cands as (
    select poi.fid as poi_fid, poi.name as poi_name,
           gleader.fid as poly_fid, gleader.name as poly_name,
           similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,'')))::numeric(3,2) sim,
           ST_Distance(poi.geom::geography, gleader.geom::geography)::int m
      from public.arena poi
      join public.arena gleader
        on poi.source_code='poi' and poi.is_active=true and poi.group_id = poi.fid
       and gleader.source_code='osm' and gleader.is_active=true and gleader.fid = gleader.group_id
       and poi.county_fips = gleader.county_fips
       and similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))) >= 0.85
  ),
  pick as (select distinct on (poi_fid) * from cands order by poi_fid, sim desc, m asc)
  select * from pick order by m desc limit 12;
""")
print("  Top 12 by FURTHEST (dramatic catches):")
for r in cur.fetchall():
    print(f"    fid={r[0]:<6} '{(r[1] or '')[:28]:28}' -> poly={r[2]} '{(r[3] or '')[:28]:28}'  sim={r[4]} m={r[5]:>5}")

# Pass C
print("\n─── PASS C — same CPAD + name-sim >= 0.7 ───")
cur.execute("""
  with cands as (
    select poi.fid as poi_fid, gleader.fid as poly_fid,
           similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))) sim,
           ST_Distance(poi.geom::geography, gleader.geom::geography)::int m
      from public.arena poi
      join public.arena gleader
        on poi.source_code='poi' and poi.is_active=true and poi.group_id = poi.fid
       and gleader.source_code='osm' and gleader.is_active=true and gleader.fid = gleader.group_id
       and poi.cpad_unit_id is not null
       and poi.cpad_unit_id = gleader.cpad_unit_id
       and similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))) >= 0.7
  ),
  pick as (select distinct on (poi_fid) * from cands order by poi_fid, sim desc, m asc)
  select count(*) from pick;
""")
n_c = cur.fetchone()[0]
print(f"  Pass C: {n_c} POIs\n")

# Union — unique POIs across A,B,C
cur.execute("""
  with all_cands as (
    select poi.fid poi_fid, gleader.fid poly_fid,
           similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))) sim,
           ST_Distance(poi.geom::geography, g.poly::geography) m
      from public.arena poi
      join public.arena_group_polys g
        on poi.source_code='poi' and poi.is_active=true and poi.group_id = poi.fid
       and ST_DWithin(g.poly::geography, poi.geom::geography, 250)
      join public.arena gleader on gleader.fid = g.group_id
     where (poi.cpad_unit_id is not null and poi.cpad_unit_id = gleader.cpad_unit_id)
        or similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))) >= 0.7
    union all
    select poi.fid, gleader.fid,
           similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))),
           ST_Distance(poi.geom::geography, gleader.geom::geography)
      from public.arena poi
      join public.arena gleader
        on poi.source_code='poi' and poi.is_active=true and poi.group_id = poi.fid
       and gleader.source_code='osm' and gleader.is_active=true and gleader.fid = gleader.group_id
       and poi.county_fips = gleader.county_fips
       and similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))) >= 0.85
    union all
    select poi.fid, gleader.fid,
           similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))),
           ST_Distance(poi.geom::geography, gleader.geom::geography)
      from public.arena poi
      join public.arena gleader
        on poi.source_code='poi' and poi.is_active=true and poi.group_id = poi.fid
       and gleader.source_code='osm' and gleader.is_active=true and gleader.fid = gleader.group_id
       and poi.cpad_unit_id is not null and poi.cpad_unit_id = gleader.cpad_unit_id
       and similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))) >= 0.7
  )
  select count(distinct poi_fid) from all_cands;
""")
n_cumul = cur.fetchone()[0]
print(f"\nCUMULATIVE unique POIs across A union B union C: {n_cumul} (of {n_singleton} singletons)")
print(f"  Standalone POIs remaining after all 3 passes: {n_singleton - n_cumul}")
