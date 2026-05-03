"""Characterize the 516 unnamed active OSM polygons across multiple dimensions."""
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

cur.execute("""
  drop table if exists tmp_unnamed;
  create temp table tmp_unnamed as
  select a.fid, a.county_name, a.county_fips, a.cpad_unit_id,
         cu.unit_name as cpad_unit_name,
         a.nav_lat, a.nav_lon, a.geom,
         g.poly,
         ST_Area(g.poly::geography) as area_m2,
         a.source_id
    from public.arena a
    join public.arena_group_polys g on g.group_id = a.fid
    left join public.cpad_units cu on cu.unit_id = a.cpad_unit_id
   where a.source_code = 'osm' and a.is_active = true
     and a.fid = a.group_id
     and (a.name is null or trim(a.name) = '');
  create index tmp_un_geom on tmp_unnamed using gist (geom);
""")
cur.execute("select count(*) from tmp_unnamed;")
n_total = cur.fetchone()[0]
print(f"Unnamed active polygon group leaders: {n_total}\n")

print("─── 1. BY COUNTY ───")
cur.execute("""
  select coalesce(county_name,'<unknown>') c, count(*) n,
         (avg(area_m2)::int) avg_area
    from tmp_unnamed group by 1 order by 2 desc limit 15;
""")
for r in cur.fetchall():
    print(f"  {r[0]:18} {r[1]:>4}   avg={r[2]:>10,} m²")

# 2. Coastal vs inland
print("\n─── 2. COASTAL VS INLAND ───")
pacific = [(42.0,-124.21),(41.7,-124.16),(41.0,-124.12),(40.5,-124.40),(39.7,-123.83),(39.0,-123.72),(38.4,-123.07),(38.0,-122.97),(37.8,-122.50),(37.7,-122.51),(37.5,-122.50),(37.2,-122.40),(36.95,-122.06),(36.6,-121.90),(36.0,-121.55),(35.65,-121.28),(35.3,-120.85),(34.95,-120.65),(34.45,-120.47),(34.40,-119.74),(34.18,-119.22),(34.05,-118.80),(33.85,-118.40),(33.55,-117.90),(33.45,-117.74),(33.10,-117.32),(32.85,-117.27),(32.55,-117.13)]
bay = [(37.81,-122.48),(37.86,-122.32),(37.92,-122.31),(38.05,-122.27),(38.11,-122.25),(38.06,-122.10),(37.97,-122.05),(37.85,-122.08),(37.71,-122.20),(37.55,-122.20),(37.46,-121.97),(37.45,-121.93),(37.51,-122.02),(37.62,-122.13),(37.78,-122.39)]
catalina = [(33.485,-118.610),(33.470,-118.580),(33.450,-118.555),(33.443,-118.510),(33.420,-118.470),(33.400,-118.430),(33.380,-118.395),(33.360,-118.360),(33.345,-118.325),(33.310,-118.305),(33.295,-118.310),(33.310,-118.345),(33.330,-118.395),(33.355,-118.450),(33.385,-118.500),(33.420,-118.555),(33.450,-118.585),(33.485,-118.610)]
shore = "MULTILINESTRING(" + ", ".join("(" + ", ".join(f"{lon} {lat}" for lat,lon in line) + ")" for line in (pacific, bay, catalina)) + ")"
cur.execute("""
  with shore as (select ST_GeomFromText(%s, 4326) as line),
       d as (select u.fid, ST_Distance(u.geom::geography, s.line::geography) as m
               from tmp_unnamed u, shore s)
  select case when m < 500 then '< 500m (on coast)'
              when m < 2000 then '500m – 2km'
              when m < 10000 then '2 – 10km'
              when m < 50000 then '10 – 50km (inland)'
              else '> 50km (deep inland)'
         end b, count(*)
    from d group by b order by min(m);
""", (shore,))
for r in cur.fetchall():
    print(f"  {r[0]:30} {r[1]:>4}")

print("\n─── 3. SIZE ───")
cur.execute("""
  select case when area_m2 < 100 then '1: < 100 m²'
              when area_m2 < 500 then '2: 100 – 500'
              when area_m2 < 1000 then '3: 500 – 1k'
              when area_m2 < 5000 then '4: 1k – 5k'
              when area_m2 < 10000 then '5: 5k – 10k'
              when area_m2 < 50000 then '6: 10k – 50k'
              when area_m2 < 200000 then '7: 50k – 200k'
              else '8: > 200k'
         end b, count(*)
    from tmp_unnamed group by b order by b;
""")
for r in cur.fetchall():
    print(f"  {r[0]:18} {r[1]:>4}")

print("\n─── 4. JURISDICTIONAL ───")
cur.execute("""
  with latest as (
    select distinct on (type, id) type, id, governing_level
      from public.osm_landing
     where geom_full is not null
     order by type, id, fetched_at desc
  )
  select coalesce(l.governing_level, '(none)'), count(*)
    from tmp_unnamed u
    left join latest l on u.source_id = 'osm/' || l.type || '/' || l.id::text
   group by 1 order by 2 desc;
""")
for r in cur.fetchall():
    print(f"  {r[0]:25} {r[1]:>4}")

print("\n─── 5. CPAD COVERAGE ───")
cur.execute("""
  select count(*) filter (where cpad_unit_name is not null) inside,
         count(*) filter (where cpad_unit_name is null) outside
    from tmp_unnamed;
""")
r = cur.fetchone()
print(f"  inside CPAD: {r[0]}")
print(f"  outside CPAD: {r[1]}")

cur.execute("""
  select cpad_unit_name, count(*) from tmp_unnamed
   where cpad_unit_name is not null
   group by 1 order by 2 desc limit 12;
""")
print("  Top CPAD parents:")
for r in cur.fetchall():
    print(f"    {r[0]:50} {r[1]:>3}")

print("\n─── 6. ISOLATION (distance to nearest NAMED active OSM beach) ───")
cur.execute("""
  with named as (
    select geom from public.arena
     where source_code='osm' and is_active=true and name is not null and trim(name) <> ''
  ),
  d as (
    select u.fid,
           (select ST_Distance(u.geom::geography, n.geom::geography)
              from named n order by u.geom <-> n.geom asc limit 1) as m
      from tmp_unnamed u
  )
  select case when m < 100 then '< 100m (adjacent)'
              when m < 500 then '100 – 500m'
              when m < 2000 then '500m – 2km'
              when m < 10000 then '2 – 10km'
              else '> 10km (truly isolated)'
         end b, count(*)
    from d group by b order by min(m);
""")
for r in cur.fetchall():
    print(f"  {r[0]:30} {r[1]:>4}")

print("\n─── 7. OTHER OSM TAGS (excluding natural,name) ───")
cur.execute("""
  with t as (
    select kv.key
      from tmp_unnamed u
      join public.osm_landing l on 'osm/' || l.type || '/' || l.id::text = u.source_id
      cross join lateral jsonb_each_text(l.tags) kv
     where kv.key not in ('natural','name')
  )
  select key, count(*) from t group by 1 order by 2 desc limit 20;
""")
for r in cur.fetchall():
    print(f"  {r[0]:25} {r[1]:>4}")

print("\n─── 8. NEAREST PLACE CENTROID ───")
cur.execute("""
  with d as (
    select u.fid,
           (select ST_Distance(u.geom::geography, ST_Centroid(j.geom)::geography)
              from public.jurisdictions j order by u.geom <-> j.geom asc limit 1) as m
      from tmp_unnamed u
  )
  select case when m is null then 'no place nearby'
              when m < 1000 then '< 1km'
              when m < 5000 then '1 – 5km'
              when m < 20000 then '5 – 20km'
              else '> 20km (remote)'
         end b, count(*)
    from d group by b order by min(m) nulls last;
""")
for r in cur.fetchall():
    print(f"  {r[0]:30} {r[1]:>4}")
