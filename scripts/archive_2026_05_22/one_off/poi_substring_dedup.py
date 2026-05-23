"""POI prefix-substring dedup — same county, ≤300m, longer name's added suffix is beach-vocabulary."""
import os, sys, urllib.parse, psycopg2
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / "scripts" / "pipeline" / ".env")
POOLER = (ROOT / "supabase" / ".temp" / "pooler-url").read_text().strip()
p = urllib.parse.urlparse(POOLER)
PG = dict(host=p.hostname, port=p.port or 5432, user=p.username,
          password=os.environ["SUPABASE_DB_PASSWORD"],
          dbname=(p.path or "/postgres").lstrip("/"), sslmode="require")

APPLY = "--apply" in sys.argv

conn = psycopg2.connect(**PG)
cur = conn.cursor()

# Curly apostrophe U+2019 normalized to straight '
SQL = r"""
  with norm as (
    select fid, name, source_code, county_fips, geom, source_id,
           translate(lower(trim(name)), %s, '''') as nlc
      from public.arena
     where source_code='poi' and is_active=true
       and name is not null
  ),
  cands as (
    select s.fid as short_fid, s.name as short_name,
           l.fid as long_fid,  l.name as long_name,
           s.source_id as short_src_id,
           ST_Distance(s.geom::geography, l.geom::geography)::int as m,
           trim(substring(l.nlc from length(s.nlc)+1)) as added_suffix
      from norm s
      join norm l
        on s.fid <> l.fid
       and length(s.nlc) < length(l.nlc)
       and l.nlc like s.nlc || ' %%'
       and s.county_fips = l.county_fips
       and ST_DWithin(s.geom::geography, l.geom::geography, 300)
  ),
  filt as (
    select * from cands
     where added_suffix ~ '^\s*(state\s+beach|city\s+beach|county\s+beach|beach|cove)\s*$'
  ),
  pick as (
    select distinct on (short_fid)
           short_fid, short_name, long_fid, long_name, short_src_id, m, added_suffix
      from filt order by short_fid, m asc, long_fid asc
  )
  select * from pick;
"""
CURLY = "’"
cur.execute(SQL, (CURLY,))
pairs = cur.fetchall()
print(f"{len(pairs)} prefix-substring pairs to flip:")
for r in pairs:
    print(f"  fid={r[0]:<6} '{r[1]}' → keeper fid={r[2]} '{r[3]}' ({r[5]}m, +'{r[6]}')")

if APPLY and pairs:
    short_fids = [r[0] for r in pairs]
    long_fids  = {r[0]: r[2] for r in pairs}
    src_ids    = {r[0]: r[4] for r in pairs}

    flipped = 0
    for sf in short_fids:
        cur.execute("""
          update public.arena
             set is_active=false,
                 inactive_reason='likely_dup_of/' || %s::text
           where fid = %s
             and is_active=true
             and not exists (
               select 1 from public.poi_landing pl
                where 'poi/' || pl.fid::text = %s
                  and pl.is_dog_beach_signal=true
             )
           returning fid;
        """, (long_fids[sf], sf, src_ids[sf]))
        if cur.fetchone():
            flipped += 1
    conn.commit()
    print(f"\n[likely_dup_of/<long_fid>] {flipped} flipped")
elif not APPLY:
    print("\n(dry-run; rerun with --apply)")
