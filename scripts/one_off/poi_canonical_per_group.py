"""Pick canonical POI per polygon group, prefer cpad-name-sim then poly-name-sim."""
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

import sys
APPLY = "--apply" in sys.argv

conn = psycopg2.connect(**PG); cur = conn.cursor()
conn.autocommit = False

# Picker: for each polygon group with 2+ active POIs, rank POIs by
#   (sim to CPAD name DESC, sim to polygon-leader name DESC, has_address DESC, fid ASC)
# Keep rank 1, mark rest inactive.
cur.execute("""
  with multi_groups as (
    select group_id from public.arena
     where is_active = true and source_code = 'poi'
     group by group_id
    having count(*) >= 2
  ),
  ranked as (
    select poi.fid as poi_fid,
           poi.group_id,
           poi.name as poi_name,
           gleader.name as poly_name,
           cu.unit_name as cpad_name,
           similarity(lower(coalesce(poi.name,'')), lower(coalesce(cu.unit_name,'')))::numeric(3,2) sim_cpad,
           similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,'')))::numeric(3,2) sim_poly,
           (poi.address is not null) has_addr,
           row_number() over (
             partition by poi.group_id
             order by similarity(lower(coalesce(poi.name,'')), lower(coalesce(cu.unit_name,''))) desc nulls last,
                      similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))) desc nulls last,
                      (poi.address is not null) desc,
                      poi.fid asc
           ) as rk
      from public.arena poi
      join multi_groups m on m.group_id = poi.group_id
      join public.arena gleader on gleader.fid = poi.group_id
      left join public.cpad_units cu on cu.unit_id = poi.cpad_unit_id
     where poi.source_code = 'poi' and poi.is_active = true
  )
  select * from ranked order by group_id, rk
   limit 80;
""")
print("Sample picker output (first 80 rows):")
last_g = None
for r in cur.fetchall():
    poi_fid, gid, poi_name, poly_name, cpad_name, sim_c, sim_p, has_addr, rk = r
    if gid != last_g:
        print(f"\n  ── group {gid} ('{(poly_name or '')[:30]}', cpad='{(cpad_name or '-')[:30]}') ──")
        last_g = gid
    star = '★' if rk == 1 else ' '
    a = '✓' if has_addr else '-'
    print(f"   {star} rk={rk}  fid={poi_fid:<6} sim_cpad={sim_c} sim_poly={sim_p} addr={a}  '{(poi_name or '')[:40]}'")

# Count how many would be flipped
cur.execute("""
  with multi_groups as (
    select group_id from public.arena
     where is_active = true and source_code = 'poi' group by group_id having count(*) >= 2
  ),
  ranked as (
    select poi.fid,
           row_number() over (
             partition by poi.group_id
             order by similarity(lower(coalesce(poi.name,'')), lower(coalesce(cu.unit_name,''))) desc nulls last,
                      similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))) desc nulls last,
                      (poi.address is not null) desc,
                      poi.fid asc
           ) as rk
      from public.arena poi
      join multi_groups m on m.group_id = poi.group_id
      join public.arena gleader on gleader.fid = poi.group_id
      left join public.cpad_units cu on cu.unit_id = poi.cpad_unit_id
     where poi.source_code = 'poi' and poi.is_active = true
  )
  select count(*) from ranked where rk > 1;
""")
n_to_flip = cur.fetchone()[0]
print(f"\nWould flip {n_to_flip} secondary POIs (rank > 1 in their group).")

if APPLY:
    cur.execute("""
      with multi_groups as (
        select group_id from public.arena
         where is_active = true and source_code = 'poi' group by group_id having count(*) >= 2
      ),
      ranked as (
        select poi.fid, poi.group_id,
               row_number() over (
                 partition by poi.group_id
                 order by similarity(lower(coalesce(poi.name,'')), lower(coalesce(cu.unit_name,''))) desc nulls last,
                          similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))) desc nulls last,
                          (poi.address is not null) desc,
                          poi.fid asc
               ) as rk
          from public.arena poi
          join multi_groups m on m.group_id = poi.group_id
          join public.arena gleader on gleader.fid = poi.group_id
          left join public.cpad_units cu on cu.unit_id = poi.cpad_unit_id
         where poi.source_code = 'poi' and poi.is_active = true
      )
      update public.arena a
         set is_active = false,
             inactive_reason = 'secondary_in_group/' || r.group_id::text
        from ranked r
       where a.fid = r.fid and r.rk > 1
         and not exists (
           select 1 from public.poi_landing pl
            where 'poi/' || pl.fid::text = a.source_id
              and pl.is_dog_beach_signal = true
         );
    """)
    print(f"[secondary_in_group] {cur.rowcount} flipped (dog_beach guard preserved).")
    conn.commit()

    cur.execute("""
      select source_code, count(*) filter (where is_active),
             count(*) filter (where not is_active)
        from public.arena group by 1 order by 1;
    """)
    print("\nfinal:")
    for r in cur.fetchall():
        print(f"  {r[0]}: active={r[1]} inactive={r[2]}")
else:
    print("\n(dry-run only; rerun with --apply to commit)")
