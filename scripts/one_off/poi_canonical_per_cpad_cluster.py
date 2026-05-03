"""A2: pick canonical POI per (group_id, cpad_unit_id) cluster within multi-POI groups.

Within each polygon group, cluster POIs by their own cpad_unit_id (NULL is its own
cluster). Within each cluster, rank by:
  (sim to cpad_unit_name DESC, sim to polygon-leader name DESC, has_address DESC, fid ASC)
Keep rank 1 active; mark rank > 1 inactive with 'secondary_in_cpad/<keeper_fid>'.

POIs in different CPAD clusters within the same group stay active — they represent
distinct beaches that happen to fall within one OSM polygon's extent.
"""
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

conn = psycopg2.connect(**PG); cur = conn.cursor()
conn.autocommit = False

# Show pickers per cluster
cur.execute("""
  with multi_groups as (
    select group_id from public.arena
     where is_active = true and source_code = 'poi'
     group by group_id having count(*) >= 2
  ),
  ranked as (
    select poi.fid, poi.group_id, poi.cpad_unit_id,
           poi.name as poi_name, gleader.name as poly_name, cu.unit_name as cpad_name,
           similarity(lower(coalesce(poi.name,'')), lower(coalesce(cu.unit_name,'')))::numeric(3,2) sim_cpad,
           similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,'')))::numeric(3,2) sim_poly,
           (poi.address is not null) has_addr,
           row_number() over (
             partition by poi.group_id, poi.cpad_unit_id
             order by similarity(lower(coalesce(poi.name,'')), lower(coalesce(cu.unit_name,''))) desc nulls last,
                      similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))) desc nulls last,
                      (poi.address is not null) desc,
                      poi.fid asc
           ) rk,
           count(*) over (partition by poi.group_id, poi.cpad_unit_id) cluster_size
      from public.arena poi
      join multi_groups m on m.group_id = poi.group_id
      join public.arena gleader on gleader.fid = poi.group_id
      left join public.cpad_units cu on cu.unit_id = poi.cpad_unit_id
     where poi.source_code = 'poi' and poi.is_active = true
  )
  select group_id, cpad_unit_id, cpad_name, cluster_size, rk,
         fid, poi_name, sim_cpad, sim_poly, has_addr, poly_name
    from ranked
   where group_id in (
     select group_id from public.arena where is_active=true and source_code='poi'
      group by group_id having count(*) >= 4
   )
   order by group_id, cpad_unit_id nulls last, rk
   limit 60;
""")
print("Sample (multi-POI groups with ≥4 POIs only, showing CPAD clustering):\n")
last_g, last_c = None, None
for r in cur.fetchall():
    gid, cid, cname, csize, rk, fid, name, sc, sp, addr, poly_name = r
    if gid != last_g:
        print(f"\n── group {gid} (poly='{(poly_name or '')[:30]}') ──")
        last_g = gid; last_c = None
    if cid != last_c:
        print(f"   CPAD cluster {cid or 'NULL'} ('{(cname or '-')[:35]}') — {csize} POIs:")
        last_c = cid
    star = '★' if rk == 1 else ' '
    a = '✓' if addr else '-'
    print(f"     {star} rk={rk}  fid={fid:<6} sim_cpad={sc} sim_poly={sp} addr={a}  '{(name or '')[:35]}'")

# Count
cur.execute("""
  with multi_groups as (
    select group_id from public.arena
     where is_active=true and source_code='poi'
     group by group_id having count(*) >= 2
  ),
  ranked as (
    select poi.fid, poi.group_id, poi.cpad_unit_id,
           row_number() over (
             partition by poi.group_id, poi.cpad_unit_id
             order by similarity(lower(coalesce(poi.name,'')), lower(coalesce(cu.unit_name,''))) desc nulls last,
                      similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))) desc nulls last,
                      (poi.address is not null) desc,
                      poi.fid asc
           ) rk
      from public.arena poi
      join multi_groups m on m.group_id = poi.group_id
      join public.arena gleader on gleader.fid = poi.group_id
      left join public.cpad_units cu on cu.unit_id = poi.cpad_unit_id
     where poi.source_code='poi' and poi.is_active=true
  )
  select count(*) from ranked where rk > 1;
""")
n_to_flip = cur.fetchone()[0]
print(f"\nWould flip {n_to_flip} secondary POIs (rk > 1 within their group×cpad cluster).")

if APPLY:
    cur.execute("""
      with multi_groups as (
        select group_id from public.arena
         where is_active=true and source_code='poi'
         group by group_id having count(*) >= 2
      ),
      ranked as (
        select poi.fid, poi.group_id, poi.cpad_unit_id,
               first_value(poi.fid) over (
                 partition by poi.group_id, poi.cpad_unit_id
                 order by similarity(lower(coalesce(poi.name,'')), lower(coalesce(cu.unit_name,''))) desc nulls last,
                          similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))) desc nulls last,
                          (poi.address is not null) desc,
                          poi.fid asc
               ) keeper_fid,
               row_number() over (
                 partition by poi.group_id, poi.cpad_unit_id
                 order by similarity(lower(coalesce(poi.name,'')), lower(coalesce(cu.unit_name,''))) desc nulls last,
                          similarity(lower(coalesce(poi.name,'')), lower(coalesce(gleader.name,''))) desc nulls last,
                          (poi.address is not null) desc,
                          poi.fid asc
               ) rk
          from public.arena poi
          join multi_groups m on m.group_id = poi.group_id
          join public.arena gleader on gleader.fid = poi.group_id
          left join public.cpad_units cu on cu.unit_id = poi.cpad_unit_id
         where poi.source_code='poi' and poi.is_active=true
      )
      update public.arena a
         set is_active = false,
             inactive_reason = 'secondary_in_cpad/' || r.keeper_fid::text
        from ranked r
       where a.fid = r.fid and r.rk > 1
         and not exists (
           select 1 from public.poi_landing pl
            where 'poi/' || pl.fid::text = a.source_id
              and pl.is_dog_beach_signal = true
         );
    """)
    print(f"\n[secondary_in_cpad] {cur.rowcount} flipped (dog_beach guard preserved).")
    conn.commit()

    cur.execute("""
      select source_code, count(*) filter (where is_active),
             count(*) filter (where not is_active)
        from public.arena group by 1 order by 1;
    """)
    print("\nfinal:")
    for r in cur.fetchall():
        print(f"  {r[0]}: active={r[1]} inactive={r[2]}")

    cur.execute("select count(distinct group_id) from public.arena where is_active=true;")
    print(f"distinct beach groups (active): {cur.fetchone()[0]}")
else:
    print("\n(dry-run only; rerun with --apply to commit)")
