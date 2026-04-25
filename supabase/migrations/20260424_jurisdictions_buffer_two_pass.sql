-- Faster two-pass place match (2026-04-24)
--
-- Previous attempt (lateral subquery with ORDER BY ST_Contains+ST_Distance)
-- was too slow at 861-row scale — Cloudflare 524 proxy timeout.
--
-- Strategy: two passes.
--   1. ST_Contains pass (indexed, fast) — fills the 90%+ that are
--      truly inside a polygon
--   2. Buffer fallback (200m) only for rows still null after pass 1 —
--      smaller subset, so the more expensive ordering only runs on it

create or replace function public.populate_layer1_geographic(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_updated int;
begin
  -- State + county (always ST_Contains — these polygons are big, low risk)
  with state_match as (
    select s.fid, st.state_code
    from public.locations_stage s
    left join public.states st on st_contains(st.geom, s.geom::geometry)
    where (p_fid is null or s.fid = p_fid) and s.geom is not null
  ),
  county_match as (
    select s.fid, c.name as county_name, c.geoid as county_fips
    from public.locations_stage s
    left join public.counties c on st_contains(c.geom, s.geom::geometry)
    where (p_fid is null or s.fid = p_fid) and s.geom is not null
  ),
  -- PASS 1: place via ST_Contains (fast)
  place_contained as (
    select s.fid, j.name as place_name, j.fips_place, j.place_type
    from public.locations_stage s
    left join public.jurisdictions j on st_contains(j.geom, s.geom::geometry)
    where (p_fid is null or s.fid = p_fid) and s.geom is not null
  )
  update public.locations_stage s
    set state_code  = sm.state_code,
        county_name = cm.county_name,
        county_fips = cm.county_fips,
        place_name  = pc.place_name,
        place_fips  = pc.fips_place,
        place_type  = pc.place_type
    from state_match sm, county_match cm, place_contained pc
   where s.fid = sm.fid and s.fid = cm.fid and s.fid = pc.fid;

  get diagnostics rows_updated = row_count;

  -- PASS 2: buffer fallback for rows still null on place_name
  update public.locations_stage s
    set place_name = sub.name,
        place_fips = sub.fips_place,
        place_type = sub.place_type
    from (
      select distinct on (s2.fid)
        s2.fid, j.name, j.fips_place, j.place_type
      from public.locations_stage s2
      join public.jurisdictions j
        on st_dwithin(j.geom::geography, s2.geom, 200)
      where s2.place_name is null
        and s2.geom is not null
        and (p_fid is null or s2.fid = p_fid)
      order by s2.fid, st_distance(j.geom::geography, s2.geom)
    ) sub
    where s.fid = sub.fid;

  return rows_updated;
end;
$$;

-- Apply same simplification to populate_from_jurisdictions: do the
-- ST_Contains case first as a fast bulk emit, then buffer fallback for
-- non-contained rows.

create or replace function public.populate_from_jurisdictions(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  -- PASS 1: ST_Contains — confidence 0.70
  with contained as (
    select s.fid, j.name, j.place_type
    from public.locations_stage s
    join public.jurisdictions j on st_contains(j.geom, s.geom::geometry)
    where (p_fid is null or s.fid = p_fid) and s.geom is not null
  ),
  ins1 as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'governance', 'tiger_places', 0.70,
      jsonb_build_object('type', 'city', 'name', name),
      now()
    from contained
    where place_type like 'C%'
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning fid
  ),
  -- PASS 2: buffer fallback — confidence 0.55, only for fids not in pass 1
  buffer_only as (
    select distinct on (s.fid) s.fid, j.name, j.place_type
    from public.locations_stage s
    join public.jurisdictions j
      on st_dwithin(j.geom::geography, s.geom, 200)
    where (p_fid is null or s.fid = p_fid)
      and s.geom is not null
      and not exists (
        select 1 from public.jurisdictions j2
        where st_contains(j2.geom, s.geom::geometry)
      )
    order by s.fid, st_distance(j.geom::geography, s.geom)
  ),
  ins2 as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'governance', 'tiger_places', 0.55,
      jsonb_build_object('type', 'city', 'name', name),
      now()
    from buffer_only
    where place_type like 'C%'
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning fid
  )
  select (select count(*) from ins1) + (select count(*) from ins2)
    into rows_touched;
  return rows_touched;
end;
$$;
