-- Expand TIGER places match to 200m buffer (2026-04-24)
--
-- Coastal beach points often sit just outside city polygons (TIGER
-- boundaries end at high water; beach is on the water side; or geocoding
-- precision puts the point 50-200m offshore). Strict point-in-polygon
-- misses those.
--
-- New rule: prefer containment, fall back to nearest within 200m.
-- 200m matches the CCC point-to-point buffer convention
-- (memory project_buffer_convention).

-- ── Layer 1: place_name/fips/type direct-fill ───────────────────────────────
create or replace function public.populate_layer1_geographic(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_updated int;
begin
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
  -- Place match: contained > nearest within 200m. Use lateral to pick
  -- the best candidate per beach.
  place_match as (
    select s.fid, best.name as place_name, best.fips_place, best.place_type
    from public.locations_stage s
    left join lateral (
      select j.name, j.fips_place, j.place_type,
             st_contains(j.geom, s.geom::geometry) as contained,
             st_distance(j.geom::geography, s.geom) as dist_m
      from public.jurisdictions j
      where st_dwithin(j.geom::geography, s.geom, 200)
      order by st_contains(j.geom, s.geom::geometry) desc,
               st_distance(j.geom::geography, s.geom) asc
      limit 1
    ) best on true
    where (p_fid is null or s.fid = p_fid) and s.geom is not null
  )
  update public.locations_stage s
    set state_code  = sm.state_code,
        county_name = cm.county_name,
        county_fips = cm.county_fips,
        place_name  = pm.place_name,
        place_fips  = pm.fips_place,
        place_type  = pm.place_type
    from state_match sm, county_match cm, place_match pm
   where s.fid = sm.fid and s.fid = cm.fid and s.fid = pm.fid;

  get diagnostics rows_updated = row_count;
  return rows_updated;
end;
$$;

comment on function public.populate_layer1_geographic(int) is
  'Layer 1 direct-fill: state, county, place. Place uses contained-first then nearest-within-200m fallback so beach points just outside city polygons (TIGER boundary at high water + geocoding precision) still get place_name/fips/type.';

-- ── Layer 2: jurisdictions populator for governance ─────────────────────────
-- Same buffer expansion. Confidence: 0.70 contained, 0.55 buffer-only.

create or replace function public.populate_from_jurisdictions(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with best_place as (
    select s.fid, j.name, j.place_type,
           st_contains(j.geom, s.geom::geometry) as contained
    from public.locations_stage s
    join lateral (
      select j2.name, j2.place_type, j2.geom
      from public.jurisdictions j2
      where st_dwithin(j2.geom::geography, s.geom, 200)
      order by st_contains(j2.geom, s.geom::geometry) desc,
               st_distance(j2.geom::geography, s.geom) asc
      limit 1
    ) j on true
    where (p_fid is null or s.fid = p_fid)
      and s.geom is not null
  ),
  ins as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'governance', 'tiger_places',
      case when contained then 0.70 else 0.55 end,
      jsonb_build_object('type', 'city', 'name', name),
      now()
    from best_place
    where place_type like 'C%'    -- incorporated places only
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from ins;
  return rows_touched;
end;
$$;

comment on function public.populate_from_jurisdictions(int) is
  'Layer 2: TIGER Places governance signal. Contained (0.70) or within-200m buffer (0.55), nearest wins. Only emits for incorporated places (place_type LIKE C%).';
