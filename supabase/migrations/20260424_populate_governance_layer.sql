-- Layer 1 direct-fill + 7 more Layer 2 governance populators (2026-04-24)
--
-- Follows the populate_from_cpad template. Each Layer 2 populator:
--   1. Finds best matching source row(s) per beach
--   2. Maps source fields to claimed_values jsonb
--   3. UPSERTs evidence rows into beach_enrichment_provenance
--   4. Returns count of evidence rows touched
--
-- After all populators run for a beach, call resolve_governance(fid) and
-- resolve_access(fid) to pick canonical and write back to staging.

-- ── Layer 1: direct-fill spatial truth columns (no provenance) ──────────────
-- Single source per column → no resolution needed. Writes directly to staging.

create or replace function public.populate_layer1_geographic(p_fid int default null)
returns int
language plpgsql
as $$
declare
  rows_updated int;
begin
  with state_match as (
    select s.fid, st.state_code
    from public.us_beach_points_staging s
    left join public.states st on st_contains(st.geom, s.geom::geometry)
    where (p_fid is null or s.fid = p_fid) and s.geom is not null
  ),
  county_match as (
    select s.fid, c.name as county_name, c.geoid as county_fips
    from public.us_beach_points_staging s
    left join public.counties c on st_contains(c.geom, s.geom::geometry)
    where (p_fid is null or s.fid = p_fid) and s.geom is not null
  ),
  place_match as (
    select s.fid, j.name as place_name, j.fips_place, j.place_type
    from public.us_beach_points_staging s
    left join public.jurisdictions j on st_contains(j.geom, s.geom::geometry)
    where (p_fid is null or s.fid = p_fid) and s.geom is not null
  )
  update public.us_beach_points_staging s
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
  'Layer 1 direct-fill: state_code, county_name/fips, place_name/fips/type via point-in-polygon spatial joins. Writes directly to staging — single deterministic source, no provenance needed.';

-- ── populate_from_jurisdictions ──────────────────────────────────────────────
-- TIGER Places governance signal: when point inside an incorporated city
-- (place_type starting with 'C'), claim governance type='city', name=place_name.
-- CDPs (U1/U2) and other types don't claim governance.

create or replace function public.populate_from_jurisdictions(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with insert_rows as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select s.fid, 'governance', 'tiger_places', 0.70,
           jsonb_build_object('type', 'city', 'name', j.name),
           now()
    from public.us_beach_points_staging s
    join public.jurisdictions j on st_contains(j.geom, s.geom::geometry)
    where (p_fid is null or s.fid = p_fid)
      and s.geom is not null
      and j.place_type like 'C%'   -- incorporated places only
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from insert_rows;
  return rows_touched;
end;
$$;

-- ── populate_from_csp_parks ──────────────────────────────────────────────────
-- CA State Parks: governance type='state' default, name=unit_name. When
-- subtype='Park Unit operated by other entity' the state default is weaker
-- (lower confidence) — park_operators populator overrides with the actual
-- operator. Inside-polygon match required.

create or replace function public.populate_from_csp_parks(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with best as (
    select distinct on (s.fid)
      s.fid,
      c.unit_name,
      c.subtype,
      st_area(c.geom::geography) as area_m2
    from public.us_beach_points_staging s
    join public.csp_parks c on st_contains(c.geom, s.geom::geometry)
    where (p_fid is null or s.fid = p_fid) and s.geom is not null
    order by s.fid, st_area(c.geom::geography) asc  -- smallest = most specific
  ),
  ins as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'governance', 'csp_parks',
      case when subtype = 'Park Unit operated by other entity' then 0.55  -- weak; defer to park_operators
           else 0.80 end,
      jsonb_build_object('type', 'state', 'name', unit_name),
      now()
    from best
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

-- ── populate_from_park_operators ─────────────────────────────────────────────
-- Override when a CSP park is operated by a city/county/etc. Joins
-- csp_parks (point-in-polygon) → park_operators (by unit_name=park_name).
-- High confidence: this is curated manual data.

create or replace function public.populate_from_park_operators(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with matches as (
    select distinct on (s.fid)
      s.fid,
      po.operator_jurisdiction,
      po.operator_body,
      po.notes
    from public.us_beach_points_staging s
    join public.csp_parks c on st_contains(c.geom, s.geom::geometry)
    join public.park_operators po on po.park_name = c.unit_name
    where (p_fid is null or s.fid = p_fid) and s.geom is not null
    order by s.fid, st_area(c.geom::geography) asc
  ),
  with_type as (
    select fid, operator_body, notes,
      case operator_jurisdiction
        when 'governing city'    then 'city'
        when 'governing county'  then 'county'
        when 'governing state'   then 'state'
        when 'governing federal' then 'federal'
        when 'governing private' then 'private'
        else                          'unknown'
      end as gov_type
    from matches
  ),
  ins as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, notes, updated_at)
    select fid, 'governance', 'park_operators', 0.95,
      jsonb_build_object('type', gov_type, 'name', operator_body),
      notes, now()
    from with_type
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          notes          = excluded.notes,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from ins;
  return rows_touched;
end;
$$;

-- ── populate_from_nps_places ─────────────────────────────────────────────────
-- National Park Service points (national, 2,334 rows). Multiple points per
-- park (visitor centers, landmarks). For each beach, find the nearest
-- nps_places point within 1 km — its park_full_name is the governing body.
-- type='federal'. Confidence 0.70 (proximity-based, no polygon).

create or replace function public.populate_from_nps_places(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with nps_geo as (
    select park_full_name,
           st_makepoint(longitude, latitude)::geography as geog
    from public.nps_places
    where latitude is not null and longitude is not null
  ),
  best as (
    select distinct on (s.fid)
      s.fid,
      n.park_full_name,
      st_distance(s.geom, n.geog) as dist_m
    from public.us_beach_points_staging s
    join nps_geo n on st_dwithin(n.geog, s.geom, 1000)
    where (p_fid is null or s.fid = p_fid) and s.geom is not null
    order by s.fid, st_distance(s.geom, n.geog) asc
  ),
  ins as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'governance', 'nps_places', 0.70,
      jsonb_build_object('type', 'federal', 'name', park_full_name),
      now()
    from best
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

-- ── populate_from_tribal_lands ───────────────────────────────────────────────
-- Federal tribal lands. Inside-polygon: type='tribal', name=lar_name.
-- High confidence: the polygon IS the boundary of tribal jurisdiction.

create or replace function public.populate_from_tribal_lands(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with best as (
    select distinct on (s.fid) s.fid, t.lar_name
    from public.us_beach_points_staging s
    join public.tribal_lands t on st_contains(t.geom, s.geom::geometry)
    where (p_fid is null or s.fid = p_fid) and s.geom is not null
    order by s.fid, st_area(t.geom::geography) asc
  ),
  ins as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'governance', 'tribal_lands', 0.85,
      jsonb_build_object('type', 'tribal', 'name', lar_name),
      now()
    from best
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

-- ── populate_from_military_bases ─────────────────────────────────────────────
-- DoD installations. Inside-polygon: governance type='federal', name=site_name;
-- access status='private' (default). Confidence INTENTIONALLY LOW (0.65)
-- so CPAD/CCC override when they have positive coverage (Coronado pattern).

create or replace function public.populate_from_military_bases(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with matches as (
    select distinct on (s.fid) s.fid, m.site_name, m.component
    from public.us_beach_points_staging s
    join public.military_bases m on st_contains(m.geom, s.geom::geometry)
    where (p_fid is null or s.fid = p_fid) and s.geom is not null
    order by s.fid, m.site_name
  ),
  upsert_governance as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, notes, updated_at)
    select fid, 'governance', 'military_bases', 0.65,
      jsonb_build_object('type', 'federal', 'name', site_name),
      'component=' || coalesce(component, ''), now()
    from matches
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          notes          = excluded.notes,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  ),
  upsert_access as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, notes, updated_at)
    select fid, 'access', 'military_bases', 0.65,
      jsonb_build_object('status', 'private'),
      'on military installation: ' || site_name, now()
    from matches
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          notes          = excluded.notes,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from upsert_governance union all select * from upsert_access
  ) _;
  return rows_touched;
end;
$$;

-- ── populate_from_private_land_zones ─────────────────────────────────────────
-- Bbox-based private exclusions (Del Monte Forest etc.). Inside bbox →
-- access status='private'. Curated manual override; high confidence.

create or replace function public.populate_from_private_land_zones(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with matches as (
    select s.fid, plz.name, plz.reason
    from public.us_beach_points_staging s
    join public.private_land_zones plz
      on plz.active = true
     and s.latitude  between plz.min_lat and plz.max_lat
     and s.longitude between plz.min_lon and plz.max_lon
    where (p_fid is null or s.fid = p_fid)
  ),
  ins as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, notes, updated_at)
    select fid, 'access', 'plz', 0.95,
      jsonb_build_object('status', 'private'),
      coalesce(name, '') || coalesce(' — ' || reason, ''),
      now()
    from matches
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          notes          = excluded.notes,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from ins;
  return rows_touched;
end;
$$;

-- Comments
comment on function public.populate_from_jurisdictions(int)     is 'Layer 2: TIGER Places → governance type=city when point inside incorporated (C*) polygon.';
comment on function public.populate_from_csp_parks(int)         is 'Layer 2: CA State Parks → governance type=state. Lower confidence when subtype="operated by other entity".';
comment on function public.populate_from_park_operators(int)    is 'Layer 2: park_operators override (state-leased CA beaches operated by city/county). High confidence (curated).';
comment on function public.populate_from_nps_places(int)        is 'Layer 2: nearest NPS point within 1km → governance type=federal, name=park_full_name.';
comment on function public.populate_from_tribal_lands(int)      is 'Layer 2: tribal_lands inside-polygon → governance type=tribal, name=lar_name.';
comment on function public.populate_from_military_bases(int)    is 'Layer 2: DoD installations inside-polygon → governance type=federal, access=private. Low confidence so CPAD/CCC override (Coronado pattern).';
comment on function public.populate_from_private_land_zones(int) is 'Layer 2: PLZ bbox match → access=private. High confidence (curated bbox exclusions like Del Monte Forest).';
