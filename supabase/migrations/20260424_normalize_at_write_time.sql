-- Normalize governing-body name at populator write time (2026-04-24)
--
-- Until now, canonical_agency_name was applied only at policy-lookup
-- time inside populate_dogs_from_governing_body. The raw CPAD/TIGER/CSP
-- names were still going into beach_enrichment_provenance.claimed_values
-- and ultimately into locations_stage.governing_body_name.
--
-- Effect on the resolver: agreement boost is keyed on EXACT
-- claimed_values jsonb match. Without write-time normalization:
--   CPAD says       {type:'city', name:'Newport Beach, City of'}
--   TIGER says      {type:'city', name:'Newport Beach'}
--   Name signal     {type:'city', name:'Newport Beach, City of'}
-- These don't agree → no boost. With normalization:
--   All three become {type:'city', name:'Newport Beach, City of'}
-- → boost fires.
--
-- Also: state-park-unit names like "Sonoma Coast SP" now stored as
-- "California Department of Parks and Recreation" — the AGENCY name,
-- not the UNIT. Original unit info is still queryable via cpad_units
-- if needed.

-- ── populate_from_cpad ──────────────────────────────────────────────────────
create or replace function public.populate_from_cpad(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with candidates as (
    select s.fid, c.unit_name, c.mng_ag_lev, c.mng_agncy, c.access_typ,
      st_contains(c.geom, s.geom::geometry)               as inside,
      st_distance(s.geom, c.geom::geography)              as dist_m,
      st_area(c.geom::geography)                          as area_m2,
      cardinality(public.shared_name_tokens(s.display_name, c.unit_name)) > 0 as name_match
    from public.locations_stage s
    join public.cpad_units c on st_dwithin(c.geom::geography, s.geom, 500)
    where (p_fid is null or s.fid = p_fid) and s.geom is not null
  ),
  with_conf as (
    select *,
      case
        when inside     and name_match                   then 0.95
        when not inside and dist_m <= 100 and name_match then 0.85
        when inside     and not name_match               then 0.75
        when not inside and dist_m <= 500 and name_match then 0.65
        when not inside and dist_m <= 100 and not name_match then 0.50
        else null
      end as confidence
    from candidates
  ),
  scored as (select * from with_conf where confidence is not null),
  ranked as (
    select *, row_number() over (partition by fid
      order by confidence desc, area_m2 asc) as rnk
    from scored
  ),
  best as (select * from ranked where rnk = 1),
  with_type as (
    select *,
      case mng_ag_lev
        when 'City'                    then 'city'
        when 'County'                  then 'county'
        when 'State'                   then 'state'
        when 'Federal'                 then 'federal'
        when 'Tribal'                  then 'tribal'
        when 'Special District'        then 'special_district'
        when 'Non Profit'              then 'nonprofit'
        when 'Joint'                   then 'joint'
        when 'Home Owners Association' then 'private'
        when 'Private'                 then 'private'
        else                                'unknown'
      end as gov_type,
      case access_typ
        when 'Open Access'       then 'public'
        when 'Restricted Access' then 'restricted'
        when 'No Public Access'  then 'private'
        when 'Unknown Access'    then 'unknown'
        else                          null
      end as access_status
    from best
  ),
  upsert_governance as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'governance', 'cpad', confidence,
      jsonb_build_object(
        'type', gov_type,
        'name', public.canonical_agency_name(gov_type, mng_agncy)
      ),
      now()
    from with_type
    where mng_ag_lev is not null
    on conflict (fid, field_group, source) do update
      set confidence = excluded.confidence, claimed_values = excluded.claimed_values,
          updated_at = now(), is_canonical = false
    returning 1
  ),
  upsert_access as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'access', 'cpad', confidence,
      jsonb_build_object('status', access_status), now()
    from with_type
    where access_status is not null
    on conflict (fid, field_group, source) do update
      set confidence = excluded.confidence, claimed_values = excluded.claimed_values,
          updated_at = now(), is_canonical = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from upsert_governance union all select * from upsert_access
  ) _;
  return rows_touched;
end;
$$;

-- ── populate_from_jurisdictions ─────────────────────────────────────────────
create or replace function public.populate_from_jurisdictions(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
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
      jsonb_build_object('type', 'city',
        'name', public.canonical_agency_name('city', name)),
      now()
    from contained where place_type like 'C%'
    on conflict (fid, field_group, source) do update
      set confidence = excluded.confidence, claimed_values = excluded.claimed_values,
          updated_at = now(), is_canonical = false
    returning fid
  ),
  buffer_only as (
    select distinct on (s.fid) s.fid, j.name, j.place_type
    from public.locations_stage s
    join public.jurisdictions j on st_dwithin(j.geom::geography, s.geom, 200)
    where (p_fid is null or s.fid = p_fid) and s.geom is not null
      and not exists (select 1 from public.jurisdictions j2 where st_contains(j2.geom, s.geom::geometry))
    order by s.fid, st_distance(j.geom::geography, s.geom)
  ),
  ins2 as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'governance', 'tiger_places', 0.55,
      jsonb_build_object('type', 'city',
        'name', public.canonical_agency_name('city', name)),
      now()
    from buffer_only where place_type like 'C%'
    on conflict (fid, field_group, source) do update
      set confidence = excluded.confidence, claimed_values = excluded.claimed_values,
          updated_at = now(), is_canonical = false
    returning fid
  )
  select (select count(*) from ins1) + (select count(*) from ins2) into rows_touched;
  return rows_touched;
end;
$$;

-- ── populate_from_csp_parks ─────────────────────────────────────────────────
create or replace function public.populate_from_csp_parks(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with best as (
    select distinct on (s.fid) s.fid, c.unit_name, c.subtype,
           st_area(c.geom::geography) as area_m2
    from public.locations_stage s
    join public.csp_parks c on st_contains(c.geom, s.geom::geometry)
    where (p_fid is null or s.fid = p_fid) and s.geom is not null
    order by s.fid, st_area(c.geom::geography) asc
  ),
  ins as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'governance', 'csp_parks',
      case when subtype = 'Park Unit operated by other entity' then 0.55 else 0.80 end,
      jsonb_build_object('type', 'state',
        'name', public.canonical_agency_name('state', unit_name)),
      now()
    from best
    on conflict (fid, field_group, source) do update
      set confidence = excluded.confidence, claimed_values = excluded.claimed_values,
          updated_at = now(), is_canonical = false
    returning 1
  )
  select count(*) into rows_touched from ins;
  return rows_touched;
end;
$$;

-- ── populate_from_park_operators ────────────────────────────────────────────
create or replace function public.populate_from_park_operators(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with matches as (
    select distinct on (s.fid) s.fid, po.operator_jurisdiction, po.operator_body, po.notes
    from public.locations_stage s
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
      jsonb_build_object('type', gov_type,
        'name', public.canonical_agency_name(gov_type, operator_body)),
      notes, now()
    from with_type
    on conflict (fid, field_group, source) do update
      set confidence = excluded.confidence, claimed_values = excluded.claimed_values,
          notes = excluded.notes, updated_at = now(), is_canonical = false
    returning 1
  )
  select count(*) into rows_touched from ins;
  return rows_touched;
end;
$$;

-- ── populate_from_nps_places ────────────────────────────────────────────────
create or replace function public.populate_from_nps_places(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with nps_geo as (
    select park_full_name, st_makepoint(longitude, latitude)::geography as geog
    from public.nps_places where latitude is not null and longitude is not null
  ),
  best as (
    select distinct on (s.fid) s.fid, n.park_full_name,
           st_distance(s.geom, n.geog) as dist_m
    from public.locations_stage s
    join nps_geo n on st_dwithin(n.geog, s.geom, 1000)
    where (p_fid is null or s.fid = p_fid) and s.geom is not null
    order by s.fid, st_distance(s.geom, n.geog) asc
  ),
  ins as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'governance', 'nps_places', 0.70,
      jsonb_build_object('type', 'federal',
        'name', public.canonical_agency_name('federal', park_full_name)),
      now()
    from best
    on conflict (fid, field_group, source) do update
      set confidence = excluded.confidence, claimed_values = excluded.claimed_values,
          updated_at = now(), is_canonical = false
    returning 1
  )
  select count(*) into rows_touched from ins;
  return rows_touched;
end;
$$;

-- ── populate_from_tribal_lands ──────────────────────────────────────────────
create or replace function public.populate_from_tribal_lands(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with best as (
    select distinct on (s.fid) s.fid, t.lar_name
    from public.locations_stage s
    join public.tribal_lands t on st_contains(t.geom, s.geom::geometry)
    where (p_fid is null or s.fid = p_fid) and s.geom is not null
    order by s.fid, st_area(t.geom::geography) asc
  ),
  ins as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'governance', 'tribal_lands', 0.85,
      jsonb_build_object('type', 'tribal',
        'name', public.canonical_agency_name('tribal', lar_name)),
      now()
    from best
    on conflict (fid, field_group, source) do update
      set confidence = excluded.confidence, claimed_values = excluded.claimed_values,
          updated_at = now(), is_canonical = false
    returning 1
  )
  select count(*) into rows_touched from ins;
  return rows_touched;
end;
$$;

-- ── populate_from_military_bases ────────────────────────────────────────────
create or replace function public.populate_from_military_bases(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with matches as (
    select distinct on (s.fid) s.fid, m.site_name, m.component
    from public.locations_stage s
    join public.military_bases m on st_contains(m.geom, s.geom::geometry)
    where (p_fid is null or s.fid = p_fid) and s.geom is not null
    order by s.fid, m.site_name
  ),
  upsert_governance as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, notes, updated_at)
    select fid, 'governance', 'military_bases', 0.65,
      jsonb_build_object('type', 'federal',
        'name', public.canonical_agency_name('federal', site_name)),
      'component=' || coalesce(component, ''), now()
    from matches
    on conflict (fid, field_group, source) do update
      set confidence = excluded.confidence, claimed_values = excluded.claimed_values,
          notes = excluded.notes, updated_at = now(), is_canonical = false
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
      set confidence = excluded.confidence, claimed_values = excluded.claimed_values,
          notes = excluded.notes, updated_at = now(), is_canonical = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from upsert_governance union all select * from upsert_access
  ) _;
  return rows_touched;
end;
$$;

-- ── populate_governance_from_name ───────────────────────────────────────────
-- The name signal already constructs its name via TIGER place_name lookup
-- in the canonical "X, City of" / "X, County of" form. We still wrap it
-- through canonical_agency_name for safety + future alias matching.
create or replace function public.populate_governance_from_name(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with classified as (
    select fid, display_name, place_name, county_name,
      case
        when display_name ~* '\m(state\s+(beach|park|recreation\s+area|marine|reserve|historic))\M' then 'state'
        when display_name ~* '\m(srs|sra|smr|smca)\M' then 'state'
        when display_name ~* '\m(county\s+(beach|park|recreation))\M' then 'county'
        when display_name ~* '\m(city\s+(beach|park|recreation))\M'   then 'city'
        when display_name ~* '\m(national\s+(park|seashore|monument|recreation|preserve|memorial))\M' then 'federal'
        when display_name ~* '\m(nps|us\s+army|navy|coast\s+guard)\M' then 'federal'
        else null
      end as gov_type
    from public.locations_stage
    where (p_fid is null or fid = p_fid)
  ),
  with_name as (
    select fid, display_name, gov_type,
      case gov_type
        when 'city'   then case when place_name is not null then place_name end
        when 'county' then case when county_name is not null then county_name end
        else null
      end as raw_name
    from classified
    where gov_type is not null
  ),
  ins as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at, notes)
    select fid, 'governance', 'name', 0.65,
      jsonb_build_object('type', gov_type,
        'name', case when raw_name is null then null
                     else public.canonical_agency_name(gov_type, raw_name) end),
      now(),
      'name-keyword signal from display_name: ' || display_name
    from with_name
    on conflict (fid, field_group, source) do update
      set confidence = excluded.confidence, claimed_values = excluded.claimed_values,
          notes = excluded.notes, updated_at = now(), is_canonical = false
    returning 1
  )
  select count(*) into rows_touched from ins;
  return rows_touched;
end;
$$;
