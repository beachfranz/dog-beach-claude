-- Rename us_beach_points_staging → locations_stage (2026-04-24)
--
-- 1. Rename table us_beach_points_staging → locations_stage
-- 2. Rename ancillary objects (indexes/constraints/triggers) for consistency
-- 3. Recreate the 18 functions that reference the table
--    with us_beach_points_staging swapped for locations_stage in their bodies
--
-- FK from beach_enrichment_provenance updates automatically via Postgres
-- ALTER TABLE RENAME (FKs follow OIDs not names).

begin;

alter table public.us_beach_points_staging rename to locations_stage;

alter index if exists ubps_geom_idx           rename to locstg_geom_idx;
alter index if exists ubps_state_idx          rename to locstg_state_idx;
alter index if exists ubps_county_fips_idx    rename to locstg_county_fips_idx;
alter index if exists ubps_place_fips_idx     rename to locstg_place_fips_idx;
alter index if exists ubps_active_idx         rename to locstg_active_idx;
alter index if exists ubps_review_status_idx  rename to locstg_review_status_idx;
alter index if exists ubps_dupe_cluster_idx   rename to locstg_dupe_cluster_idx;
alter index if exists ubps_one_canonical_per_cluster rename to locstg_one_canonical_per_cluster;
alter trigger ubps_touch_updated_at on public.locations_stage rename to locstg_touch_updated_at;

alter table public.locations_stage
  rename constraint us_beach_points_staging_governing_body_type_check to locations_stage_governing_body_type_check;

-- ── resolve_governance ─────────────────────────────────────────────────────────────
create or replace function public.resolve_governance(p_fid int)
returns bigint
language plpgsql
as $$
declare
  winner_id bigint;
  v jsonb;
begin
  winner_id := public.pick_canonical_evidence(p_fid, 'governance');
  if winner_id is null then
    return null;
  end if;

  select claimed_values into v
    from public.beach_enrichment_provenance where id = winner_id;

  update public.locations_stage
    set governing_body_type = v->>'type',
        governing_body_name = v->>'name'
    where fid = p_fid;

  return winner_id;
end;
$$;

-- ── resolve_access ─────────────────────────────────────────────────────────────
create or replace function public.resolve_access(p_fid int)
returns bigint
language plpgsql
as $$
declare
  winner_id bigint;
  v jsonb;
begin
  winner_id := public.pick_canonical_evidence(p_fid, 'access');
  if winner_id is null then
    return null;
  end if;

  select claimed_values into v
    from public.beach_enrichment_provenance where id = winner_id;

  update public.locations_stage
    set access_status = v->>'status'
    where fid = p_fid;

  return winner_id;
end;
$$;

-- ── resolve_dogs ─────────────────────────────────────────────────────────────
create or replace function public.resolve_dogs(p_fid int)
returns bigint
language plpgsql
as $$
declare
  winner_id bigint;
  v jsonb;
begin
  winner_id := public.pick_canonical_evidence(p_fid, 'dogs');
  if winner_id is null then
    return null;
  end if;

  select claimed_values into v
    from public.beach_enrichment_provenance where id = winner_id;

  update public.locations_stage
    set dogs_allowed           = v->>'allowed',
        dogs_leash_required    = v->>'leash_required',
        dogs_restricted_hours  = v->'restricted_hours',
        dogs_seasonal_rules    = v->'seasonal_rules',
        dogs_zone_description  = v->>'zone_description'
    where fid = p_fid;

  return winner_id;
end;
$$;

-- ── resolve_practical ─────────────────────────────────────────────────────────────
create or replace function public.resolve_practical(p_fid int)
returns bigint
language plpgsql
as $$
declare
  winner_id bigint;
  v jsonb;
begin
  winner_id := public.pick_canonical_evidence(p_fid, 'practical');
  if winner_id is null then
    return null;
  end if;

  select claimed_values into v
    from public.beach_enrichment_provenance where id = winner_id;

  -- 'practical' covers hours, parking, amenities. The jsonb may carry any
  -- subset of these — only update keys present in claimed_values, leaving
  -- staging columns unchanged where the source didn't claim them.
  update public.locations_stage
    set
      open_time            = coalesce((v->>'open_time')::time,            open_time),
      close_time           = coalesce((v->>'close_time')::time,           close_time),
      hours_text           = coalesce(v->>'hours_text',                   hours_text),
      has_parking          = coalesce((v->>'has_parking')::boolean,       has_parking),
      parking_type         = coalesce(v->>'parking_type',                 parking_type),
      parking_notes        = coalesce(v->>'parking_notes',                parking_notes),
      has_restrooms        = coalesce((v->>'has_restrooms')::boolean,     has_restrooms),
      has_showers          = coalesce((v->>'has_showers')::boolean,       has_showers),
      has_drinking_water   = coalesce((v->>'has_drinking_water')::boolean,has_drinking_water),
      has_lifeguards       = coalesce((v->>'has_lifeguards')::boolean,    has_lifeguards),
      has_disabled_access  = coalesce((v->>'has_disabled_access')::boolean,has_disabled_access),
      has_food             = coalesce((v->>'has_food')::boolean,          has_food),
      has_fire_pits        = coalesce((v->>'has_fire_pits')::boolean,     has_fire_pits),
      has_picnic_area      = coalesce((v->>'has_picnic_area')::boolean,   has_picnic_area)
    where fid = p_fid;

  return winner_id;
end;
$$;

-- ── populate_all ─────────────────────────────────────────────────────────────
create or replace function public.populate_all(p_fid int default null)
returns jsonb
language plpgsql
as $$
declare
  result jsonb := '{}'::jsonb;
  c int;
begin
  -- Layer 1: direct-fill spatial truth
  c := public.populate_layer1_geographic(p_fid);
  result := result || jsonb_build_object('layer1_geographic', c);

  -- Layer 2: source-specific populators (each emits evidence rows)
  c := public.populate_from_cpad(p_fid);                 result := result || jsonb_build_object('cpad', c);
  c := public.populate_from_ccc(p_fid);                  result := result || jsonb_build_object('ccc', c);
  c := public.populate_from_jurisdictions(p_fid);        result := result || jsonb_build_object('jurisdictions', c);
  c := public.populate_from_csp_parks(p_fid);            result := result || jsonb_build_object('csp_parks', c);
  c := public.populate_from_park_operators(p_fid);       result := result || jsonb_build_object('park_operators', c);
  c := public.populate_from_nps_places(p_fid);           result := result || jsonb_build_object('nps_places', c);
  c := public.populate_from_tribal_lands(p_fid);         result := result || jsonb_build_object('tribal_lands', c);
  c := public.populate_from_military_bases(p_fid);       result := result || jsonb_build_object('military_bases', c);
  c := public.populate_from_private_land_zones(p_fid);   result := result || jsonb_build_object('private_land_zones', c);
  c := public.populate_from_research(p_fid);             result := result || jsonb_build_object('research', c);

  -- Resolvers: read all evidence per (fid, field_group), pick canonical,
  -- write back to staging columns. For each in-scope beach, run all 4.
  declare
    gov_count       int := 0;
    access_count    int := 0;
    dogs_count      int := 0;
    practical_count int := 0;
    f int;
  begin
    for f in
      select fid from public.locations_stage
      where p_fid is null or fid = p_fid
    loop
      if public.resolve_governance(f) is not null then gov_count       := gov_count + 1;       end if;
      if public.resolve_access(f)     is not null then access_count    := access_count + 1;    end if;
      if public.resolve_dogs(f)       is not null then dogs_count      := dogs_count + 1;      end if;
      if public.resolve_practical(f)  is not null then practical_count := practical_count + 1; end if;
    end loop;

    result := result || jsonb_build_object(
      'resolve_governance', gov_count,
      'resolve_access',     access_count,
      'resolve_dogs',       dogs_count,
      'resolve_practical',  practical_count
    );
  end;

  return result;
end;
$$;

-- ── populate_from_ccc ─────────────────────────────────────────────────────────────
create or replace function public.populate_from_ccc(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with candidates as (
    select
      s.fid,
      c.name           as ccc_name,
      c.open_to_public,
      c.dog_friendly,
      c.parking,
      c.restrooms,
      c.showers,
      c.drinking_water,
      c.lifeguard,
      c.disabled_access,
      c.food,
      c.fire_pits,
      c.picnic_area,
      st_distance(s.geom, c.geom::geography) as dist_m,
      cardinality(public.shared_name_tokens(s.display_name, c.name)) > 0
                                              as name_match
    from public.locations_stage s
    join public.ccc_access_points c
      on st_dwithin(c.geom::geography, s.geom, 200)
    where (p_fid is null or s.fid = p_fid)
      and s.geom is not null
  ),
  best as (
    select distinct on (fid) *
    from candidates
    order by fid, dist_m asc, (name_match)::int desc   -- prefer closer; tiebreak name
  ),
  with_conf as (
    select *,
      case
        when dist_m <=  50 and name_match then 0.95
        when dist_m <=  50                then 0.85
        when                  name_match then 0.75
        else                                   0.65
      end as confidence
    from best
  ),
  -- access evidence: only when open_to_public yields a definite bool
  access_rows as (
    select fid, confidence,
      case when public.ccc_yn(open_to_public) is true  then 'public'
           when public.ccc_yn(open_to_public) is false then 'private'
           else null end as status
    from with_conf
  ),
  ins_access as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'access', 'ccc', confidence,
      jsonb_build_object('status', status), now()
    from access_rows
    where status is not null
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  ),
  -- practical evidence: 1 jsonb with up to 9 boolean flags (parking + 8 amenities)
  -- Only emit row when at least one flag is non-null
  practical_rows as (
    select fid, confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'has_parking',         public.ccc_yn(parking),
        'has_restrooms',       public.ccc_yn(restrooms),
        'has_showers',         public.ccc_yn(showers),
        'has_drinking_water',  public.ccc_yn(drinking_water),
        'has_lifeguards',      public.ccc_yn(lifeguard),
        'has_disabled_access', public.ccc_yn(disabled_access),
        'has_food',            public.ccc_yn(food),
        'has_fire_pits',       public.ccc_yn(fire_pits),
        'has_picnic_area',     public.ccc_yn(picnic_area)
      )) as flags
    from with_conf
  ),
  ins_practical as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'practical', 'ccc', confidence, flags, now()
    from practical_rows
    where flags <> '{}'::jsonb
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  ),
  -- dogs evidence: only when dog_friendly yields a definite enum value.
  -- CCC's dog_friendly is 28% filled, so most rows skip this.
  -- Lower confidence than amenities since dog_friendly is sparse + binary.
  dogs_rows as (
    select fid,
      least(0.70, confidence) as dogs_conf,   -- cap at 0.70 — CCC is partial source for dogs
      public.ccc_dog_friendly_to_enum(dog_friendly) as allowed
    from with_conf
  ),
  ins_dogs as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'dogs', 'ccc', dogs_conf,
      jsonb_build_object('allowed', allowed), now()
    from dogs_rows
    where allowed is not null
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from ins_access
    union all select * from ins_practical
    union all select * from ins_dogs
  ) _;

  return rows_touched;
end;
$$;

-- ── populate_from_cpad ─────────────────────────────────────────────────────────────
create or replace function public.populate_from_cpad(p_fid int default null)
returns int
language plpgsql
as $$
declare
  rows_touched int := 0;
begin
  with candidates as (
    select
      s.fid,
      c.unit_name,
      c.mng_ag_lev,
      c.mng_agncy,
      c.access_typ,
      c.geom              as cpad_geom,
      s.display_name,
      st_contains(c.geom, s.geom::geometry)             as inside,
      st_distance(s.geom, c.geom::geography)            as dist_m,
      st_area(c.geom::geography)                        as area_m2,
      cardinality(public.shared_name_tokens(s.display_name, c.unit_name)) > 0
                                                         as name_match
    from public.locations_stage s
    join public.cpad_units c
      on st_dwithin(c.geom::geography, s.geom, 100)     -- 100m max buffer
    where (p_fid is null or s.fid = p_fid)
      and s.geom is not null
  ),
  ranked as (
    select *,
      row_number() over (
        partition by fid
        order by inside desc,            -- inside polygon beats outside
                 name_match desc,        -- name match preferred
                 area_m2 asc             -- smallest polygon among ties = most specific
      ) as rnk
    from candidates
  ),
  best as (
    select * from ranked where rnk = 1
  ),
  -- Compute confidence per match-quality matrix
  with_conf as (
    select *,
      case
        when inside     and name_match then 0.95
        when inside                    then 0.85
        when not inside and name_match then 0.75
        else                                0.60
      end as confidence
    from best
  ),
  -- Map mng_ag_lev to our governing_body_type enum
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
    from with_conf
  ),
  -- Emit governance evidence rows
  upsert_governance as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select
      fid, 'governance', 'cpad', confidence,
      jsonb_build_object('type', gov_type, 'name', mng_agncy),
      now()
    from with_type
    where mng_ag_lev is not null
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  ),
  -- Emit access evidence rows (only when CPAD has access_typ filled)
  upsert_access as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select
      fid, 'access', 'cpad', confidence,
      jsonb_build_object('status', access_status),
      now()
    from with_type
    where access_status is not null
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from upsert_governance
    union all
    select * from upsert_access
  ) _;

  return rows_touched;
end;
$$;

-- ── populate_from_research ─────────────────────────────────────────────────────────────
create or replace function public.populate_from_research(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with bsn as (
    select
      bsn.src_fid as fid,
      public.bsn_confidence_to_numeric(bsn.enrichment_confidence) as confidence,
      bsn.dogs_allowed,
      public.bsn_leash_to_enum(bsn.dogs_leash_required) as leash_required,
      bsn.dogs_daily_windows,
      bsn.dogs_seasonal_closures,
      coalesce(bsn.dogs_allowed_areas, bsn.dogs_off_leash_area) as zone_description,
      bsn.dogs_policy_notes,
      bsn.hours_text,
      bsn.has_parking, bsn.parking_type, bsn.parking_notes,
      bsn.has_restrooms, bsn.has_showers, bsn.has_lifeguards, bsn.has_picnic_area,
      bsn.has_food, bsn.has_drinking_water, bsn.has_fire_pits, bsn.has_disabled_access
    from public.beaches_staging_new bsn
    -- Only rows that bridge to locations_stage
    join public.locations_stage s on s.fid = bsn.src_fid
    where bsn.src_fid is not null
      and (p_fid is null or bsn.src_fid = p_fid)
  ),
  -- Build dogs claimed_values
  dogs_built as (
    select fid, confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',          dogs_allowed,
        'leash_required',   leash_required,
        'restricted_hours', dogs_daily_windows,
        -- Transform old seasonal_closures shape {start,end,reason}
        -- to new seasonal_rules shape {from,to,notes}
        'seasonal_rules',
          case
            when dogs_seasonal_closures is null
              or jsonb_typeof(dogs_seasonal_closures) <> 'array'
              or jsonb_array_length(dogs_seasonal_closures) = 0
            then null
            else (
              select jsonb_agg(jsonb_build_object(
                'from',  e->>'start',
                'to',    e->>'end',
                'notes', e->>'reason'
              ))
              from jsonb_array_elements(dogs_seasonal_closures) e
            )
          end,
        'zone_description', zone_description,
        'notes',            dogs_policy_notes
      )) as v
    from bsn
  ),
  ins_dogs as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'dogs', 'research', confidence, v, now()
    from dogs_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  ),
  -- Build practical claimed_values (open/close_time aren't in bsn — leave for LLM)
  practical_built as (
    select fid, confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'hours_text',         hours_text,
        'has_parking',        has_parking,
        'parking_type',       parking_type,
        'parking_notes',      parking_notes,
        'has_restrooms',      has_restrooms,
        'has_showers',        has_showers,
        'has_lifeguards',     has_lifeguards,
        'has_picnic_area',    has_picnic_area,
        'has_food',           has_food,
        'has_drinking_water', has_drinking_water,
        'has_fire_pits',      has_fire_pits,
        'has_disabled_access',has_disabled_access
      )) as v
    from bsn
  ),
  ins_practical as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'practical', 'research', confidence, v, now()
    from practical_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source) do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from ins_dogs union all select * from ins_practical
  ) _;

  return rows_touched;
end;
$$;

-- ── populate_layer1_geographic ─────────────────────────────────────────────────────────────
create or replace function public.populate_layer1_geographic(p_fid int default null)
returns int
language plpgsql
as $$
declare
  rows_updated int;
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
  place_match as (
    select s.fid, j.name as place_name, j.fips_place, j.place_type
    from public.locations_stage s
    left join public.jurisdictions j on st_contains(j.geom, s.geom::geometry)
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

-- ── populate_from_jurisdictions ─────────────────────────────────────────────────────────────
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
    from public.locations_stage s
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

-- ── populate_from_csp_parks ─────────────────────────────────────────────────────────────
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
    from public.locations_stage s
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

-- ── populate_from_park_operators ─────────────────────────────────────────────────────────────
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

-- ── populate_from_nps_places ─────────────────────────────────────────────────────────────
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
    from public.locations_stage s
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

-- ── populate_from_tribal_lands ─────────────────────────────────────────────────────────────
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

-- ── populate_from_military_bases ─────────────────────────────────────────────────────────────
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

-- ── populate_from_private_land_zones ─────────────────────────────────────────────────────────────
create or replace function public.populate_from_private_land_zones(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with matches as (
    select s.fid, plz.name, plz.reason
    from public.locations_stage s
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

-- ── staging_find_dedup_pairs ─────────────────────────────────────────────────────────────
create or replace function public.staging_find_dedup_pairs(
  max_distance_m  double precision default 50,
  min_similarity  double precision default 0.5
)
returns table (
  winner_fid  int,
  winner_name text,
  loser_fid   int,
  loser_name  text,
  dist_m      double precision,
  name_sim    double precision
)
language sql stable
as $$
  with candidates as (
    select fid, display_name, geom, review_status, duplicate_status
    from public.locations_stage
    where geom is not null
      and (duplicate_status is null or duplicate_status = 'needs_review')
      and (review_status is null or review_status <> 'flagged')
  ),
  pairs as (
    select
      a.fid as a_fid, a.display_name as a_name, a.review_status as a_status,
      b.fid as b_fid, b.display_name as b_name, b.review_status as b_status,
      similarity(a.display_name, b.display_name) as name_sim,
      st_distance(a.geom, b.geom) as dist_m
    from candidates a
    join candidates b
      on a.fid < b.fid
     and st_dwithin(a.geom, b.geom, max_distance_m)
  ),
  scored as (
    select
      case
        when (a_status is null or a_status = 'verified')
         and (b_status is distinct from null and b_status <> 'verified') then a_fid
        when (b_status is null or b_status = 'verified')
         and (a_status is distinct from null and a_status <> 'verified') then b_fid
        when length(a_name) > length(b_name) then a_fid
        when length(b_name) > length(a_name) then b_fid
        when a_fid < b_fid then a_fid
        else b_fid
      end as winner_fid,
      case
        when (a_status is null or a_status = 'verified')
         and (b_status is distinct from null and b_status <> 'verified') then a_name
        when (b_status is null or b_status = 'verified')
         and (a_status is distinct from null and a_status <> 'verified') then b_name
        when length(a_name) > length(b_name) then a_name
        when length(b_name) > length(a_name) then b_name
        when a_fid < b_fid then a_name
        else b_name
      end as winner_name,
      case
        when (a_status is null or a_status = 'verified')
         and (b_status is distinct from null and b_status <> 'verified') then b_fid
        when (b_status is null or b_status = 'verified')
         and (a_status is distinct from null and a_status <> 'verified') then a_fid
        when length(a_name) > length(b_name) then b_fid
        when length(b_name) > length(a_name) then a_fid
        when a_fid < b_fid then b_fid
        else a_fid
      end as loser_fid,
      case
        when (a_status is null or a_status = 'verified')
         and (b_status is distinct from null and b_status <> 'verified') then b_name
        when (b_status is null or b_status = 'verified')
         and (a_status is distinct from null and a_status <> 'verified') then a_name
        when length(a_name) > length(b_name) then b_name
        when length(b_name) > length(a_name) then a_name
        when a_fid < b_fid then b_name
        else a_name
      end as loser_name,
      dist_m,
      name_sim
    from pairs
    where name_sim >= min_similarity
  )
  -- One row per loser: closest winner takes precedence if a row matches multiple
  select distinct on (loser_fid)
    winner_fid, winner_name, loser_fid, loser_name, dist_m, name_sim
  from scored
  order by loser_fid, dist_m asc;
$$;

-- ── staging_find_neighbor_inheritance ─────────────────────────────────────────────────────────────
create or replace function public.staging_find_neighbor_inheritance(
  max_distance_m   double precision default 200,
  trusted_sources  text[] default array['cpad','pad_us','tiger_places']
)
returns table (
  unlocked_fid    int,
  unlocked_name   text,
  locked_fid      int,
  locked_name     text,
  locked_type     text,
  locked_body     text,
  locked_source   text,
  dist_m          double precision
)
language sql stable
as $$
  with unlocked as (
    -- Rows with no governance assigned yet
    select s.fid, s.display_name, s.geom
    from public.locations_stage s
    left join public.beach_enrichment_provenance p
      on p.fid = s.fid and p.field_group = 'governance'
    where s.geom is not null
      and p.fid is null
  ),
  locked as (
    -- Rows with governance from a trusted polygon source
    select s.fid, s.display_name, s.geom,
           s.governing_body_type, s.governing_body_name,
           p.source as gov_source
    from public.locations_stage s
    join public.beach_enrichment_provenance p
      on p.fid = s.fid and p.field_group = 'governance'
    where s.geom is not null
      and p.source = any (trusted_sources)
      and s.governing_body_type is not null
  ),
  pairs as (
    select
      u.fid as u_fid, u.display_name as u_name,
      l.fid as l_fid, l.display_name as l_name,
      l.governing_body_type as l_type,
      l.governing_body_name as l_body,
      l.gov_source as l_source,
      st_distance(u.geom, l.geom) as dist_m
    from unlocked u
    join locked l
      on st_dwithin(u.geom, l.geom, max_distance_m)
  )
  -- One inheritance per unlocked row: closest trusted neighbor wins
  select distinct on (u_fid)
    u_fid as unlocked_fid, u_name as unlocked_name,
    l_fid as locked_fid,   l_name as locked_name,
    l_type as locked_type, l_body as locked_body, l_source as locked_source,
    dist_m
  from pairs
  order by u_fid, dist_m asc;
$$;

commit;