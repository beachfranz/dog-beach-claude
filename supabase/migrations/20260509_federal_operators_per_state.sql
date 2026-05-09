-- 20260509_federal_operators_per_state.sql
--
-- Franz: collapse the separate `pad_us_unit_dogs_policy` curation pipeline
-- into the existing operator-extraction pipeline. NPS / USFWS / etc.
-- become first-class entries in the `operators` table at level='federal',
-- one row per unique coastal unit. The existing extract_operator_dogs_policy.py
-- then researches them like any other operator.
--
-- Changes:
--   1. populate_operators_for_state(state) extended with Pass 3 (federal):
--      one operator row per unique coastal-rec PAD-US unit_name whose
--      polygons intersect the state's counties. Uses
--      coastal_pad_us_units_for_state(state) for the unit list.
--   2. state_operator_ids_with_beaches(state) now includes level='federal'
--      via federal-operator polygon intersect with state's beaches.
--
-- The federal_policy_seed phase becomes redundant once this lands;
-- removed from the orchestrator's PHASES list separately.
--
-- pad_us_unit_dogs_policy is retained as a manual-override layer for
-- high-confidence curated rules (e.g. Cape Cod NS plover-zone seasonal
-- closures) but is no longer the primary federal-policy path.

begin;

-- ── 1. Extend populate_operators_for_state with federal pass ───────────

create or replace function public.populate_operators_for_state(p_state text)
returns table(cities_added integer, counties_added integer, federal_added integer)
language plpgsql
as $function$
declare
  v_state_fp text := case p_state
    when 'AL' then '01' when 'AK' then '02' when 'AZ' then '04' when 'AR' then '05'
    when 'CA' then '06' when 'CO' then '08' when 'CT' then '09' when 'DE' then '10'
    when 'FL' then '12' when 'GA' then '13' when 'HI' then '15' when 'ID' then '16'
    when 'IL' then '17' when 'IN' then '18' when 'IA' then '19' when 'KS' then '20'
    when 'KY' then '21' when 'LA' then '22' when 'ME' then '23' when 'MD' then '24'
    when 'MA' then '25' when 'MI' then '26' when 'MN' then '27' when 'MS' then '28'
    when 'MO' then '29' when 'MT' then '30' when 'NE' then '31' when 'NV' then '32'
    when 'NH' then '33' when 'NJ' then '34' when 'NM' then '35' when 'NY' then '36'
    when 'NC' then '37' when 'ND' then '38' when 'OH' then '39' when 'OK' then '40'
    when 'OR' then '41' when 'PA' then '42' when 'RI' then '44' when 'SC' then '45'
    when 'SD' then '46' when 'TN' then '47' when 'TX' then '48' when 'UT' then '49'
    when 'VT' then '50' when 'VA' then '51' when 'WA' then '53' when 'WV' then '54'
    when 'WI' then '55' when 'WY' then '56' else null end;
  v_cities int := 0;
  v_counties int := 0;
  v_federal int := 0;
begin
  if v_state_fp is null then
    raise exception 'unknown state code: %', p_state;
  end if;

  -- Pass 1: cities (TIGER incorporated places — place_type starting 'C')
  with ins as (
    insert into public.operators (
      slug, canonical_name, short_name, aliases, level, subtype,
      jurisdiction_id, fips_state, fips_place, state_code, origin_source
    )
    select
      public.slugify_agency('City of ' || j.name),
      'City of ' || j.name,
      j.name,
      array['City of ' || j.name, j.name, j.name || ', City of'],
      'city', 'city',
      j.id, j.fips_state, j.fips_place, p_state, 'tiger_places'
    from public.jurisdictions j
    where j.state = p_state and j.place_type like 'C%'
    on conflict (slug) do update set
      jurisdiction_id = excluded.jurisdiction_id,
      fips_state      = excluded.fips_state,
      fips_place      = excluded.fips_place,
      aliases         = (select array_agg(distinct a)
                         from unnest(public.operators.aliases || excluded.aliases) a),
      updated_at      = now()
    returning 1
  )
  select count(*) into v_cities from ins;

  -- Pass 2: counties (TIGER counties under this state)
  with ins as (
    insert into public.operators (
      slug, canonical_name, short_name, aliases, level, subtype,
      county_geoid, fips_state, fips_county, state_code, origin_source
    )
    select
      public.slugify_agency(c.name || ' County'),
      c.name || ' County',
      c.name,
      array[c.name || ' County', c.name, c.name || ' County, County of', 'County of ' || c.name],
      'county', 'county',
      c.geoid, v_state_fp, substring(c.geoid from 3 for 3),
      p_state, 'tiger_counties'
    from public.counties c
    where c.state_fp = v_state_fp
    on conflict (slug) do update set
      county_geoid = excluded.county_geoid,
      fips_state   = excluded.fips_state,
      fips_county  = excluded.fips_county,
      aliases      = (select array_agg(distinct a)
                      from unnest(public.operators.aliases || excluded.aliases) a),
      updated_at   = now()
    returning 1
  )
  select count(*) into v_counties from ins;

  -- ── Pass 3 (★ NEW): federal coastal-rec units per PAD-US ──────────────
  -- One operator row per unique coastal-rec unit_name whose polygons
  -- intersect this state's counties. Geom = union of all this unit's
  -- polygons in the state. State_code = p_state (multi-state units like
  -- Assateague get a per-state row each, with each row's geom being
  -- only the polygons in that state).

  with ranked as (
    -- One row per unit_name. mng_name aggregated as an array (some
    -- multi-polygon units span both 'NPS' and 'CONE' management codes
    -- in PAD-US — we want a single operator row per unit, with all
    -- distinct mng_names captured).
    select pu.unit_name,
           array_agg(distinct coalesce(pu.mng_name, 'NPS')) as mng_names,
           st_multi(st_union(pu.geom)) as state_geom
      from public.coastal_pad_us_units_for_state(p_state) c
      join public.pad_us_units pu on pu.unit_id = c.unit_id
     where pu.unit_name is not null
     group by pu.unit_name
  ),
  ins as (
    insert into public.operators (
      slug, canonical_name, short_name, aliases, level, subtype,
      state_code, fips_state, geom, pad_us_mng_name, origin_source
    )
    select
      public.slugify_agency(unit_name || ' (' || p_state || ')'),
      unit_name,
      regexp_replace(unit_name,
        ' (National Seashore|National Wildlife Refuge|National Lakeshore'
        '|National Recreation Area|National Park|National Monument)$',
        '', 'i'),
      array[unit_name],
      'federal',
      lower(coalesce(
        substring(unit_name from '(National Seashore|National Wildlife Refuge'
                                '|National Lakeshore|National Recreation Area'
                                '|National Park|National Monument)'),
        'federal')),
      p_state,
      v_state_fp,
      state_geom,
      mng_names,
      'seed_federal'
    from ranked
    on conflict (slug) do update set
      geom            = excluded.geom,
      pad_us_mng_name = (select array_agg(distinct n)
                         from unnest(coalesce(public.operators.pad_us_mng_name, '{}'::text[])
                                     || excluded.pad_us_mng_name) n),
      aliases         = (select array_agg(distinct a)
                         from unnest(public.operators.aliases || excluded.aliases) a),
      updated_at      = now()
    returning 1
  )
  select count(*) into v_federal from ins;

  return query select v_cities, v_counties, v_federal;
end $function$;


-- ── 2. Smart operator filter — include level='federal' ─────────────────

create or replace function public.state_operator_ids_with_beaches(p_state text)
returns table(operator_id bigint)
language sql
stable
as $function$
  with beach_counties as (
    select distinct g.county_fips as cfip
      from public.beaches_gold g
     where g.is_active and g.state = p_state and g.county_fips is not null
  ),
  bep_ops as (
    select distinct (bep.claimed_values->>'operator_id')::bigint as operator_id
      from public.beach_enrichment_provenance bep
      join public.beaches_gold g on g.fid = bep.gold_fid
     where g.is_active and g.state = p_state
       and bep.field_group = 'polygon_containment'
       and bep.claimed_values ? 'operator_id'
  ),
  -- Spatial signal: any operator with geom that intersects/near a beach.
  -- This now picks up the new level='federal' operators (PAD-US polygons).
  spatial_ops as (
    select distinct op.id as operator_id
      from public.operators op
     where op.state_code = p_state and op.is_active
       and op.level in ('city','county','state','federal')
       and op.geom is not null
       and exists (
         select 1 from public.beaches_gold g
          where g.is_active and g.state = p_state
            and st_dwithin(op.geom::geography, g.geom::geography, 1000)
       )
  ),
  county_fips_ops as (
    select op.id as operator_id
      from public.operators op
     where op.state_code = p_state and op.is_active
       and op.level = 'county'
       and op.county_geoid in (select cfip from beach_counties)
  ),
  city_via_jurisdiction_ops as (
    select op.id as operator_id
      from public.operators op
      join public.jurisdictions j on j.id = op.jurisdiction_id
     where op.state_code = p_state and op.is_active
       and op.level = 'city'
       and j.geom is not null
       and exists (
         select 1 from public.beaches_gold g
          where g.is_active and g.state = p_state
            and st_intersects(j.geom, g.geom)
       )
  ),
  state_or_federal_level_ops as (
    -- State-level operators: always include (statewide jurisdiction).
    -- Federal-level: only if their geom intersects a beach (handled by
    -- spatial_ops above). Federal operators without geom (legacy CA
    -- catch-alls like 'National Park Service') are excluded — they're
    -- too generic to research per-state.
    select op.id as operator_id
      from public.operators op
     where op.state_code = p_state and op.is_active
       and op.level = 'state'
  ),
  union_all as (
    select operator_id from bep_ops
    union
    select operator_id from spatial_ops
    union
    select operator_id from county_fips_ops
    union
    select operator_id from city_via_jurisdiction_ops
    union
    select operator_id from state_or_federal_level_ops
  )
  select operator_id from union_all where operator_id is not null;
$function$;


grant execute on function public.populate_operators_for_state(text)         to anon, authenticated, service_role;
grant execute on function public.state_operator_ids_with_beaches(text)      to anon, authenticated, service_role;

commit;
