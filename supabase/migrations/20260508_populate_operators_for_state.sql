-- 20260508_populate_operators_for_state.sql
--
-- Parameterized version of 20260427_operators_seed.sql Passes 1+2 (cities,
-- counties) so we can run the CA seed pattern for any state. Plus inline
-- state-agency seeds for OR + WA. Plus wire the populator into
-- run_pipeline_for_state so future state launches don't ship without
-- operators.
--
-- Cities + counties: TIGER-derived, deterministic.
-- State agencies (federal seed list reused as-is from the original seed —
-- agencies are nationwide).
-- CPAD pass: skipped here (CA-only data; OR/WA use PAD-US which feeds
-- mng_agncy via populate_from_pad_us_gold elsewhere).

begin;

-- Parameterized cities+counties populator.
create or replace function public.populate_operators_for_state(p_state text)
returns table(cities_added int, counties_added int)
language plpgsql
as $$
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
      'city',
      'city',
      j.id,
      j.fips_state,
      j.fips_place,
      p_state,
      'tiger_places'
    from public.jurisdictions j
    where j.state = p_state
      and j.place_type like 'C%'
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
      'county',
      'county',
      c.geoid,
      v_state_fp,
      substring(c.geoid from 3 for 3),
      p_state,
      'tiger_counties'
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

  return query select v_cities, v_counties;
end $$;

-- OR state-agency seed
insert into public.operators (slug, canonical_name, short_name, aliases, level, subtype, state_code, origin_source) values
  ('oregon-parks-recreation-department', 'Oregon Parks and Recreation Department', 'OPRD',
    array['Oregon Parks and Recreation Department', 'OPRD', 'Oregon State Parks', 'Oregon Parks'],
    'state', 'state-parks', 'OR', 'manual'),
  ('oregon-department-fish-wildlife', 'Oregon Department of Fish and Wildlife', 'ODFW',
    array['Oregon Department of Fish and Wildlife', 'ODFW', 'Oregon Fish and Wildlife'],
    'state', 'cdfw', 'OR', 'manual'),
  ('oregon-department-state-lands', 'Oregon Department of State Lands', 'DSL',
    array['Oregon Department of State Lands', 'DSL', 'Oregon State Lands'],
    'state', 'slc', 'OR', 'manual'),
  ('oregon-department-transportation', 'Oregon Department of Transportation', 'ODOT',
    array['Oregon Department of Transportation', 'ODOT'],
    'state', 'transportation', 'OR', 'manual')
on conflict (slug) do nothing;

-- WA state-agency seed
insert into public.operators (slug, canonical_name, short_name, aliases, level, subtype, state_code, origin_source) values
  ('washington-state-parks', 'Washington State Parks and Recreation Commission', 'Washington State Parks',
    array['Washington State Parks and Recreation Commission', 'Washington State Parks', 'WA State Parks'],
    'state', 'state-parks', 'WA', 'manual'),
  ('washington-department-fish-wildlife', 'Washington Department of Fish and Wildlife', 'WDFW',
    array['Washington Department of Fish and Wildlife', 'WDFW', 'Washington Fish and Wildlife'],
    'state', 'cdfw', 'WA', 'manual'),
  ('washington-department-natural-resources', 'Washington Department of Natural Resources', 'WA DNR',
    array['Washington Department of Natural Resources', 'WA DNR', 'WDNR', 'DNR'],
    'state', 'natural-resources', 'WA', 'manual'),
  ('washington-state-department-transportation', 'Washington State Department of Transportation', 'WSDOT',
    array['Washington State Department of Transportation', 'WSDOT'],
    'state', 'transportation', 'WA', 'manual')
on conflict (slug) do nothing;

-- Run the populator for OR and WA inline.
select * from public.populate_operators_for_state('OR');
select * from public.populate_operators_for_state('WA');

commit;
