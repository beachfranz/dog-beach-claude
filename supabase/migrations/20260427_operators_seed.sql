-- Bootstrap public.operators from CPAD + jurisdictions + counties + seed
-- federal/state/tribal lists. Run once. Idempotent via slug UPSERT.
--
-- Order matters: we INSERT canonical-identity rows first (cities,
-- counties, federal/state/tribal seeds), then UPSERT CPAD agencies on
-- slug. CPAD names that match an already-inserted operator merge into
-- it (extending aliases + cpad_agncy_name); CPAD names that don't match
-- create new rows.

-- Allow 'joint' co-management (CPAD has 89 such polygons in CA).
alter table public.operators drop constraint if exists operators_level_check;
alter table public.operators add  constraint operators_level_check
  check (level in (
    'federal','state','tribal','county','city',
    'special-district','private','joint','unknown'));


-- ── Helper: slugify_agency ──────────────────────────────────────────
-- Deterministic slug generation. Handles CPAD's trailing-suffix pattern
-- ("Newport Beach, City of" → "City of Newport Beach") before
-- normalizing. Strips common stop words to keep slugs tight.
create or replace function public.slugify_agency(p_name text) returns text
language sql immutable as $$
  with reordered as (
    select case
      when p_name ~* ', city of$' then
        'City of ' || regexp_replace(p_name, ',\s*city of\s*$', '', 'i')
      when p_name ~* ', county of$' then
        regexp_replace(p_name, ',\s*county of\s*$', '', 'i')
      when p_name ~* ', state of$' then
        regexp_replace(p_name, ',\s*state of\s*$', '', 'i')
      else p_name
    end as nm
  ),
  cleaned as (
    -- lowercase, strip non-alphanumeric to spaces
    select lower(regexp_replace(nm, '[^a-zA-Z0-9 ]+', ' ', 'g')) as nm
    from reordered
  ),
  hyphened as (
    select regexp_replace(trim(cleaned.nm), '\s+', '-', 'g') as nm from cleaned
  )
  select nm from hyphened;
$$;


-- ── Helper: CPAD level → operator level ─────────────────────────────
create or replace function public._cpad_lev_to_operator_level(p_lev text) returns text
language sql immutable as $$
  select case lower(coalesce(p_lev, ''))
    when 'federal'                 then 'federal'
    when 'state'                   then 'state'
    when 'county'                  then 'county'
    when 'city'                    then 'city'
    when 'special district'        then 'special-district'
    when 'tribal'                  then 'tribal'
    when 'private'                 then 'private'
    when 'non profit'              then 'private'
    when 'home owners association' then 'private'
    when 'joint'                   then 'joint'
    else 'unknown'
  end;
$$;


-- ── Pass 1: cities (TIGER incorporated places) ──────────────────────
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
  'CA',
  'tiger_places'
from public.jurisdictions j
where j.state = 'CA'
  and j.place_type like 'C%'
on conflict (slug) do update set
  jurisdiction_id = excluded.jurisdiction_id,
  fips_state      = excluded.fips_state,
  fips_place      = excluded.fips_place,
  aliases         = (select array_agg(distinct a)
                     from unnest(public.operators.aliases || excluded.aliases) a),
  updated_at      = now();


-- ── Pass 2: counties (TIGER) ────────────────────────────────────────
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
  '06',
  substring(c.geoid from 3 for 3),
  'CA',
  'tiger_counties'
from public.counties c
where c.geoid like '06%'
on conflict (slug) do update set
  county_geoid = excluded.county_geoid,
  fips_state   = excluded.fips_state,
  fips_county  = excluded.fips_county,
  aliases      = (select array_agg(distinct a)
                  from unnest(public.operators.aliases || excluded.aliases) a),
  updated_at   = now();


-- ── Pass 3: federal seed list ───────────────────────────────────────
-- Common federal land-managing agencies relevant to CA coasts/lands.
-- Subtype distinguishes operational sub-units when CPAD differentiates.
insert into public.operators (slug, canonical_name, short_name, aliases, level, subtype, origin_source)
values
  ('national-park-service',                'National Park Service',                              'NPS',         array['National Park Service','NPS','U.S. National Park Service'],                                              'federal','nps',    'seed_federal'),
  ('us-forest-service',                    'U.S. Forest Service',                                'USFS',        array['U.S. Forest Service','US Forest Service','USFS','United States Forest Service','USDA Forest Service'], 'federal','usfs',   'seed_federal'),
  ('bureau-of-land-management',            'Bureau of Land Management',                          'BLM',         array['Bureau of Land Management','BLM','U.S. Bureau of Land Management'],                                    'federal','blm',    'seed_federal'),
  ('us-fish-wildlife-service',             'U.S. Fish and Wildlife Service',                     'USFWS',       array['U.S. Fish and Wildlife Service','US Fish and Wildlife Service','USFWS','U.S. Fish & Wildlife Service'], 'federal','usfws',  'seed_federal'),
  ('us-army-corps-engineers',              'U.S. Army Corps of Engineers',                       'USACE',       array['U.S. Army Corps of Engineers','US Army Corps of Engineers','USACE','Army Corps of Engineers'],         'federal','usace',  'seed_federal'),
  ('us-department-defense',                'U.S. Department of Defense',                         'DoD',         array['U.S. Department of Defense','US Department of Defense','DoD','Department of Defense'],                'federal','dod',    'seed_federal'),
  ('us-navy',                              'U.S. Navy',                                          'Navy',        array['U.S. Navy','US Navy','Navy','Department of the Navy'],                                                 'federal','dod',    'seed_federal'),
  ('us-marine-corps',                      'U.S. Marine Corps',                                  'USMC',        array['U.S. Marine Corps','US Marine Corps','USMC','Marine Corps'],                                           'federal','dod',    'seed_federal'),
  ('us-coast-guard',                       'U.S. Coast Guard',                                   'USCG',        array['U.S. Coast Guard','US Coast Guard','USCG','Coast Guard'],                                              'federal','dod',    'seed_federal'),
  ('us-bureau-reclamation',                'U.S. Bureau of Reclamation',                         'USBR',        array['U.S. Bureau of Reclamation','Bureau of Reclamation','USBR'],                                           'federal','usbr',   'seed_federal');


-- ── Pass 4: state seed list (key CA agencies) ───────────────────────
insert into public.operators (slug, canonical_name, short_name, aliases, level, subtype, state_code, origin_source)
values
  ('california-department-parks-recreation', 'California Department of Parks and Recreation', 'California State Parks',         array['California Department of Parks and Recreation','California State Parks','CA State Parks','State Parks','CDPR'],                                                 'state','state-parks',  'CA','manual'),
  ('california-department-fish-wildlife',    'California Department of Fish and Wildlife',    'CDFW',                            array['California Department of Fish and Wildlife','CDFW','California Fish and Wildlife','CA Fish and Wildlife','Department of Fish and Wildlife'],                  'state','cdfw',         'CA','manual'),
  ('california-state-lands-commission',      'California State Lands Commission',             'SLC',                             array['California State Lands Commission','State Lands Commission','SLC'],                                                                                            'state','slc',          'CA','manual'),
  ('california-coastal-conservancy',         'California Coastal Conservancy',                'Coastal Conservancy',             array['California Coastal Conservancy','Coastal Conservancy','State Coastal Conservancy'],                                                                           'state','conservancy',  'CA','manual'),
  ('california-coastal-commission',          'California Coastal Commission',                 'CCC',                             array['California Coastal Commission','Coastal Commission','CCC'],                                                                                                    'state','commission',   'CA','manual'),
  ('university-california-natural-reserve',  'University of California Natural Reserve System','UC Natural Reserve System',      array['University of California Natural Reserve System','UC Natural Reserve System','UCNRS'],                                                                          'state','ucnrs',        'CA','manual'),
  ('california-department-transportation',   'California Department of Transportation',       'Caltrans',                        array['California Department of Transportation','Caltrans','CalTrans'],                                                                                                'state','transportation','CA','manual'),
  ('california-department-water-resources',  'California Department of Water Resources',      'DWR',                             array['California Department of Water Resources','DWR'],                                                                                                              'state','water',        'CA','manual'),
  ('san-francisco-bay-conservation-development-commission', 'San Francisco Bay Conservation and Development Commission','BCDC',  array['San Francisco Bay Conservation and Development Commission','BCDC','Bay Conservation and Development Commission'],                                              'state','commission',   'CA','manual');


-- ── Pass 5: CPAD agencies (UPSERT — merge or insert) ───────────────
insert into public.operators (
  slug, canonical_name, aliases, level, cpad_agncy_name, cpad_agncy_level,
  state_code, origin_source
)
select distinct on (public.slugify_agency(mng_agncy))
  public.slugify_agency(mng_agncy),
  mng_agncy,
  array[mng_agncy],
  public._cpad_lev_to_operator_level(mng_ag_lev),
  mng_agncy,
  mng_ag_lev,
  'CA',
  'cpad'
from public.cpad_units
where mng_agncy is not null
  and mng_agncy <> ''
order by public.slugify_agency(mng_agncy), mng_agncy
on conflict (slug) do update set
  cpad_agncy_name  = coalesce(public.operators.cpad_agncy_name, excluded.cpad_agncy_name),
  cpad_agncy_level = coalesce(public.operators.cpad_agncy_level, excluded.cpad_agncy_level),
  aliases          = (select array_agg(distinct a)
                      from unnest(public.operators.aliases || excluded.aliases) a),
  updated_at       = now();


-- ── Pass 6: backfill osm_operator_strings from observed OSM tags ────
-- For each operator, attach the distinct OSM operator= values that
-- currently match by (a) exact match against canonical_name or any
-- alias. No fuzzy matching here — that's a follow-up resolver pass.
update public.operators op
set osm_operator_strings = (
  select array_agg(distinct osm_op)
  from (
    select distinct (tags->>'operator') as osm_op
    from public.osm_features
    where tags->>'operator' is not null
      and tags->>'operator' <> ''
      and (
        (tags->>'operator') = op.canonical_name
        or (tags->>'operator') = any(op.aliases)
      )
  ) s
)
where exists (
  select 1 from public.osm_features
  where tags->>'operator' = op.canonical_name
     or (tags->>'operator') = any(op.aliases)
);


-- ── Pass 7: refresh denormalized counts ─────────────────────────────
update public.operators op set cpad_unit_count = (
  select count(*) from public.cpad_units cu
  where cu.mng_agncy = op.cpad_agncy_name
)
where op.cpad_agncy_name is not null;
