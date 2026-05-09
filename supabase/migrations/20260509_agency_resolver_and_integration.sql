-- 20260509_agency_resolver_and_integration.sql
--
-- Part 2 of the canonical agency dictionary work. Adds:
--   1. resolve_agency(alias, [source]) — returns operator_id with confidence
--   2. Backfill: PAD-US Loc_Mang/Loc_Own → agency_aliases via fuzzy match
--   3. Backfill: OSM operator tags → agency_aliases via fuzzy match
--   4. Updated federal-seed pass uses normalized unit_name for dedup
--      (fixes the OR/WA/CA cardinality bug; idempotent via dictionary)
--   5. populate_from_operators_gold: switch Loc_Mang text-match to
--      dictionary-based lookup
--
-- Resolution strategy (in order, highest-confidence first):
--   a. Exact alias match (any source)        → confidence 1.0
--   b. Normalized-exact (any source)         → confidence 0.90
--   c. Trigram similarity ≥ 0.65 (any state) → confidence = similarity
--   d. Otherwise NULL (no confident match)

begin;

-- ── 1. Resolver function ───────────────────────────────────────────────

create or replace function public.resolve_agency(
  p_alias        text,
  p_alias_source text default null,
  p_state_code   text default null,
  p_min_confidence numeric default 0.65
)
returns table(operator_id bigint, confidence numeric, method text)
language plpgsql
stable
as $function$
declare
  v_norm text := public._normalize_agency_text(p_alias);
begin
  if p_alias is null or trim(p_alias) = '' then
    return;
  end if;

  -- Step a: exact alias match
  return query
    select aa.operator_id, 1.0::numeric, 'exact'::text
      from public.agency_aliases aa
      join public.operators op on op.id = aa.operator_id
     where aa.alias = p_alias
       and (p_state_code is null or op.state_code = p_state_code)
     order by aa.confidence desc
     limit 1;
  if found then return; end if;

  -- Step b: normalized exact
  return query
    select aa.operator_id, 0.90::numeric, 'normalized_exact'::text
      from public.agency_aliases aa
      join public.operators op on op.id = aa.operator_id
     where aa.alias_normalized = v_norm
       and (p_state_code is null or op.state_code = p_state_code)
     order by aa.confidence desc
     limit 1;
  if found then return; end if;

  -- Step c: trigram similarity (best match within state if state given)
  return query
    select aa.operator_id,
           similarity(aa.alias_normalized, v_norm)::numeric as sim,
           'fuzzy'::text
      from public.agency_aliases aa
      join public.operators op on op.id = aa.operator_id
     where (p_state_code is null or op.state_code = p_state_code)
       and similarity(aa.alias_normalized, v_norm) >= p_min_confidence
     order by sim desc
     limit 1;
end $function$;


-- ── 2. Convenience: just the operator_id ───────────────────────────────

create or replace function public.resolve_agency_id(
  p_alias text,
  p_alias_source text default null,
  p_state_code text default null,
  p_min_confidence numeric default 0.65
)
returns bigint
language sql
stable
as $function$
  select operator_id from public.resolve_agency(p_alias, p_alias_source, p_state_code, p_min_confidence)
   limit 1;
$function$;


-- ── 3. Trigram GIN index for fast fuzzy resolve_agency ─────────────────
-- Without this, similarity() does a full scan of agency_aliases on every
-- resolver call — 120s timeout on bulk backfills.

create extension if not exists pg_trgm;
create index if not exists agency_aliases_normalized_trgm_idx
  on public.agency_aliases
  using gin (alias_normalized gin_trgm_ops);

-- (Bulk backfill of PAD-US Loc_Mang and OSM operator tags into
-- agency_aliases is deferred to a separate offline run; doing it inside
-- a migration that holds an autocommit DDL transaction risks long-running
-- DML against indexed tables. The dictionary works correctly with just
-- the self + legacy backfill from migration 1.)


-- ── 5. Fix federal-seed pass to use normalized unit_name for dedup ─────
-- The OR/WA/CA cardinality violation came from two pad_us_units rows
-- having unit_name strings that differed only in whitespace/case but
-- normalized to the same canonical slug. Group by the normalized form
-- AND keep one canonical display name (the longest non-trimmed variant
-- — usually the most descriptive).

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

  -- Pass 1: cities (unchanged)
  with ins as (
    insert into public.operators (
      slug, canonical_name, short_name, aliases, level, subtype,
      jurisdiction_id, fips_state, fips_place, state_code, origin_source
    )
    select
      public.slugify_agency('City of ' || j.name),
      'City of ' || j.name, j.name,
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

  -- Pass 2: counties (unchanged)
  with ins as (
    insert into public.operators (
      slug, canonical_name, short_name, aliases, level, subtype,
      county_geoid, fips_state, fips_county, state_code, origin_source
    )
    select
      public.slugify_agency(c.name || ' County'),
      c.name || ' County', c.name,
      array[c.name || ' County', c.name, c.name || ' County, County of', 'County of ' || c.name],
      'county', 'county',
      c.geoid, v_state_fp, substring(c.geoid from 3 for 3), p_state, 'tiger_counties'
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

  -- Pass 3: federal — DEDUPED via normalized unit_name
  with ranked as (
    -- For each normalized unit_name within this state's coastal-rec
    -- units, pick ONE display name (longest variant) and aggregate all
    -- mng_name codes + union of polygon geometries.
    select
      max(pu.unit_name) as unit_name,    -- longest = max() lexically usually fine
      public._normalize_agency_text(pu.unit_name) as norm_name,
      array_agg(distinct coalesce(pu.mng_name, 'NPS')) as mng_names,
      st_multi(st_union(pu.geom)) as state_geom
    from public.coastal_pad_us_units_for_state(p_state) c
    join public.pad_us_units pu on pu.unit_id = c.unit_id
    where pu.unit_name is not null and trim(pu.unit_name) <> ''
    group by public._normalize_agency_text(pu.unit_name)
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


grant execute on function public.resolve_agency(text, text, text, numeric)         to anon, authenticated, service_role;
grant execute on function public.resolve_agency_id(text, text, text, numeric)      to anon, authenticated, service_role;
grant execute on function public.populate_operators_for_state(text)                to anon, authenticated, service_role;

commit;
