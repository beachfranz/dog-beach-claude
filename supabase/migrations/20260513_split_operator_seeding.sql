-- 20260513_split_operator_seeding.sql
--
-- Splits populate_operators_for_state's 12 passes into per-pass FUNCTIONS,
-- and converts the orchestrator into a PROCEDURE that COMMITs between
-- passes. Two motivations:
--
--   1. Memory safety. The previous monolithic 12-pass FUNCTION held all
--      passes' intermediate state in one transaction. For OR's PAD-US
--      data shape (max 915K-vertex FED polygons, 9.6K total units,
--      2.2K distinct loc_mangs) this OOMed Supabase TWICE on 2026-05-13.
--      With per-pass COMMITs, memory is released between passes.
--
--   2. Observability. Per-pass functions let the Dagster asset call each
--      one individually, log per-pass timing, and retry just the failed
--      pass. Previously the only signal was the monolithic call hanging
--      for 2.5 hours with no progress visibility.
--
-- Also includes the ST_Union → ST_Collect fix in
-- _seed_operators_from_pad_us_unified. ST_Collect skips topology
-- resolution (microseconds vs seconds per group); downstream
-- ST_Intersects works identically on the resulting multipoly. The
-- geom column type is geometry(MultiPolygon,4326); ST_CollectionExtract
-- with type=3 preserves the multipolygon constraint.
--
-- Callers:
--   - run_state_pipeline.py phase 'operators' → `CALL populate_operators_for_state(...)`
--   - Dagster asset operators_for_state → can call individual passes
--     for per-pass Dagster logs, OR CALL the procedure (its choice).
--
-- See [[project_or_phase10_oom_2026-05-13]] memory pin for incident detail.

begin;

-- ════════════════════════════════════════════════════════════════════════
-- 1. Fix _seed_operators_from_pad_us_unified: ST_Union → ST_Collect
-- ════════════════════════════════════════════════════════════════════════

create or replace function public._seed_operators_from_pad_us_unified(
  p_state text, p_state_fp text, p_mng_types text[],
  p_target_level text, p_subtype text, p_origin_source text,
  p_beach_touching_only boolean default false
) returns integer
language plpgsql as $function$
declare v_count int := 0;
begin
  with ranked as (
    select public.slugify_agency(pu.loc_mang || ' (' || p_state || ')') as slug,
           max(pu.loc_mang)                as canonical_name,
           array_agg(distinct pu.loc_mang) as aliases,
           array_agg(distinct pu.loc_mang) as loc_mangs,
           -- ST_Collect + CollectionExtract avoids ST_Union's topology
           -- resolution cost. For million-vertex polygons this saves
           -- minutes per group. Downstream ST_Intersects is unaffected.
           st_multi(st_collectionextract(st_collect(pu.geom), 3)) as state_geom
      from public.pad_us_units pu
     where pu.state = p_state
       and (
         pu.mng_type = ANY(p_mng_types)
         or public._classify_loc_mang_level(pu.loc_mang, pu.mng_type) = p_target_level
       )
       and public._normalize_agency_text(pu.loc_mang) is not null
       and (
         not p_beach_touching_only
         or exists (
           select 1 from public.beaches_gold g
            where g.state = p_state and g.is_active
              and g.scoring_tier in ('daily','hourly')
              and st_intersects(g.geom, pu.geom)
         )
       )
     group by public.slugify_agency(pu.loc_mang || ' (' || p_state || ')')
  ),
  ins as (
    insert into public.operators (
      slug, canonical_name, short_name, aliases, level, subtype,
      state_code, fips_state, geom, pad_us_mng_name, origin_source
    )
    select
      slug, canonical_name, canonical_name, aliases,
      p_target_level, p_subtype,
      p_state, p_state_fp, state_geom, loc_mangs,
      p_origin_source
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
  select count(*) into v_count from ins;
  return v_count;
end $function$;

-- ════════════════════════════════════════════════════════════════════════
-- 2. State FIPS helper (centralizes the lookup so per-pass functions
--    don't each repeat the CASE expression)
-- ════════════════════════════════════════════════════════════════════════

create or replace function public._op_state_fp(p_state text)
returns text language sql immutable as $$
  select case p_state
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
    when 'WI' then '55' when 'WY' then '56' end;
$$;

-- ════════════════════════════════════════════════════════════════════════
-- 3. Per-pass functions — each returns int (rows added/changed)
-- ════════════════════════════════════════════════════════════════════════

-- Pass 1: cities (TIGER incorporated places — place_type LIKE 'C%')
create or replace function public._op_pass1_cities(p_state text)
returns integer language plpgsql as $function$
declare v_n int := 0;
begin
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
  select count(*) into v_n from ins;
  return v_n;
end $function$;

-- Pass 2: counties (TIGER counties under this state)
create or replace function public._op_pass2_counties(p_state text)
returns integer language plpgsql as $function$
declare v_n int := 0; v_state_fp text := public._op_state_fp(p_state);
begin
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
  select count(*) into v_n from ins;
  return v_n;
end $function$;

-- Pass 3: federal — WIDENED. All mng_type='FED'. Uses ST_Collect.
create or replace function public._op_pass3_federal(p_state text)
returns integer language plpgsql as $function$
declare v_n int := 0; v_state_fp text := public._op_state_fp(p_state);
begin
  with ranked as (
    select public.slugify_agency(pu.unit_name || ' (' || p_state || ')') as slug,
           max(pu.unit_name) as unit_name,
           array_agg(distinct coalesce(pu.mng_name, 'FED')) as mng_names,
           array_agg(distinct pu.loc_mang) filter (
             where public._normalize_agency_text(pu.loc_mang) is not null
           ) as loc_mangs,
           -- ST_Collect-based (no topology cost). Federal NPS / USFS units
           -- can be massive (max 915K vertices in OR); ST_Union would OOM.
           st_multi(st_collectionextract(st_collect(pu.geom), 3)) as state_geom
      from public.pad_us_units pu
     where pu.state = p_state
       and pu.mng_type = 'FED'
       and pu.unit_name is not null and trim(pu.unit_name) <> ''
     group by public.slugify_agency(pu.unit_name || ' (' || p_state || ')')
  ),
  ins as (
    insert into public.operators (
      slug, canonical_name, short_name, aliases, level, subtype,
      state_code, fips_state, geom, pad_us_mng_name, origin_source
    )
    select
      slug, unit_name,
      regexp_replace(unit_name,
        ' (National Seashore|National Wildlife Refuge|National Lakeshore'
        '|National Recreation Area|National Park|National Monument|National Forest'
        '|National Marine Sanctuary|National Estuarine Research Reserve)$',
        '', 'i'),
      array[unit_name] || coalesce(loc_mangs, array[]::text[]),
      'federal',
      lower(coalesce(
        substring(unit_name from
                  '(National Seashore|National Wildlife Refuge|National Lakeshore'
                  '|National Recreation Area|National Park|National Monument'
                  '|National Park|National Monument|National Forest'
                  '|National Marine Sanctuary)'),
        'federal')),
      p_state, v_state_fp, state_geom,
      mng_names || coalesce(loc_mangs, array[]::text[]),
      'seed_federal_widened'
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
  select count(*) into v_n from ins;
  return v_n;
end $function$;

-- Passes 4-8: thin wrappers around _seed_operators_from_pad_us_unified.
-- Skip-CA logic lives in the orchestrator, not here.

create or replace function public._op_pass4_state(p_state text)
returns integer language sql as $$
  select public._seed_operators_from_pad_us_unified(
    p_state, public._op_state_fp(p_state),
    array['STAT'], 'state', 'state', 'seed_state_pad_us', false);
$$;

create or replace function public._op_pass5_district(p_state text)
returns integer language sql as $$
  select public._seed_operators_from_pad_us_unified(
    p_state, public._op_state_fp(p_state),
    array['DIST'], 'special-district', 'special-district', 'seed_district_pad_us', false);
$$;

create or replace function public._op_pass6a_ngo(p_state text)
returns integer language sql as $$
  select public._seed_operators_from_pad_us_unified(
    p_state, public._op_state_fp(p_state),
    array['NGO'], 'private', 'ngo', 'seed_ngo_pad_us', false);
$$;

create or replace function public._op_pass6b_pvt(p_state text)
returns integer language sql as $$
  select public._seed_operators_from_pad_us_unified(
    p_state, public._op_state_fp(p_state),
    array['PVT'], 'private', 'private', 'seed_pvt_pad_us', true);
$$;

create or replace function public._op_pass7_tribal(p_state text)
returns integer language sql as $$
  select public._seed_operators_from_pad_us_unified(
    p_state, public._op_state_fp(p_state),
    array['TRIB'], 'tribal', 'tribal', 'seed_tribal_pad_us', false);
$$;

create or replace function public._op_pass8_joint(p_state text)
returns integer language sql as $$
  select public._seed_operators_from_pad_us_unified(
    p_state, public._op_state_fp(p_state),
    array['JNT'], 'joint', 'joint', 'seed_joint_pad_us', false);
$$;

-- Pass 9: BIA tribal_lands fallback (tribes PAD-US doesn't tag TRIB)
create or replace function public._op_pass9_bia(p_state text)
returns integer language plpgsql as $function$
declare v_n int := 0; v_state_fp text := public._op_state_fp(p_state);
begin
  with state_geom as (
    select st_collect(geom) g from public.counties where state_fp = v_state_fp
  ),
  new_tribes as (
    select public.slugify_agency(tl.lar_name || ' (' || p_state || ')') as slug,
           max(tl.lar_name) as canonical_name,
           st_multi(st_collectionextract(st_collect(tl.geom), 3)) as geom
      from public.tribal_lands tl, state_geom sg
     where tl.geom is not null
       and st_intersects(tl.geom, sg.g)
       and public._normalize_agency_text(tl.lar_name) is not null
       and not exists (
         select 1 from public.operators o
          where o.is_active and o.level = 'tribal' and o.state_code = p_state
            and public._normalize_agency_text(o.canonical_name)
              = public._normalize_agency_text(tl.lar_name)
       )
     group by public.slugify_agency(tl.lar_name || ' (' || p_state || ')')
  ),
  ins as (
    insert into public.operators (
      slug, canonical_name, short_name, aliases, level, subtype,
      state_code, fips_state, geom, origin_source
    )
    select slug, canonical_name, canonical_name, array[canonical_name],
           'tribal', 'tribal', p_state, v_state_fp, geom, 'seed_tribal_bia'
    from new_tribes
    on conflict (slug) do nothing
    returning 1
  )
  select count(*) into v_n from ins;
  return v_n;
end $function$;

-- Pass 10: OSM operator tags
create or replace function public._op_pass10_osm(p_state text)
returns integer language plpgsql as $function$
declare v_n int := 0; v_state_fp text := public._op_state_fp(p_state);
begin
  with state_counties as (
    select geoid from public.counties where state_fp = v_state_fp
  ),
  osm_ops_raw as (
    select distinct of.tags->>'operator' as op_name
      from public.osm_features of
     where of.county_geoid in (select geoid from state_counties)
       and of.tags ? 'operator'
       and trim(of.tags->>'operator') <> ''
       and public._normalize_agency_text(of.tags->>'operator') is not null
       and not exists (
         select 1 from public.operators o
          where o.is_active and o.state_code = p_state
            and (
              public._normalize_agency_text(o.canonical_name)
                = public._normalize_agency_text(of.tags->>'operator')
              or exists (
                select 1 from unnest(o.aliases) a
                 where public._normalize_agency_text(a)
                   = public._normalize_agency_text(of.tags->>'operator')
              )
            )
       )
  ),
  osm_ops as (
    select public.slugify_agency(op_name || ' (' || p_state || ')') as slug,
           max(op_name) as canonical_name
      from osm_ops_raw
     group by public.slugify_agency(op_name || ' (' || p_state || ')')
  ),
  ins as (
    insert into public.operators (
      slug, canonical_name, short_name, aliases, level, subtype,
      state_code, fips_state, origin_source
    )
    select slug, canonical_name, canonical_name, array[canonical_name],
           public._classify_loc_mang_level(canonical_name, null),
           'osm', p_state, v_state_fp, 'seed_osm_operator'
    from osm_ops
    on conflict (slug) do nothing
    returning 1
  )
  select count(*) into v_n from ins;
  return v_n;
end $function$;

-- Pass 11: BEP claimed_values operator_name
create or replace function public._op_pass11_bep(p_state text)
returns integer language plpgsql as $function$
declare v_n int := 0; v_state_fp text := public._op_state_fp(p_state);
begin
  with bep_ops_raw as (
    select distinct bep.claimed_values->>'operator_name' as op_name
      from public.beach_enrichment_provenance bep
      join public.beaches_gold g on g.fid = bep.gold_fid
     where g.state = p_state and g.is_active
       and bep.claimed_values ? 'operator_name'
       and trim(bep.claimed_values->>'operator_name') <> ''
       and public._normalize_agency_text(bep.claimed_values->>'operator_name') is not null
       and not exists (
         select 1 from public.operators o
          where o.is_active and o.state_code = p_state
            and (
              public._normalize_agency_text(o.canonical_name)
                = public._normalize_agency_text(bep.claimed_values->>'operator_name')
              or exists (
                select 1 from unnest(o.aliases) a
                 where public._normalize_agency_text(a)
                   = public._normalize_agency_text(bep.claimed_values->>'operator_name')
              )
            )
       )
  ),
  bep_ops as (
    select public.slugify_agency(op_name || ' (' || p_state || ')') as slug,
           max(op_name) as canonical_name
      from bep_ops_raw
     group by public.slugify_agency(op_name || ' (' || p_state || ')')
  ),
  ins as (
    insert into public.operators (
      slug, canonical_name, short_name, aliases, level, subtype,
      state_code, fips_state, origin_source
    )
    select slug, canonical_name, canonical_name, array[canonical_name],
           public._classify_loc_mang_level(canonical_name, null),
           'bep', p_state, v_state_fp, 'seed_bep_operator_name'
    from bep_ops
    on conflict (slug) do nothing
    returning 1
  )
  select count(*) into v_n from ins;
  return v_n;
end $function$;

-- Linkage enrichment: append PAD-US loc_mangs to each operator's
-- pad_us_mng_name array when the slugs match. Returns number of
-- operators updated.
create or replace function public._op_linkage(p_state text)
returns integer language plpgsql as $function$
declare v_n int := 0;
begin
  with op_slugs as (
    select o.id, o.slug as op_slug,
           array(select public.slugify_agency(a) from unnest(o.aliases) a) as alias_slugs
      from public.operators o
     where o.state_code = p_state and o.is_active
  ),
  unit_loc_mangs as (
    select distinct
      pu.loc_mang,
      public.slugify_agency(pu.loc_mang) as lm_slug
    from public.pad_us_units pu
    where pu.state = p_state
      and public._normalize_agency_text(pu.loc_mang) is not null
  ),
  matches as (
    select os.id as operator_id,
           array_agg(distinct ulm.loc_mang) as new_strings
      from op_slugs os
      join unit_loc_mangs ulm
        on ulm.lm_slug = os.op_slug
        or ulm.lm_slug = ANY(os.alias_slugs)
     group by os.id
  ),
  upd as (
    update public.operators o
       set pad_us_mng_name = (
         select array_agg(distinct n)
           from unnest(
             coalesce(o.pad_us_mng_name, '{}'::text[]) || m.new_strings
           ) n
       ),
       updated_at = now()
      from matches m
     where o.id = m.operator_id
    returning 1
  )
  select count(*) into v_n from upd;
  return v_n;
end $function$;

-- Pass 12: PER-LEVEL CHUNKED fuzzy canonical consolidation. Internally
-- loops over levels; each level is its own pair self-join. Returns
-- total consolidated count across all levels.
create or replace function public._op_pass12_consolidation(p_state text)
returns integer language plpgsql as $function$
declare
  v_total int := 0;
  v_level_count int := 0;
  v_level text;
begin
  for v_level in
    select distinct level
      from public.operators
     where state_code = p_state and is_active and parent_operator_id is null
  loop
    with pairs as (
      select a.id  as a_id, b.id  as b_id,
             a.aliases as a_aliases, b.aliases as b_aliases,
             a.pad_us_mng_name as a_pmn, b.pad_us_mng_name as b_pmn,
             coalesce(array_length(a.pad_us_mng_name, 1), 0) as a_pad,
             coalesce(array_length(b.pad_us_mng_name, 1), 0) as b_pad,
             coalesce(array_length(a.aliases, 1), 0)         as a_alias,
             coalesce(array_length(b.aliases, 1), 0)         as b_alias
        from public.operators a
        join public.operators b
          on a.state_code = b.state_code
         and a.level      = b.level
         and a.id        < b.id
         and a.is_active and b.is_active
         and a.parent_operator_id is null
         and b.parent_operator_id is null
       where a.state_code = p_state
         and a.level = v_level
         and similarity(a.canonical_name, b.canonical_name) > 0.6
         and exists (
           select 1
             from unnest(a.aliases) aa, unnest(b.aliases) ab
            where public._normalize_agency_text(aa) is not null
              and public._normalize_agency_text(aa) = public._normalize_agency_text(ab)
         )
    ),
    picked as (
      select
        case when (a_pad, a_alias, b_id) >= (b_pad, b_alias, a_id) then a_id else b_id end as canonical_id,
        case when (a_pad, a_alias, b_id) >= (b_pad, b_alias, a_id) then b_id else a_id end as dupe_id,
        case when (a_pad, a_alias, b_id) >= (b_pad, b_alias, a_id) then b_aliases else a_aliases end as dupe_aliases,
        case when (a_pad, a_alias, b_id) >= (b_pad, b_alias, a_id) then b_pmn else a_pmn end as dupe_pmn
        from pairs
    ),
    per_canonical as (
      select canonical_id,
             array_agg(distinct alias) filter (where alias is not null) as add_aliases,
             array_agg(distinct pmn)   filter (where pmn   is not null) as add_pmns,
             array_agg(distinct dupe_id) as dupe_ids
        from picked
        left join lateral unnest(dupe_aliases) alias on true
        left join lateral unnest(coalesce(dupe_pmn, '{}'::text[])) pmn on true
       group by canonical_id
    ),
    merge_canonical as (
      update public.operators op
         set aliases = (
               select array_agg(distinct x)
                 from unnest(op.aliases || coalesce(pc.add_aliases, '{}'::text[])) x
             ),
             pad_us_mng_name = (
               select array_agg(distinct x)
                 from unnest(coalesce(op.pad_us_mng_name, '{}'::text[])
                           || coalesce(pc.add_pmns, '{}'::text[])) x
             ),
             updated_at = now()
        from per_canonical pc
       where op.id = pc.canonical_id
      returning op.id
    ),
    deactivate_dupes as (
      update public.operators op
         set parent_operator_id = pc.canonical_id,
             is_active          = false,
             inactive_reason    = 'deduped_pass12',
             updated_at         = now()
        from per_canonical pc
       where op.id = ANY(pc.dupe_ids)
         and op.is_active
      returning op.id
    )
    select count(*) into v_level_count from deactivate_dupes;
    v_total := v_total + coalesce(v_level_count, 0);
    raise notice 'Pass 12 level=%: consolidated=%', v_level, v_level_count;
  end loop;
  return v_total;
end $function$;

-- ════════════════════════════════════════════════════════════════════════
-- 4. Replace orchestrator FUNCTION with PROCEDURE that COMMITs.
--    Drops the old FUNCTION so the same name now resolves to the
--    procedure. Callers switch from `SELECT * FROM ...` to `CALL ...`.
-- ════════════════════════════════════════════════════════════════════════

drop function if exists public.populate_operators_for_state(text);

create or replace procedure public.populate_operators_for_state(
  p_state text,
  out cities_added       int,
  out counties_added     int,
  out federal_added      int,
  out state_added        int,
  out district_added     int,
  out ngo_added          int,
  out pvt_added          int,
  out tribal_added       int,
  out joint_added        int,
  out bia_added          int,
  out osm_added          int,
  out bep_added          int,
  out linkage_updates    int,
  out consolidated_count int
) language plpgsql as $procedure$
declare
  v_skip_non_ca boolean := (p_state = 'CA');
begin
  if public._op_state_fp(p_state) is null then
    raise exception 'unknown state code: %', p_state;
  end if;

  cities_added       := public._op_pass1_cities(p_state);  commit;
  counties_added     := public._op_pass2_counties(p_state); commit;
  federal_added      := public._op_pass3_federal(p_state); commit;

  if v_skip_non_ca then
    state_added := 0; district_added := 0;
    ngo_added := 0; pvt_added := 0;
    tribal_added := 0; joint_added := 0;
    bia_added := 0; osm_added := 0; bep_added := 0;
  else
    state_added    := public._op_pass4_state(p_state);     commit;
    district_added := public._op_pass5_district(p_state);  commit;
    ngo_added      := public._op_pass6a_ngo(p_state);      commit;
    pvt_added      := public._op_pass6b_pvt(p_state);      commit;
    tribal_added   := public._op_pass7_tribal(p_state);    commit;
    joint_added    := public._op_pass8_joint(p_state);     commit;
    bia_added      := public._op_pass9_bia(p_state);       commit;
    osm_added      := public._op_pass10_osm(p_state);      commit;
    bep_added      := public._op_pass11_bep(p_state);      commit;
  end if;

  linkage_updates    := public._op_linkage(p_state);          commit;
  consolidated_count := public._op_pass12_consolidation(p_state); commit;
end $procedure$;

commit;
