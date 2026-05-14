-- 20260513_populate_operators_for_state_v2.sql
--
-- Rewrites populate_operators_for_state(state) from 3 passes (cities,
-- counties, federal-coastal) to 11 passes covering every operator level
-- present in PAD-US plus OSM operator tags and BEP-mentioned operator
-- names. Closes the structural gap where state-agency / district / NGO /
-- private / tribal / joint PAD-US units have no operator on any path.
--
-- Design decisions (Franz 2026-05-13):
--   - PVT scope: beach-relevant only (1,887 distinct CA/OR/WA otherwise)
--   - Skip-CA: Passes 4-11 are no-ops when p_state='CA' (CPAD covers it)
--   - Include OSM operator tags (Pass 10) and BEP claimed_values
--     operator_name (Pass 11 — substituted for policy_research_extractions
--     since that table has no operator_name column)
--   - Classifier override: loc_mang text patterns win over mng_type for
--     level assignment (catches OR State Parks tagged mng_type='LOC')
--
-- Linkage enrichment (after all passes):
--   For every operator in the state, scan PAD-US loc_mang strings whose
--   slugify(loc_mang) matches the operator's slug or any alias's slug.
--   Append matched loc_mang values to operators.pad_us_mng_name array.
--   This closes the gap where Pass 1/2 cities/counties had no PAD-US
--   linking even though PAD-US polygons name those entities.

begin;

-- ── Indexes that Pass 12 (fuzzy consolidation) needs ──────────────
-- Trigram index on canonical_name + alias_normalize lookup speed.
-- Without these, the similarity() in Pass 12 falls back to seq-scan,
-- which on a 500+-row level (e.g. WA federal post-build) produces
-- the pool-exhaustion / Cloudflare 524 pattern.
create extension if not exists pg_trgm;
create index if not exists operators_canonical_name_trgm_idx
  on public.operators using gin (canonical_name gin_trgm_ops);


-- ── Helper: seed-from-pad-us-by-mng-type (used by Passes 4-7) ───────

create or replace function public._seed_operators_from_pad_us_unified(
  p_state               text,
  p_state_fp            text,
  p_mng_types           text[],
  p_target_level        text,
  p_subtype             text,
  p_origin_source       text,
  p_beach_touching_only boolean default false
) returns integer language plpgsql as $function$
declare v_count int := 0;
begin
  -- Group by the FINAL slug (not normalized loc_mang) so name variants
  -- that collapse to the same slug ("City of X" / "X, City of") get
  -- combined before INSERT — prevents 'ON CONFLICT cannot affect row twice'.
  with ranked as (
    select public.slugify_agency(pu.loc_mang || ' (' || p_state || ')') as slug,
           max(pu.loc_mang)                as canonical_name,
           array_agg(distinct pu.loc_mang) as aliases,
           array_agg(distinct pu.loc_mang) as loc_mangs,
           st_multi(st_union(pu.geom))     as state_geom
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
            where g.state = p_state and g.is_active and g.is_scoreable
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
      slug,
      canonical_name,
      canonical_name,
      aliases,
      p_target_level,
      p_subtype,
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

grant execute on function public._seed_operators_from_pad_us_unified(text,text,text[],text,text,text,boolean)
  to anon, authenticated, service_role;


-- ── Main: populate_operators_for_state (11-pass rewrite) ────────────

drop function if exists public.populate_operators_for_state(text);

create or replace function public.populate_operators_for_state(p_state text)
returns table(
  cities_added       integer,
  counties_added     integer,
  federal_added      integer,
  state_added        integer,
  district_added     integer,
  ngo_added          integer,
  pvt_added          integer,
  tribal_added       integer,
  joint_added        integer,
  bia_added          integer,
  osm_added          integer,
  bep_added          integer,
  consolidated_count integer
) language plpgsql as $function$
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
  v_skip_non_ca_passes boolean := (p_state = 'CA');
  v_cities int := 0;  v_counties int := 0;  v_federal int := 0;
  v_state int := 0;   v_district int := 0;
  v_ngo int := 0;     v_pvt int := 0;
  v_tribal int := 0;  v_joint int := 0;
  v_bia int := 0;     v_osm int := 0;       v_bep int := 0;
  v_consolidated int := 0;
  v_level_consolidated int := 0;
  v_level text;
begin
  if v_state_fp is null then
    raise exception 'unknown state code: %', p_state;
  end if;

  ------------------------------------------------------------------------
  -- Pass 1: cities (TIGER incorporated places — place_type LIKE 'C%')
  ------------------------------------------------------------------------
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

  ------------------------------------------------------------------------
  -- Pass 2: counties (TIGER counties under this state)
  ------------------------------------------------------------------------
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

  ------------------------------------------------------------------------
  -- Pass 3: federal — WIDENED. All mng_type='FED'. No name pattern filter.
  -- Grouped by SLUG (not unit_name) to absorb name variants that collapse
  -- to the same slug — prevents 'ON CONFLICT cannot affect row twice'.
  ------------------------------------------------------------------------
  with ranked as (
    select public.slugify_agency(pu.unit_name || ' (' || p_state || ')') as slug,
           max(pu.unit_name) as unit_name,
           array_agg(distinct coalesce(pu.mng_name, 'FED')) as mng_names,
           array_agg(distinct pu.loc_mang) filter (
             where public._normalize_agency_text(pu.loc_mang) is not null
           ) as loc_mangs,
           st_multi(st_union(pu.geom)) as state_geom
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
      slug,
      unit_name,
      regexp_replace(unit_name,
        ' (National Seashore|National Wildlife Refuge|National Lakeshore'
        '|National Recreation Area|National Park|National Monument|National Forest'
        '|National Marine Sanctuary|National Estuarine Research Reserve)$',
        '', 'i'),
      array[unit_name] || coalesce(loc_mangs, array[]::text[]),
      'federal',
      lower(coalesce(
        substring(unit_name from '(National Seashore|National Wildlife Refuge'
                                '|National Lakeshore|National Recreation Area'
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
  select count(*) into v_federal from ins;

  ------------------------------------------------------------------------
  -- Passes 4-11: only run for non-CA states (CA has legacy CPAD seeding)
  ------------------------------------------------------------------------
  if not v_skip_non_ca_passes then

    -- Pass 4: state agencies (STAT mng_type OR classifier returns 'state')
    v_state := public._seed_operators_from_pad_us_unified(
      p_state, v_state_fp, array['STAT'], 'state', 'state', 'seed_state_pad_us', false);

    -- Pass 5: special districts (DIST mng_type OR classifier 'special-district')
    v_district := public._seed_operators_from_pad_us_unified(
      p_state, v_state_fp, array['DIST'], 'special-district', 'special-district',
      'seed_district_pad_us', false);

    -- Pass 6a: NGO — all
    v_ngo := public._seed_operators_from_pad_us_unified(
      p_state, v_state_fp, array['NGO'], 'private', 'ngo', 'seed_ngo_pad_us', false);

    -- Pass 6b: PVT — beach-relevant only
    v_pvt := public._seed_operators_from_pad_us_unified(
      p_state, v_state_fp, array['PVT'], 'private', 'private', 'seed_pvt_pad_us', true);

    -- Pass 7: TRIB — all
    v_tribal := public._seed_operators_from_pad_us_unified(
      p_state, v_state_fp, array['TRIB'], 'tribal', 'tribal', 'seed_tribal_pad_us', false);

    -- Pass 8: JNT (joint management) — kept as single operator per joint
    -- loc_mang string for v1; can be split into constituent operators later.
    v_joint := public._seed_operators_from_pad_us_unified(
      p_state, v_state_fp, array['JNT'], 'joint', 'joint', 'seed_joint_pad_us', false);

    --------------------------------------------------------------------
    -- Pass 9: BIA tribal_lands fallback (tribes PAD-US doesn't tag TRIB)
    -- Group by slug to dedupe lar_name variants that collapse to same slug.
    --------------------------------------------------------------------
    with state_geom as (
      select st_collect(geom) g from public.counties where state_fp = v_state_fp
    ),
    new_tribes as (
      select public.slugify_agency(tl.lar_name || ' (' || p_state || ')') as slug,
             max(tl.lar_name) as canonical_name,
             st_multi(st_union(tl.geom)) as geom
        from public.tribal_lands tl, state_geom sg
       where tl.geom is not null
         and st_intersects(tl.geom, sg.g)
         and public._normalize_agency_text(tl.lar_name) is not null
         and not exists (
           select 1 from public.operators o
            where o.is_active and o.level = 'tribal'
              and o.state_code = p_state
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
      select
        slug,
        canonical_name, canonical_name, array[canonical_name],
        'tribal', 'tribal',
        p_state, v_state_fp, geom,
        'seed_tribal_bia'
      from new_tribes
      on conflict (slug) do nothing
      returning 1
    )
    select count(*) into v_bia from ins;

    --------------------------------------------------------------------
    -- Pass 10: OSM operator tags — distinct tags->>'operator' values
    -- inside the state's counties that don't yet match an operator.
    -- Group by slug to dedupe variants.
    --------------------------------------------------------------------
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
    osm_ops AS (
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
      select
        slug,
        canonical_name, canonical_name, array[canonical_name],
        public._classify_loc_mang_level(canonical_name, null),
        'osm',
        p_state, v_state_fp,
        'seed_osm_operator'
      from osm_ops
      on conflict (slug) do nothing
      returning 1
    )
    select count(*) into v_osm from ins;

    --------------------------------------------------------------------
    -- Pass 11: BEP claimed_values operator_name (substituted from
    -- policy_research_extractions which has no operator_name column).
    -- Catches operator strings written by LLM/research/curator paths
    -- that aren't yet first-class operators. Group by slug to dedupe.
    --------------------------------------------------------------------
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
    bep_ops AS (
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
      select
        slug,
        canonical_name, canonical_name, array[canonical_name],
        public._classify_loc_mang_level(canonical_name, null),
        'bep',
        p_state, v_state_fp,
        'seed_bep_operator_name'
      from bep_ops
      on conflict (slug) do nothing
      returning 1
    )
    select count(*) into v_bep from ins;

  end if;  -- /skip_non_ca_passes

  ------------------------------------------------------------------------
  -- Linkage enrichment: scan PAD-US loc_mang values whose slug matches
  -- any operator's slug or alias-slug, append to pad_us_mng_name array.
  -- Runs for ALL states (including CA) — never inserts new operators,
  -- only enriches existing ones.
  ------------------------------------------------------------------------
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
  )
  update public.operators o
     set pad_us_mng_name = (
       select array_agg(distinct n)
         from unnest(
           coalesce(o.pad_us_mng_name, '{}'::text[]) || m.new_strings
         ) n
     ),
     updated_at = now()
    from matches m
   where o.id = m.operator_id;

  ------------------------------------------------------------------------
  -- Pass 12: PER-LEVEL CHUNKED fuzzy canonical consolidation.
  -- Per the chunked-at-design-time standard (feedback_chunked_subprocess.md):
  -- the pair self-join is partitioned by `level`, so the working set per
  -- iteration is bounded to a single level's operators. WA's federal level
  -- alone can be 500+ rows; an all-levels join is 125K+ pairs and exhausts
  -- the connection pool / gateway (524).
  --
  -- Trigram index on operators.canonical_name (created at top of this
  -- migration) makes the similarity() comparisons index-backed instead of
  -- seq-scan. Without that index, each level's pair sweep degrades to O(n²).
  --
  -- Within a level: find pairs where similarity(canonical_name) > 0.6
  -- AND they share at least one alias whose normalized form matches.
  -- Pick canonical by pad_us_mng_name array length (ties: more aliases,
  -- then lower id). Merge dupe's aliases + pad_us_mng_name into canonical,
  -- mark dupe inactive with parent_operator_id pointing at canonical.
  -- Single-pass per level — chains (A~B, B~C) may need a follow-up run.
  -- Idempotent.
  ------------------------------------------------------------------------
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
        case when (a_pad, a_alias, b_id) >= (b_pad, b_alias, a_id)
             then a_id else b_id end as canonical_id,
        case when (a_pad, a_alias, b_id) >= (b_pad, b_alias, a_id)
             then b_id else a_id end as dupe_id,
        case when (a_pad, a_alias, b_id) >= (b_pad, b_alias, a_id)
             then b_aliases else a_aliases end as dupe_aliases,
        case when (a_pad, a_alias, b_id) >= (b_pad, b_alias, a_id)
             then b_pmn else a_pmn end as dupe_pmn
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
    select count(*) into v_level_consolidated from deactivate_dupes;

    v_consolidated := v_consolidated + coalesce(v_level_consolidated, 0);
    raise notice 'Pass 12 level=%: consolidated=%', v_level, v_level_consolidated;
  end loop;

  return query select v_cities, v_counties, v_federal,
    v_state, v_district, v_ngo, v_pvt, v_tribal, v_joint,
    v_bia, v_osm, v_bep, v_consolidated;
end $function$;

grant execute on function public.populate_operators_for_state(text)
  to anon, authenticated, service_role;

commit;
