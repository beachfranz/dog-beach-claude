-- 20260513_lookup_operator_unified.sql
--
-- Adds public.lookup_operator(...) — ONE function that resolves any
-- combination of identity signals to a canonical operator_id. Replaces
-- 4 distinct inline lookups in populate_governance_gold.
--
-- BEFORE (inside populate_governance_gold):
--   Source 1 (pad_us)              → inline subquery: slug match
--   Source 2 (city_jurisdiction)   → inline JOIN on jurisdiction_id
--   Source 3 (county_jurisdiction) → inline JOIN on county_geoid
--   Source 4 (polygon_containment) → inline call to resolve_agency
--   Each had its own filter on is_active + parent_operator_id
--
-- AFTER: each source calls lookup_operator(state, ...). One function
-- owns: precedence rules, active/parent filtering, fuzzy fallback.
--
-- Precedence (highest specificity first):
--   1. jurisdiction_id (TIGER place FK)     → operators.level='city'
--   2. county_geoid    (TIGER county FK)    → operators.level='county'
--   3. unit_name slug  (PAD-US Pass 3 seed) → operators.slug exact
--   4. loc_mang slug   (PAD-US Pass 4-8 seed) → operators.slug exact
--   5. agency_name fuzzy → resolve_agency() (trigram, returns op_id)
--   ALL: filter is_active AND parent_operator_id IS NULL.

begin;

create or replace function public.lookup_operator(
  p_state text,
  p_jurisdiction_id bigint default null,
  p_county_geoid    text   default null,
  p_unit_name       text   default null,
  p_loc_mang        text   default null,
  p_agency_name     text   default null,
  p_min_confidence  numeric default 0.65
)
returns bigint
language plpgsql stable as $function$
declare
  v_op_id bigint;
begin
  if p_state is null then return null; end if;

  -- 1. Jurisdiction FK (incorporated city)
  if p_jurisdiction_id is not null then
    select id into v_op_id
      from public.operators
     where state_code = p_state
       and level = 'city'
       and jurisdiction_id = p_jurisdiction_id
       and is_active
       and parent_operator_id is null
     limit 1;
    if v_op_id is not null then return v_op_id; end if;
  end if;

  -- 2. County FK
  if p_county_geoid is not null then
    select id into v_op_id
      from public.operators
     where state_code = p_state
       and level = 'county'
       and county_geoid = p_county_geoid
       and is_active
       and parent_operator_id is null
     limit 1;
    if v_op_id is not null then return v_op_id; end if;
  end if;

  -- 3. unit_name slug (PAD-US Pass 3 federal seed)
  if p_unit_name is not null and trim(p_unit_name) <> '' then
    select id into v_op_id
      from public.operators
     where state_code = p_state
       and slug = public.slugify_agency(p_unit_name || ' (' || p_state || ')')
       and is_active
       and parent_operator_id is null
     limit 1;
    if v_op_id is not null then return v_op_id; end if;
  end if;

  -- 4. loc_mang slug (PAD-US Pass 4-8 state/district/ngo/pvt/tribal/joint seed)
  if p_loc_mang is not null and trim(p_loc_mang) <> '' then
    select id into v_op_id
      from public.operators
     where state_code = p_state
       and slug = public.slugify_agency(p_loc_mang || ' (' || p_state || ')')
       and is_active
       and parent_operator_id is null
     limit 1;
    if v_op_id is not null then return v_op_id; end if;
  end if;

  -- 5. resolve_agency fuzzy (trigram on agency_aliases, already filters
  -- is_active + parent_operator_id is null since 20260513_resolve_agency_active_only)
  if p_agency_name is not null and trim(p_agency_name) <> '' then
    select operator_id into v_op_id
      from public.resolve_agency(p_agency_name, 'lookup_operator', p_state, p_min_confidence)
     order by confidence desc limit 1;
    if v_op_id is not null then return v_op_id; end if;
  end if;

  return null;
end $function$;

grant execute on function public.lookup_operator(text, bigint, text, text, text, text, numeric)
  to anon, authenticated, service_role;

-- ────────────────────────────────────────────────────────────────────────
-- Refactor populate_governance_gold to use the unified lookup
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.populate_governance_gold(p_fid bigint)
returns integer
language plpgsql
set statement_timeout to '600s'
as $function$
declare
  v_total int := 0;
  v_state text;
  v_padus jsonb;
  v_padus_normalized jsonb;
  v_padus_op_id bigint;
begin
  select state into v_state from public.beaches_gold where fid = p_fid;
  if v_state is null then return 0; end if;

  -- ── Source 1: PAD-US spatial containment ──────────────────────────────
  with target as (
    select fid, coalesce(display_name_override, name) as full_name,
           geom, state, geom::geography as geom_geog
      from public.beaches_gold
     where fid = p_fid and geom is not null and is_active
  ),
  candidates as (
    select t.fid, t.state, c.unit_name, c.mng_type, c.mng_name, c.loc_mang,
      st_contains(c.geom, t.geom::geometry) as inside,
      st_distance(t.geom_geog, c.geom_geog) as dist_m,
      st_area(c.geom_geog) as area_m2,
      cardinality(public.shared_name_tokens(t.full_name, c.unit_name)) > 0 as name_match
    from target t
    join public.pad_us_units c on st_dwithin(c.geom, t.geom::geometry, 0.005)
   where st_distance(t.geom_geog, c.geom_geog) <= 500
  ),
  scored as (
    select *,
      case
        when inside     and name_match                       then 0.90
        when not inside and dist_m <= 100 and name_match     then 0.80
        when inside     and not name_match                   then 0.70
        when not inside and dist_m <= 500 and name_match     then 0.60
        when not inside and dist_m <= 100 and not name_match then 0.45
      end as confidence
    from candidates
    where (
      (inside and name_match) or (not inside and dist_m <= 100 and name_match)
      or (inside and not name_match) or (not inside and dist_m <= 500 and name_match)
      or (not inside and dist_m <= 100 and not name_match)
    )
  ),
  best as (
    select * from (
      select *, row_number() over (partition by fid
        order by confidence desc, area_m2 asc) as rnk
        from scored
    ) s where rnk = 1
  ),
  with_op as (
    select *,
      case mng_type
        when 'FED' then 'federal' when 'STAT' then 'state'
        when 'LOC' then 'local' when 'DIST' then 'special_district'
        when 'NGO' then 'nonprofit' when 'PVT' then 'private'
        when 'TRIB' then 'tribal' when 'JNT' then 'joint'
        else 'unknown' end as gov_type,
      -- ★ unified lookup
      public.lookup_operator(
        p_state       => best.state,
        p_unit_name   => best.unit_name,
        p_loc_mang    => best.loc_mang
      ) as operator_id
    from best
  )
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, operator_id, updated_at)
  select fid, 'governance', 'pad_us', confidence,
    jsonb_build_object('type', gov_type,
      'name', public.canonical_agency_name(gov_type, mng_name)),
    operator_id, now()
  from with_op
  where mng_type is not null
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update set confidence = excluded.confidence,
                claimed_values = excluded.claimed_values,
                operator_id = excluded.operator_id,
                updated_at = now(),
                is_canonical = false;
  get diagnostics v_total = row_count;

  -- ── Source 2: city operator via c1_jurisdiction_id FK ─────────────────
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, operator_id, updated_at)
  select g.fid, 'governance', 'city_jurisdiction', 0.85,
         jsonb_build_object('type', 'city',
                            'name', (select canonical_name from public.operators where id = op_id)),
         op_id, now()
    from (
      select g.fid,
             public.lookup_operator(
               p_state           => g.state,
               p_jurisdiction_id => g.c1_jurisdiction_id
             ) as op_id
        from public.beaches_gold g
       where g.fid = p_fid
         and g.is_active
         and g.c1_jurisdiction_id is not null
    ) s
    join public.beaches_gold g on g.fid = s.fid
   where s.op_id is not null
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update set confidence = excluded.confidence,
                claimed_values = excluded.claimed_values,
                operator_id = excluded.operator_id,
                updated_at = now(),
                is_canonical = false;

  -- ── Source 3: county operator via county_geoid FK ─────────────────────
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, operator_id, updated_at)
  select g.fid, 'governance', 'county_jurisdiction', 0.70,
         jsonb_build_object('type', 'county',
                            'name', (select canonical_name from public.operators where id = op_id)),
         op_id, now()
    from (
      select g.fid,
             public.lookup_operator(
               p_state        => g.state,
               p_county_geoid => g.county_geoid
             ) as op_id
        from public.beaches_gold g
       where g.fid = p_fid
         and g.is_active
         and g.county_geoid is not null
    ) s
    join public.beaches_gold g on g.fid = s.fid
   where s.op_id is not null
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update set confidence = excluded.confidence,
                claimed_values = excluded.claimed_values,
                operator_id = excluded.operator_id,
                updated_at = now(),
                is_canonical = false;

  -- ── Source 4: polygon_containment owner-name fallback ─────────────────
  -- Reads canonical polygon_containment BEP → uses _normalize_pad_us_manager
  -- to derive the agency name → unified lookup (which itself tries fuzzy
  -- resolve_agency as the last resort). Requires _resolve_polygon_containment
  -- to have run first.
  select claimed_values into v_padus
    from public.beach_enrichment_provenance
   where gold_fid = p_fid
     and field_group = 'polygon_containment'
     and is_canonical = true
     and claimed_values->>'polygon_kind' = 'pad_us_unit'
   limit 1;

  if v_padus is not null then
    if v_padus->>'own_name' = 'DESG' then
      v_padus_normalized := public._normalize_pad_us_manager(
        v_padus->>'mng_type', v_padus->>'mng_name',
        v_padus->>'polygon_name', v_state);
    else
      v_padus_normalized := public._normalize_pad_us_manager(
        v_padus->>'own_type', v_padus->>'own_name',
        v_padus->>'polygon_name', v_state);
    end if;

    -- ★ unified lookup — agency_name fuzzy fallback path
    v_padus_op_id := public.lookup_operator(
      p_state       => v_state,
      p_agency_name => v_padus_normalized->>'name'
    );

    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, is_canonical,
       updated_at, claimed_values, operator_id)
    values (p_fid, 'governance', 'polygon_containment_governance_v1', 0.75, false,
            now(),
            v_padus_normalized || jsonb_build_object(
              'pad_us_unit_id', v_padus->>'polygon_id',
              'pad_us_unit_name', v_padus->>'polygon_name'),
            v_padus_op_id)
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update set confidence = excluded.confidence,
                  claimed_values = excluded.claimed_values,
                  operator_id = excluded.operator_id,
                  updated_at = now(),
                  is_canonical = false;
  end if;

  return v_total;
end $function$;

commit;
