-- 20260513_unified_governance_populator.sql
--
-- Collapses 3 separate governance populators into 1 entry point.
-- Eliminates the "remember to add it to BOTH promote_to_gold AND
-- refire_bep_cascade" trap that caused today's OR regression.
--
-- BEFORE: 3 populators, each emitting governance BEP rows from a
-- different signal:
--   populate_from_pad_us_gold              → source='pad_us' + access
--   populate_governance_from_city_county_gold → source='city_jurisdiction'|'county_jurisdiction'
--   populate_governance_from_polygon_gold  → source='polygon_containment_governance_v1'
-- Each caller (promote_to_gold, refire_bep_cascade) maintained its
-- own list of which to call. Adding a 4th source = remember to wire
-- it into both call sites or risk silent attribution loss.
--
-- AFTER: ONE entry point — populate_governance_gold(fid) — that
-- internally emits all governance source rows. Callers only need to
-- call this one function. Adding a new source = one CTE + one INSERT
-- here, no caller updates required.
--
-- populate_from_pad_us_gold is preserved but stripped to ONLY emit
-- the 'access' field_group (its non-governance responsibility).
-- The other two populators are dropped.

begin;

-- ────────────────────────────────────────────────────────────────────────
-- 1. New unified governance populator
--
-- Caller contract: _resolve_polygon_containment(fid) MUST run before
-- this function, because source 4 (polygon_containment_governance_v1)
-- reads the canonical polygon_containment BEP row.
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
  -- Direct spatial match; operator_id via slug lookup on unit_name (Pass 3
  -- federal seed) or loc_mang (Passes 4-8 state-level seed). Confidence
  -- scales with containment + name match.
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
      (select o.id from public.operators o
        where o.is_active and o.parent_operator_id is null
          and o.state_code = best.state
          and (
            (best.unit_name is not null and trim(best.unit_name) <> '' and
             o.slug = public.slugify_agency(best.unit_name || ' (' || best.state || ')'))
            or (best.loc_mang is not null and trim(best.loc_mang) <> '' and
                o.slug = public.slugify_agency(best.loc_mang || ' (' || best.state || ')'))
          )
        limit 1) as operator_id
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
         jsonb_build_object('type', 'city', 'name', op.canonical_name),
         op.id, now()
    from public.beaches_gold g
    join public.operators op
      on op.jurisdiction_id = g.c1_jurisdiction_id
     and op.level = 'city'
     and op.is_active
     and op.parent_operator_id is null
   where g.fid = p_fid
     and g.is_active
     and g.c1_jurisdiction_id is not null
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
         jsonb_build_object('type', 'county', 'name', op.canonical_name),
         op.id, now()
    from public.beaches_gold g
    join public.operators op
      on op.county_geoid = g.county_geoid
     and op.level = 'county'
     and op.is_active
     and op.parent_operator_id is null
   where g.fid = p_fid
     and g.is_active
     and g.county_geoid is not null
  on conflict (gold_fid, field_group, source) where gold_fid is not null
  do update set confidence = excluded.confidence,
                claimed_values = excluded.claimed_values,
                operator_id = excluded.operator_id,
                updated_at = now(),
                is_canonical = false;

  -- ── Source 4: polygon_containment fallback via resolve_agency ─────────
  -- Reads the CANONICAL polygon_containment BEP row → uses
  -- _normalize_pad_us_manager + resolve_agency to find the operator
  -- by name when slug-match (source 1) didn't find one. Requires
  -- _resolve_polygon_containment to have run first.
  --
  -- Idempotency for this source: ON CONFLICT DO UPDATE on
  -- (gold_fid, field_group, source). Old explicit-delete from
  -- populate_governance_from_polygon_gold removed.
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

    select operator_id into v_padus_op_id
      from public.resolve_agency(v_padus_normalized->>'name', 'polygon_containment',
                                 v_state, 0.65)
     order by confidence desc, method limit 1;

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

grant execute on function public.populate_governance_gold(bigint)
  to anon, authenticated, service_role;

-- ────────────────────────────────────────────────────────────────────────
-- 2. Strip populate_from_pad_us_gold to access-only.
--    (Previously emitted governance + access; now only access.)
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.populate_from_pad_us_gold(p_fid bigint default null)
returns integer
language plpgsql
set statement_timeout to '600s'
as $function$
declare rows_touched int;
begin
  with target as (
    select fid, geom, state, geom::geography as geom_geog
      from public.beaches_gold
     where (p_fid is null or fid = p_fid)
       and geom is not null and is_active = true
  ),
  candidates as (
    select t.fid, t.state, c.unit_name, c.mng_type, c.mng_name,
      c.raw_attrs->>'Pub_Access' as pub_access_code,
      st_contains(c.geom, t.geom::geometry) as inside,
      st_distance(t.geom_geog, c.geom_geog) as dist_m,
      st_area(c.geom_geog) as area_m2
    from target t
    join public.pad_us_units c on st_dwithin(c.geom, t.geom::geometry, 0.005)
   where st_distance(t.geom_geog, c.geom_geog) <= 500
  ),
  best as (
    select * from (
      select *, row_number() over (
        partition by fid
        order by case when inside then 0 else 1 end, dist_m, area_m2
      ) as rnk from candidates
    ) s where rnk = 1
  ),
  with_access as (
    select fid,
      case pub_access_code
        when 'OA' then 'public' when 'RA' then 'restricted'
        when 'XA' then 'private' when 'UK' then 'unknown'
        else case mng_type
          when 'PVT' then 'private' when 'TRIB' then 'restricted'
          when 'UNK' then 'unknown' else 'public' end
      end as access_status
    from best
  ),
  upsert_access as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'access', 'pad_us', 0.70,
      jsonb_build_object('status', access_status), now()
    from with_access
    where access_status is not null
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update set confidence = excluded.confidence,
                  claimed_values = excluded.claimed_values,
                  updated_at = now(),
                  is_canonical = false
    returning 1
  )
  select count(*) into rows_touched from upsert_access;
  return rows_touched;
end $function$;

-- ────────────────────────────────────────────────────────────────────────
-- 3. Drop the two now-subsumed populators.
-- ────────────────────────────────────────────────────────────────────────

drop function if exists public.populate_governance_from_city_county_gold(bigint);
drop function if exists public.populate_governance_from_polygon_gold(bigint);

-- ────────────────────────────────────────────────────────────────────────
-- 4. Update promote_to_gold: replace 3 calls with 1.
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.promote_to_gold(p_fids bigint[], p_score boolean default false, p_publish boolean default true)
returns table(rows_promoted bigint, rows_already_in_gold bigint, containment_evidence bigint, source_evidence bigint, beach_dog_policy_changed bigint, beach_amenities_changed bigint, noaa_assigned bigint, slugs_assigned bigint)
language plpgsql
as $function$
declare promoted int := 0; existing int := 0; fid_iter bigint;
        contain_ev int := 0; source_ev int := 0;
        bdp_changed int := 0; ba_changed int := 0;
        noaa_count int := 0; slug_count int := 0;
        new_fids bigint[];
begin
  if p_fids is null or array_length(p_fids, 1) is null then
    return query select 0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint;
    return;
  end if;

  with to_promote as (
    select a.* from public.arena a
     where a.fid = any(p_fids) and a.is_active = true
       and not exists (select 1 from public.beaches_gold g where g.fid = a.fid)
       and not exists (
         select 1 from public.beaches_gold g
           join public.arena a2 on a2.fid = g.fid
          where a2.group_id = coalesce(a.group_id, a.fid)
            and g.is_active and g.fid <> a.fid
       )
  ),
  to_promote_with_canonical as (
    select t.*,
           coalesce(osm.nav_lat, t.nav_lat, t.lat) as canonical_lat,
           coalesce(osm.nav_lon, t.nav_lon, t.lon) as canonical_lon
      from to_promote t
      left join lateral (
        select nav_lat, nav_lon
          from public.arena
         where group_id = coalesce(t.group_id, t.fid)
           and source_code = 'osm' and is_active = true
           and nav_lat is not null and nav_lon is not null
         limit 1
      ) osm on true
  ),
  ins as (
    insert into public.beaches_gold
      (fid, location_id, name, lat, lon, county_name, county_fips,
       source_code, source_id, group_id,
       nav_lat, nav_lon, nav_source, name_source, park_name, state, promoted_from, promoted_at,
       is_active, noaa_station_id, timezone, open_time, close_time, geom)
    select t.fid, public._make_location_slug(t.name, t.county_name, t.fid),
      t.name, t.canonical_lat, t.canonical_lon, t.county_name, t.county_fips,
      t.source_code, t.source_id, coalesce(t.group_id, t.fid),
      t.nav_lat, t.nav_lon, t.nav_source, t.name_source, t.park_name,
      coalesce(public._infer_state_from_county_fips(t.county_fips),
               public._infer_state_from_county(t.county_name)),
      'promote_to_gold_v3', now(), true,
      public._nearest_noaa_station(st_setsrid(st_makepoint(t.canonical_lon, t.canonical_lat), 4326)::geography),
      'America/Los_Angeles', '05:00', '22:00',
      st_setsrid(st_makepoint(t.canonical_lon, t.canonical_lat), 4326)
    from to_promote_with_canonical t
    returning fid, location_id, noaa_station_id
  )
  select array_agg(fid), count(*), count(*) filter (where location_id is not null),
         count(*) filter (where noaa_station_id is not null)
    into new_fids, promoted, slug_count, noaa_count from ins;
  existing := array_length(p_fids, 1) - promoted;

  foreach fid_iter in array p_fids loop
    perform public.populate_polygon_containment_gold(fid_iter);
    perform public.populate_from_cpad_gold(fid_iter);
    perform public.populate_from_pad_us_gold(fid_iter);              -- access-only now
    perform public.populate_from_park_operators_gold(fid_iter);
    perform public.populate_from_operators_gold(fid_iter);
    perform public.populate_from_state_default_gold(fid_iter);
    perform public.populate_from_research_gold(fid_iter);
    perform public.populate_from_park_url_gold(fid_iter);
    perform public.populate_from_park_url_governance_gold(fid_iter);
    perform public.populate_from_unified_v1_gold(fid_iter);
    perform public.populate_from_city_dog_policy_gold(fid_iter);
    perform public.populate_from_county_dog_policy_gold(fid_iter);
    perform public._emit_evidence_from_osm_amenities(fid_iter);

    perform public._resolve_polygon_containment(fid_iter);
    -- ★ ONE governance entry point — replaces 3 prior populator calls
    perform public.populate_governance_gold(fid_iter);
    perform public._resolve_governance_gold(fid_iter);
    perform public._resolve_dogs_gold(fid_iter);
    perform public._resolve_practical_gold(fid_iter);
    perform public._resolve_field_group_gold('access', fid_iter);
    perform public.compute_beach_field_consensus(fid_iter);
  end loop;

  select count(*) into contain_ev from public.beach_enrichment_provenance
   where field_group = 'polygon_containment' and gold_fid = any(p_fids);
  select count(*) into source_ev from public.beach_enrichment_provenance
   where field_group in ('dogs','practical','access','governance')
     and gold_fid = any(p_fids);

  foreach fid_iter in array p_fids loop
    perform public.promote_canonical_to_consumer_tables(fid_iter);
  end loop;

  select count(*) into bdp_changed from public.beach_dog_policy
   where arena_group_id = any(p_fids) and source = 'auto_promoted_from_consensus';
  select count(*) into ba_changed from public.beach_amenities
   where arena_group_id = any(p_fids) and source = 'auto_promoted_from_consensus';

  return query select promoted::bigint, existing::bigint, contain_ev::bigint, source_ev::bigint,
                      bdp_changed::bigint, ba_changed::bigint, noaa_count::bigint, slug_count::bigint;
end;
$function$;

-- ────────────────────────────────────────────────────────────────────────
-- 5. Update refire_bep_cascade: replace 3 calls with 1.
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.refire_bep_cascade(p_fids bigint[])
returns table(fids_processed bigint, bep_rows_deleted bigint, bep_rows_after bigint)
language plpgsql
security definer
set search_path to 'public', 'pg_catalog'
as $function$
declare fid_iter bigint;
        v_processed bigint := 0;
        v_pre_count bigint;
        v_post_count bigint;
        v_run_id uuid := gen_random_uuid();
begin
  if p_fids is null or array_length(p_fids, 1) is null then
    return query select 0::bigint, 0::bigint, 0::bigint;
    return;
  end if;

  insert into public.refire_audit
        (refire_run_id, fid, phase, cpad_unit_id, c1_jurisdiction_id, county_geoid)
  select v_run_id, fid, 'before',
         cpad_unit_id, c1_jurisdiction_id, county_geoid
    from public.beaches_gold
   where fid = any(p_fids);

  select count(*) into v_pre_count from public.beach_enrichment_provenance
   where gold_fid = any(p_fids)
     and field_group in ('dogs','practical','governance','access','polygon_containment');

  foreach fid_iter in array p_fids loop
    delete from public.beach_enrichment_provenance
     where gold_fid = fid_iter
       and field_group in ('dogs','practical','governance','access','polygon_containment');

    perform public.populate_polygon_containment_gold(fid_iter);
    perform public.populate_from_cpad_gold(fid_iter);
    perform public.populate_from_pad_us_gold(fid_iter);             -- access-only
    perform public.populate_from_park_operators_gold(fid_iter);
    perform public.populate_from_operators_gold(fid_iter);
    perform public.populate_from_state_default_gold(fid_iter);
    perform public.populate_from_research_gold(fid_iter);
    perform public.populate_from_park_url_gold(fid_iter);
    perform public.populate_from_park_url_governance_gold(fid_iter);
    perform public.populate_from_unified_v1_gold(fid_iter);
    perform public.populate_from_city_dog_policy_gold(fid_iter);
    perform public.populate_from_county_dog_policy_gold(fid_iter);
    perform public._emit_evidence_from_osm_amenities(fid_iter);
    perform public.populate_accessibility_from_osm_amenities(fid_iter);

    perform public._resolve_polygon_containment(fid_iter);
    -- ★ ONE governance entry point — replaces 3 prior populator calls
    perform public.populate_governance_gold(fid_iter);

    perform public._resolve_governance_gold(fid_iter);
    perform public._resolve_dogs_gold(fid_iter);
    perform public._resolve_practical_gold(fid_iter);
    perform public._resolve_field_group_gold('access', fid_iter);
    perform public._resolve_field_group_gold('accessibility', fid_iter);

    perform public.compute_beach_field_consensus(fid_iter);
    perform public.promote_canonical_to_consumer_tables(fid_iter);
    perform public.promote_canonical_accessibility_to_beach_amenities(fid_iter);

    v_processed := v_processed + 1;
  end loop;

  insert into public.refire_audit
        (refire_run_id, fid, phase, cpad_unit_id, c1_jurisdiction_id, county_geoid)
  select v_run_id, fid, 'after',
         cpad_unit_id, c1_jurisdiction_id, county_geoid
    from public.beaches_gold
   where fid = any(p_fids);

  select count(*) into v_post_count from public.beach_enrichment_provenance
   where gold_fid = any(p_fids)
     and field_group in ('dogs','practical','governance','access','polygon_containment');

  return query select v_processed, v_pre_count, v_post_count;
end;
$function$;

commit;
