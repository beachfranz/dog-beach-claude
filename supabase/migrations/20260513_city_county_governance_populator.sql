-- 20260513_city_county_governance_populator.sql
--
-- Adds populate_governance_from_city_county_gold — the missing link
-- between TIGER FKs (c1_jurisdiction_id, county_geoid) and the city/
-- county operators seeded by Pass 1+2 of operator_seeding.
--
-- Pre-existing populators that emit `governance` BEP rows with
-- operator_id set: only populate_from_pad_us_gold (federal/state).
-- City/county operators were in the operators table but had no
-- attribution path → systematically excluded from Phase 26's
-- "definitively assigned" candidate set.
--
-- This populator joins on FKs (not spatial; the spatial work was
-- already done by populate_jurisdictions_containment_gold and the
-- county-geoid promoter). Cheap and deterministic.
--
-- Confidence scoring:
--   city_jurisdiction:   0.85 (incorporated city — strong signal)
--   county_jurisdiction: 0.70 (county — weaker, fallback)
-- Both LOWER than pad_us governance (0.90) — federal/state designation
-- inside a city still wins canonical (e.g. NWR inside city limits).
--
-- This populator is added to the promote_to_gold chain so it runs
-- automatically on every beach promotion.

begin;

create or replace function public.populate_governance_from_city_county_gold(
  p_fid bigint default null
) returns integer
language plpgsql
set statement_timeout to '300s'
as $function$
declare rows_touched int;
begin
  with city_match as (
    -- city operator via c1_jurisdiction_id FK
    select g.fid, op.id as operator_id, op.canonical_name as op_name
      from public.beaches_gold g
      join public.operators op
        on op.jurisdiction_id = g.c1_jurisdiction_id
       and op.level = 'city'
       and op.is_active
     where (p_fid is null or g.fid = p_fid)
       and g.is_active
       and g.c1_jurisdiction_id is not null
  ),
  county_match as (
    -- county operator via county_geoid FK
    select g.fid, op.id as operator_id, op.canonical_name as op_name
      from public.beaches_gold g
      join public.operators op
        on op.county_geoid = g.county_geoid
       and op.level = 'county'
       and op.is_active
     where (p_fid is null or g.fid = p_fid)
       and g.is_active
       and g.county_geoid is not null
  ),
  upsert_city as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, operator_id, updated_at)
    select fid, 'governance', 'city_jurisdiction', 0.85,
           jsonb_build_object('type', 'city', 'name', op_name),
           operator_id, now()
      from city_match
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          operator_id    = excluded.operator_id,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  ),
  upsert_county as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, operator_id, updated_at)
    select fid, 'governance', 'county_jurisdiction', 0.70,
           jsonb_build_object('type', 'county', 'name', op_name),
           operator_id, now()
      from county_match
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          operator_id    = excluded.operator_id,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from upsert_city union all select * from upsert_county
  ) _;
  return rows_touched;
end $function$;

grant execute on function public.populate_governance_from_city_county_gold(bigint)
  to anon, authenticated, service_role;

-- ────────────────────────────────────────────────────────────────────────
-- Add to promote_to_gold chain. The existing promote_to_gold function
-- iterates per fid and calls a sequence of populators. Insert the new
-- populator after populate_from_pad_us_gold (so PAD-US runs first,
-- city/county supplement it).
--
-- Rather than rewrite promote_to_gold's 100-line body, we re-define it
-- by reading the current body and splicing in the new call. The
-- structure is `foreach fid_iter in array p_fids loop ... end loop`.
-- ────────────────────────────────────────────────────────────────────────

-- Get the current promote_to_gold body and patch it programmatically
-- isn't great via raw SQL. Instead, re-declare with the new populator
-- inserted. Keep all other populators identical.

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
      coalesce(
        public._infer_state_from_county_fips(t.county_fips),
        public._infer_state_from_county(t.county_name)
      ),
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
    perform public.populate_from_pad_us_gold(fid_iter);
    -- ★ NEW 2026-05-13: city/county governance attribution via FK joins
    perform public.populate_governance_from_city_county_gold(fid_iter);
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

commit;
