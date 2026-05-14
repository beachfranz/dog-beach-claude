-- 20260513_collapse_to_build_beach_evidence.sql
--
-- Collapses promote_to_gold's per-fid evidence loop AND refire_bep_cascade
-- into a single canonical function: build_beach_evidence(p_fid).
--
-- BEFORE: two functions, same body, drifted apart (e.g. promote_to_gold
-- had populate_governance_from_city_county_gold but refire_bep_cascade
-- didn't, causing the OR regression earlier today).
--
-- AFTER: one body, called from both places. Promote_to_gold owns
-- INSERT + one-time-setup (slug, noaa_station, address backfill);
-- everything per-fid downstream goes through build_beach_evidence.
--
-- build_beach_evidence is idempotent — first-time build and Nth-rebuild
-- are the same call. It clears regenerable BEP rows then runs the full
-- chain (populators → resolvers → consensus → consumer promote).
--
-- refire_bep_cascade(p_fids[]) becomes a thin wrapper that loops over
-- the array calling build_beach_evidence(fid) per element. Preserves
-- backward compat for: tg_invalidate_dog_policy_on_geom_change trigger,
-- legacy one-off scripts, run_state_pipeline.py callers.

begin;

-- ────────────────────────────────────────────────────────────────────────
-- 1. build_beach_evidence(p_fid bigint) — the canonical evidence-rebuilder.
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.build_beach_evidence(p_fid bigint)
returns void
language plpgsql
security definer
set search_path to 'public', 'pg_catalog'
as $function$
begin
  if p_fid is null then return; end if;

  -- Clear regenerable field groups. Accessibility BEP rows
  -- (incl. manual_curator overrides) survive.
  delete from public.beach_enrichment_provenance
   where gold_fid = p_fid
     and field_group in ('dogs','practical','governance','access','polygon_containment');

  -- Population chain (each populator is idempotent; ON CONFLICT DO UPDATE)
  perform public.populate_polygon_containment_gold(p_fid);
  perform public.populate_from_cpad_gold(p_fid);
  perform public.populate_from_pad_us_gold(p_fid);           -- access-only
  perform public.populate_from_park_operators_gold(p_fid);
  perform public.populate_from_operators_gold(p_fid);
  perform public.populate_from_state_default_gold(p_fid);
  perform public.populate_from_research_gold(p_fid);
  perform public.populate_from_park_url_gold(p_fid);
  perform public.populate_from_park_url_governance_gold(p_fid);
  perform public.populate_from_unified_v1_gold(p_fid);
  perform public.populate_from_city_dog_policy_gold(p_fid);
  perform public.populate_from_county_dog_policy_gold(p_fid);
  perform public._emit_evidence_from_osm_amenities(p_fid);
  perform public.populate_accessibility_from_osm_amenities(p_fid);

  -- Resolution + governance derivation
  perform public._resolve_polygon_containment(p_fid);
  perform public.populate_governance_gold(p_fid);
  perform public._resolve_governance_gold(p_fid);
  perform public._resolve_dogs_gold(p_fid);
  perform public._resolve_practical_gold(p_fid);
  perform public._resolve_field_group_gold('access', p_fid);
  perform public._resolve_field_group_gold('accessibility', p_fid);

  -- Consensus + consumer-table promotion
  perform public.compute_beach_field_consensus(p_fid);
  perform public.promote_canonical_to_consumer_tables(p_fid);
  perform public.promote_canonical_accessibility_to_beach_amenities(p_fid);
end $function$;

grant execute on function public.build_beach_evidence(bigint)
  to anon, authenticated, service_role;

-- ────────────────────────────────────────────────────────────────────────
-- 2. promote_to_gold(p_fids[], p_score, p_publish) — now delegates to
--    build_beach_evidence for the per-fid loop. Keeps INSERT + slug/noaa.
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

  -- ── Step 1: INSERT new beaches_gold rows (the "build" of the row itself)
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

  -- ── Step 2: build the beach evidence chain for every fid (delegates)
  foreach fid_iter in array p_fids loop
    perform public.build_beach_evidence(fid_iter);
  end loop;

  select count(*) into contain_ev from public.beach_enrichment_provenance
   where field_group = 'polygon_containment' and gold_fid = any(p_fids);
  select count(*) into source_ev from public.beach_enrichment_provenance
   where field_group in ('dogs','practical','access','governance')
     and gold_fid = any(p_fids);

  select count(*) into bdp_changed from public.beach_dog_policy
   where arena_group_id = any(p_fids) and source = 'auto_promoted_from_consensus';
  select count(*) into ba_changed from public.beach_amenities
   where arena_group_id = any(p_fids) and source = 'auto_promoted_from_consensus';

  return query select promoted::bigint, existing::bigint, contain_ev::bigint, source_ev::bigint,
                      bdp_changed::bigint, ba_changed::bigint, noaa_count::bigint, slug_count::bigint;
end;
$function$;

-- ────────────────────────────────────────────────────────────────────────
-- 3. refire_bep_cascade — now a thin deprecated wrapper around
--    build_beach_evidence. Preserves backward compat for the geom-change
--    trigger and legacy callers. Mark in comment for future removal.
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

  -- DEPRECATED 2026-05-13: this function is preserved as a wrapper for
  -- backward compat. New callers should use build_beach_evidence(fid)
  -- directly (per-fid, idempotent, simpler).

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
    perform public.build_beach_evidence(fid_iter);
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
