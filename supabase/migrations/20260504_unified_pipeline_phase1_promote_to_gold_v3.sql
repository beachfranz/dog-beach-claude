-- 20260504_unified_pipeline_phase1_promote_to_gold_v3.sql
--
-- Captures the in-place fixes applied during the 30-beach soak test:
--   1. _make_location_slug now handles collisions (appends -<fid> on dup)
--   2. promote_to_gold scopes per-fid populators to NEWLY-PROMOTED fids
--      only (existing fids already have populator outputs); calls
--      populate_from_unified_v1_gold for ALL p_fids (for the
--      --with-extraction re-entry case); resolvers/consensus/promoter
--      run normally
--   3. Statement_timeout bumped to 600s in the calling script
--      (scripts/promote_to_gold.py)
--
-- Result of soak test on 30 fresh arena beaches: 30 promoted, 26
-- got beach_dog_policy from auto_promoted_from_consensus, 25 got
-- beach_amenities. The 4 beaches missing dog_policy are the URL-pool-
-- starved ones whose web search returned 0 results.

begin;

create or replace function public._make_location_slug(p_name text, p_county text, p_fid bigint)
returns text language plpgsql as $$
declare base text; s text;
begin
  if p_name is null then return null; end if;
  base := lower(coalesce(p_name, '') || '-' || coalesce(p_county, ''));
  base := regexp_replace(base, '[''‘’]', '', 'g');
  base := regexp_replace(base, '[^a-z0-9]+', '-', 'g');
  s := trim(both '-' from base);
  if s = '' then return 'unnamed-' || p_fid::text; end if;
  if exists (select 1 from public.beaches_gold
              where location_id = s and fid <> p_fid) then
    return s || '-' || p_fid::text;
  end if;
  return s;
end;
$$;

comment on function public._make_location_slug(text, text, bigint) is
  'Slugifies name + county for beaches_gold.location_id. Appends -<fid> '
  'suffix when another beach already has the slug. Examples: "Oceanside '
  'City Beach"+"San Diego" → "oceanside-city-beach-san-diego" (or '
  '"...-4096" if taken). Reads beaches_gold for collision check.';


create or replace function public.promote_to_gold(p_fids bigint[], p_score boolean default false)
returns table(rows_promoted bigint, rows_already_in_gold bigint, containment_evidence bigint,
              source_evidence bigint, beach_dog_policy_changed bigint, beach_amenities_changed bigint,
              noaa_assigned bigint, slugs_assigned bigint) as $$
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

  -- 1. INSERT to beaches_gold (Layer B); capture the NEWLY-PROMOTED fids
  with to_promote as (
    select a.* from public.arena a
     where a.fid = any(p_fids) and a.is_active = true
       and not exists (select 1 from public.beaches_gold g where g.fid = a.fid)
  ),
  ins as (
    insert into public.beaches_gold
      (fid, location_id, name, lat, lon, county_name, source_code, source_id, group_id,
       nav_lat, nav_lon, nav_source, name_source, park_name, state, promoted_from, promoted_at,
       is_active, noaa_station_id, timezone, open_time, close_time, is_scoreable, geom)
    select t.fid, public._make_location_slug(t.name, t.county_name, t.fid),
      t.name, t.lat, t.lon, t.county_name, t.source_code, t.source_id, coalesce(t.group_id, t.fid),
      t.nav_lat, t.nav_lon, t.nav_source, t.name_source, null,
      public._infer_state_from_county(t.county_name),
      'promote_to_gold_v3', now(), true,
      public._nearest_noaa_station(st_setsrid(st_makepoint(t.lon, t.lat), 4326)::geography),
      'America/Los_Angeles', '05:00', '22:00', coalesce(p_score, false),
      st_setsrid(st_makepoint(t.lon, t.lat), 4326)
    from to_promote t
    returning fid, location_id, noaa_station_id
  )
  select array_agg(fid), count(*), count(*) filter (where location_id is not null),
         count(*) filter (where noaa_station_id is not null)
    into new_fids, promoted, slug_count, noaa_count from ins;
  existing := array_length(p_fids, 1) - promoted;

  -- 2-3. Per-fid populators (Layer C + Layer D source). Only run on
  --      NEWLY-PROMOTED fids — existing fids already have populator outputs.
  --      The full-batch (NULL) path was dropped because the cpad spatial
  --      join across 805 active beaches × 17K cpad_units exceeded the
  --      connection statement_timeout.
  if new_fids is not null then
    foreach fid_iter in array new_fids loop
      perform public.populate_polygon_containment_gold(fid_iter);
      perform public.populate_from_cpad_gold(fid_iter);
      perform public.populate_from_park_operators_gold(fid_iter);
      perform public.populate_from_research_gold(fid_iter);
      perform public.populate_from_park_url_gold(fid_iter);
      perform public.populate_from_park_url_governance_gold(fid_iter);
      perform public.populate_from_unified_v1_gold(fid_iter);
    end loop;
  end if;

  -- 3b. populate_from_unified_v1_gold for existing-in-gold fids in p_fids
  --     too. This handles the --with-extraction re-entry case where new
  --     extract_for_beach extractions just landed and need to flow through.
  foreach fid_iter in array p_fids loop
    if (new_fids is null or fid_iter <> all(new_fids)) then
      perform public.populate_from_unified_v1_gold(fid_iter);
    end if;
  end loop;

  -- 4. Resolvers (idempotent; touch only existing rows)
  perform public._resolve_polygon_containment(null);
  perform public._resolve_governance_gold(null);
  perform public._resolve_dogs_gold(null);
  perform public._resolve_practical_gold(null);
  perform public._resolve_field_group_gold('access', null);

  -- 5. Layer 2 cross-source consensus (per-fid, scoped)
  foreach fid_iter in array p_fids loop
    perform public.compute_beach_field_consensus(fid_iter);
  end loop;

  -- 6. Tally evidence for the promoted set
  select count(*) into contain_ev from public.beach_enrichment_provenance
   where field_group = 'polygon_containment' and gold_fid = any(p_fids);
  select count(*) into source_ev from public.beach_enrichment_provenance
   where field_group in ('dogs','practical','access','governance')
     and gold_fid = any(p_fids);

  -- 7. Auto-promoter to consumer tables (per-fid)
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
$$ language plpgsql;

comment on function public.promote_to_gold(bigint[], boolean) is
  'Phase 1 unified pipeline v3 (2026-05-04). Soup-to-nuts arena → consumer '
  'surface in one call. Steps: INSERT to beaches_gold (auto-derive '
  'slug/noaa/timezone) → per-fid populate_polygon_containment_gold + 6 '
  'source populators (only on NEWLY-PROMOTED fids; populate_from_unified_v1_gold '
  'also runs on existing-in-gold p_fids for --with-extraction re-entry) → '
  '5 resolvers (batch) → compute_beach_field_consensus per fid (Layer 2) → '
  'promote_canonical_to_consumer_tables per fid. Idempotent. Statement_timeout '
  'bumped to 600s by calling script.';

commit;
