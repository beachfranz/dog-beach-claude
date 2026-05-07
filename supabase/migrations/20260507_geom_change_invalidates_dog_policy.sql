-- 20260507_geom_change_invalidates_dog_policy.sql
--
-- Two-part fix for the "garbage dogs column on freshly-seeded beaches" bug
-- discovered 2026-05-07 during the SoCal core-beaches audit.
--
-- Symptom: after seeding 4 new SoCal beaches and discovering the geom
-- placement was wrong (coords landed on tideland polygons instead of
-- park polygons), I updated geoms on beaches_gold and re-ran
-- `promote_to_gold(...)` — but the BEP rows from spatial joins
-- (cpad_unit_dogs_policy_v1 et al.) didn't refresh. Direct invocation
-- of `_emit_evidence_from_cpad_unit_dogs_policy(fid)` fixed it.
--
-- Root cause #1: `promote_to_gold` only runs the FULL populator suite
-- (containment + cpad + park_operators + research + park_url +
-- park_url_governance + unified_v1 + city + county) for NEWLY-PROMOTED
-- fids. For existing fids it only re-runs unified_v1 + city + county —
-- skipping every spatial populator. So geom edits on existing fids
-- never propagate to BEP.
--
-- Root cause #2: anyone editing `beaches_gold.geom` directly (dedup
-- merge, manual fix, polygon-buffer adjustment) and forgetting to
-- re-run promote_to_gold leaves stale beach_dog_policy data.
--
-- Fix:
--   Part 1: promote_to_gold runs the full per-fid populator+resolver
--           suite for ALL fids (new + existing). Resolvers now scoped
--           per-fid for efficiency.
--   Part 2: AFTER UPDATE OF geom trigger on beaches_gold wipes
--           spatially-derived BEP rows and re-runs the chain. Gated
--           on `OLD.geom IS DISTINCT FROM NEW.geom` so identity
--           updates don't fire it.

begin;

-- ======================================================================
-- Part 1: promote_to_gold refreshes all spatial populators for ALL fids
-- ======================================================================

create or replace function public.promote_to_gold(p_fids bigint[], p_score boolean default false)
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

  -- ALL fids (new + existing) get the full populator + resolver suite.
  -- Each populator is idempotent via ON CONFLICT. This guarantees that
  -- geom edits on existing fids propagate to BEP and downstream.
  foreach fid_iter in array p_fids loop
    perform public.populate_polygon_containment_gold(fid_iter);
    perform public.populate_from_cpad_gold(fid_iter);
    perform public.populate_from_park_operators_gold(fid_iter);
    perform public.populate_from_research_gold(fid_iter);
    perform public.populate_from_park_url_gold(fid_iter);
    perform public.populate_from_park_url_governance_gold(fid_iter);
    perform public.populate_from_unified_v1_gold(fid_iter);
    perform public.populate_from_city_dog_policy_gold(fid_iter);
    perform public.populate_from_county_dog_policy_gold(fid_iter);

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

comment on function public.promote_to_gold(bigint[], boolean) is
  'Canonical orchestrator: arena → beaches_gold → spatial populators → resolvers → consensus → consumer tables. As of 2026-05-07 runs the full populator suite for new AND existing fids (was only running for new fids before, which left existing-fid geom edits stranded).';

-- ======================================================================
-- Part 2: AFTER UPDATE OF geom trigger on beaches_gold
-- ======================================================================

create or replace function public.tg_invalidate_dog_policy_on_geom_change()
returns trigger
language plpgsql
as $$
begin
  -- Wipe BEP rows whose evidence was derived from spatial joins. Source
  -- populators will re-emit fresh rows below. We don't touch BEP rows
  -- from non-spatial sources (extraction, research, park_url) — those
  -- are name/url-keyed and don't depend on geom.
  delete from public.beach_enrichment_provenance
   where gold_fid = NEW.fid
     and field_group in ('dogs', 'practical', 'governance', 'access', 'polygon_containment');

  -- Re-run the per-fid pipeline. Same set as promote_to_gold.
  perform public.populate_polygon_containment_gold(NEW.fid);
  perform public.populate_from_cpad_gold(NEW.fid);
  perform public.populate_from_park_operators_gold(NEW.fid);
  perform public.populate_from_research_gold(NEW.fid);
  perform public.populate_from_park_url_gold(NEW.fid);
  perform public.populate_from_park_url_governance_gold(NEW.fid);
  perform public.populate_from_unified_v1_gold(NEW.fid);
  perform public.populate_from_city_dog_policy_gold(NEW.fid);
  perform public.populate_from_county_dog_policy_gold(NEW.fid);

  perform public._resolve_polygon_containment(NEW.fid);
  perform public._resolve_governance_gold(NEW.fid);
  perform public._resolve_dogs_gold(NEW.fid);
  perform public._resolve_practical_gold(NEW.fid);
  perform public._resolve_field_group_gold('access', NEW.fid);
  perform public.compute_beach_field_consensus(NEW.fid);
  perform public.promote_canonical_to_consumer_tables(NEW.fid);

  return NEW;
end;
$$;

comment on function public.tg_invalidate_dog_policy_on_geom_change() is
  'Trigger handler: any UPDATE to beaches_gold.geom invalidates spatially-derived BEP rows for the fid and re-runs the populator + resolver + consensus + promote chain. Bulk geom updates should DISABLE this trigger and run promote_to_gold(array_agg(fid)) at the end.';

drop trigger if exists tg_beaches_gold_geom_change on public.beaches_gold;
create trigger tg_beaches_gold_geom_change
  after update of geom on public.beaches_gold
  for each row
  when (OLD.geom is distinct from NEW.geom)
  execute function public.tg_invalidate_dog_policy_on_geom_change();

commit;
