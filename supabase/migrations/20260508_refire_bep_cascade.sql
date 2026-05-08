-- 20260508_refire_bep_cascade.sql
--
-- Step 5 of the bulletproofing chain: re-fire the BEP populate cascade
-- for fids whose beaches_gold.geom was moved without triggering
-- tg_beaches_gold_geom_change (which is disabled per Pin #21).
--
-- Affected populations:
--   - 40 rows from the 2026-05-08 OSM-polygon-prefer backfill
--   - ~40 dedup keepers from tonight (their geom got swapped to
--     the OSM polygon centroid as part of the late-stage dedup)
--   - Future: any fid changed by run_late_stage_dedup() or curator drag
--     while the geom-trigger remains disabled
--
-- This function takes a fid array, deletes the spatial-derived BEP rows
-- for each, re-runs all populators + resolvers + consensus, and promotes
-- canonical values back to consumer tables. Same cascade body as the
-- disabled tg_beaches_gold_geom_change trigger.
--
-- It's intentionally slow per fid (~5-10 sec). Run in batches; not safe
-- to call on the entire active catalog at once via HTTP (Supabase 524
-- timeout is the issue Pin #21 documents).

begin;

create or replace function public.refire_bep_cascade(p_fids bigint[])
returns table(fids_processed bigint, bep_rows_deleted bigint, bep_rows_after bigint)
language plpgsql
security definer
as $function$
declare
  fid_iter bigint;
  v_processed bigint := 0;
  v_deleted bigint := 0;
  v_after bigint := 0;
  v_pre_count bigint;
  v_post_count bigint;
begin
  if p_fids is null or array_length(p_fids, 1) is null then
    return query select 0::bigint, 0::bigint, 0::bigint;
    return;
  end if;

  -- Count BEP rows before for the spatial field groups
  select count(*) into v_pre_count
    from public.beach_enrichment_provenance
   where gold_fid = any(p_fids)
     and field_group in ('dogs', 'practical', 'governance', 'access', 'polygon_containment');

  foreach fid_iter in array p_fids loop
    -- Delete spatial-derived BEP rows; populate_*_gold will recreate them.
    delete from public.beach_enrichment_provenance
     where gold_fid = fid_iter
       and field_group in ('dogs', 'practical', 'governance', 'access', 'polygon_containment');

    -- Re-run the full populator suite. Mirrors what
    -- tg_invalidate_dog_policy_on_geom_change would do.
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
    perform public.promote_canonical_to_consumer_tables(fid_iter);

    v_processed := v_processed + 1;
  end loop;

  select count(*) into v_post_count
    from public.beach_enrichment_provenance
   where gold_fid = any(p_fids)
     and field_group in ('dogs', 'practical', 'governance', 'access', 'polygon_containment');

  v_deleted := v_pre_count;
  v_after := v_post_count;

  return query select v_processed, v_deleted, v_after;
end;
$function$;

grant execute on function public.refire_bep_cascade(bigint[]) to anon, authenticated, service_role;

commit;
