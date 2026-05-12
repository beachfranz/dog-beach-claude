-- 20260512_refire_cascade_accessibility_hook.sql
--
-- Hook accessibility populator + resolver + promoter into refire_bep_cascade.
-- Deliberately NOT adding 'accessibility' to the delete-list at the top of
-- the cascade — that would wipe manual_curator BEP rows which we want to
-- preserve as durable overrides. Populator upserts (ON CONFLICT update)
-- so re-runs are safe.

begin;

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
    -- Delete only the regenerable field groups. Accessibility BEP rows
    -- (incl. manual_curator overrides) survive the cascade.
    delete from public.beach_enrichment_provenance
     where gold_fid = fid_iter
       and field_group in ('dogs','practical','governance','access','polygon_containment');

    perform public.populate_polygon_containment_gold(fid_iter);
    perform public.populate_from_cpad_gold(fid_iter);
    perform public.populate_from_pad_us_gold(fid_iter);
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
    -- NEW: accessibility populator (upsert, no delete)
    perform public.populate_accessibility_from_osm_amenities(fid_iter);

    perform public._resolve_polygon_containment(fid_iter);
    perform public.populate_governance_from_polygon_gold(fid_iter);

    perform public._resolve_governance_gold(fid_iter);
    perform public._resolve_dogs_gold(fid_iter);
    perform public._resolve_practical_gold(fid_iter);
    perform public._resolve_field_group_gold('access', fid_iter);
    -- NEW: resolve accessibility canonical
    perform public._resolve_field_group_gold('accessibility', fid_iter);

    perform public.compute_beach_field_consensus(fid_iter);
    perform public.promote_canonical_to_consumer_tables(fid_iter);
    -- NEW: promote accessibility canonical → beach_amenities.accessibility_features
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
