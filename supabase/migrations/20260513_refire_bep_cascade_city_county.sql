-- 20260513_refire_bep_cascade_city_county.sql
--
-- refire_bep_cascade deletes all governance BEP rows (including
-- source='city_jurisdiction' and 'county_jurisdiction') and then
-- re-runs the populator chain. But the chain was missing
-- populate_governance_from_city_county_gold — the new populator added
-- earlier today to attribute city/county operators via FK joins.
--
-- Symptom observed for OR: 60 attributed operators → 20 after first
-- bep_refire. 25 cities + 15 counties dropped because their BEP rows
-- got deleted and never re-emitted.
--
-- Fix: add populate_governance_from_city_county_gold to the cascade,
-- in the same position it occupies in promote_to_gold (right after
-- populate_from_pad_us_gold).

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
    delete from public.beach_enrichment_provenance
     where gold_fid = fid_iter
       and field_group in ('dogs','practical','governance','access','polygon_containment');

    perform public.populate_polygon_containment_gold(fid_iter);
    perform public.populate_from_cpad_gold(fid_iter);
    perform public.populate_from_pad_us_gold(fid_iter);
    -- ★ 2026-05-13: city/county FK-based governance attribution.
    -- Without this, refire_bep_cascade wipes the rows that
    -- populate_governance_from_city_county_gold writes in promote_to_gold
    -- and never re-emits them — city/county operators disappear from
    -- the candidate set.
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
    perform public.populate_accessibility_from_osm_amenities(fid_iter);

    perform public._resolve_polygon_containment(fid_iter);
    perform public.populate_governance_from_polygon_gold(fid_iter);

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
