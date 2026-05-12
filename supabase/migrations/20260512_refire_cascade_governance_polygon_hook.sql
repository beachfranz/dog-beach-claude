-- 20260512_refire_cascade_governance_polygon_hook.sql
--
-- Adds populate_governance_from_polygon_gold() to refire_bep_cascade so
-- the polygon-derived governance source is regenerated on every refire.
-- Without this, the BEP-row delete at the top of the loop would wipe
-- the source and the resolver's state-denylist fallback would have
-- nothing to promote.
--
-- The order matters: must run AFTER _resolve_polygon_containment (which
-- writes the canonical polygon_containment rows that this populator
-- reads), but BEFORE _resolve_governance_gold (which reads the new
-- rows as a fallback when the cpad denylist demotes a row).

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

    -- Polygon containment must resolve first; its canonical rows are
    -- the input to populate_governance_from_polygon_gold.
    perform public._resolve_polygon_containment(fid_iter);
    perform public.populate_governance_from_polygon_gold(fid_iter);

    perform public._resolve_governance_gold(fid_iter);
    perform public._resolve_dogs_gold(fid_iter);
    perform public._resolve_practical_gold(fid_iter);
    perform public._resolve_field_group_gold('access', fid_iter);
    perform public.compute_beach_field_consensus(fid_iter);
    perform public.promote_canonical_to_consumer_tables(fid_iter);

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
