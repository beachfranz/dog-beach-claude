-- 20260510_refire_bep_cascade_with_audit.sql
--
-- Today's geom-change sweep (Franz, 2026-05-10) re-fired 144 CA fids
-- through refire_bep_cascade. We couldn't answer "how many flipped" after
-- the fact: the function deletes the old BEP rows up front, beaches_gold
-- has no updated_at, track_commit_timestamp is off, and there are no
-- history tables. So the pre-sweep state was irrecoverable.
--
-- This migration makes the cascade self-auditing. Every call captures
-- before+after snapshots of the three polygon-attribution columns into
-- refire_audit, keyed by a per-run UUID. The next sweep can diff
-- straightforwardly. Snapshot cost is negligible (two single-table reads
-- per fid).

begin;

create table if not exists public.refire_audit (
  id              bigserial primary key,
  refire_run_id   uuid not null,
  fid             bigint not null,
  phase           text not null check (phase in ('before','after')),
  captured_at     timestamptz not null default now(),
  cpad_unit_id    integer,
  c1_jurisdiction_id bigint,
  county_geoid    text
);

create index if not exists refire_audit_run_idx
  on public.refire_audit (refire_run_id, fid, phase);
create index if not exists refire_audit_fid_idx
  on public.refire_audit (fid, captured_at desc);

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

  -- BEFORE snapshot (single batch INSERT for the whole p_fids array)
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

    perform public._resolve_polygon_containment(fid_iter);
    perform public._resolve_governance_gold(fid_iter);
    perform public._resolve_dogs_gold(fid_iter);
    perform public._resolve_practical_gold(fid_iter);
    perform public._resolve_field_group_gold('access', fid_iter);
    perform public.compute_beach_field_consensus(fid_iter);
    perform public.promote_canonical_to_consumer_tables(fid_iter);

    v_processed := v_processed + 1;
  end loop;

  -- AFTER snapshot
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

-- Convenience view for reading the most-recent diff.
create or replace view public.refire_audit_diff as
with paired as (
  select b.refire_run_id, b.fid, b.captured_at as ran_at,
         b.cpad_unit_id       as cpad_before, a.cpad_unit_id       as cpad_after,
         b.c1_jurisdiction_id as juris_before, a.c1_jurisdiction_id as juris_after,
         b.county_geoid       as county_before, a.county_geoid       as county_after
    from public.refire_audit b
    join public.refire_audit a using (refire_run_id, fid)
   where b.phase = 'before' and a.phase = 'after'
)
select *,
       (cpad_before is distinct from cpad_after)        as cpad_changed,
       (juris_before is distinct from juris_after)      as juris_changed,
       (county_before is distinct from county_after)    as county_changed,
       (cpad_before is distinct from cpad_after
        or juris_before is distinct from juris_after
        or county_before is distinct from county_after) as any_changed
  from paired;

commit;
