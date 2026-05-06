-- 20260507_emit_operator_policy_exceptions.sql
--
-- Fifth dog-policy emitter — bridges operator_policy_exceptions (per-named-
-- beach overrides under an operator) into the BEP-driven consensus pipeline.
--
-- Companion to _emit_evidence_from_operator_dogs_policy: that emitter handles
-- the operator-level default ("LA County: dogs prohibited on county beaches"),
-- this emitter handles the per-beach exceptions ("Carbon Beach in LA County:
-- dogs allowed on sand").
--
-- Match strategy: trigram similarity between beaches_gold.name and
-- operator_policy_exceptions.beach_name, scoped to beaches inside the
-- operator's polygon. Best match per beach wins. Confidence scales with
-- similarity:
--   sim > 0.7  → 0.80 (high-confidence per-beach override; outranks operator default at 0.70)
--   sim 0.5-0.7 → 0.70 (moderate; ties with operator default)
--   sim 0.4-0.5 → 0.60 (low; flagged for review)
--
-- Source name: operator_policy_exceptions_v1.
--
-- Rule vocabulary (210 rows in source table):
--   allowed:    69 → dogs_allowed='yes', leash_required='on_leash' (default)
--   prohibited: 90 → dogs_allowed='no'
--   no:          6 → dogs_allowed='no'
--   off_leash:  44 → dogs_allowed='yes', leash_required='off_leash', off_leash_exists='true'
--   restricted:  1 → dogs_allowed='mixed' (legacy, retiring)

begin;

alter table public.beach_enrichment_provenance
  drop constraint beach_enrichment_provenance_source_check;
alter table public.beach_enrichment_provenance
  add constraint beach_enrichment_provenance_source_check
  check (source = any (array[
    'manual','manual_curator','plz','cpad','tiger_places','ccc','llm',
    'web_scrape','research','csp_parks','park_operators','nps_places',
    'tribal_lands','military_bases','pad_us','sma_code_mappings',
    'jurisdictions','csp_places','name','governing_body','park_url',
    'park_url_buffer_attribution','park_url_governance','old_school_llm',
    'counties','json_explode','unified_v1','city_policy','county_policy',
    'text_repass_v1','cpad_unit_dogs_policy_v1','pad_us_dogs_policy_v1',
    'operator_dogs_policy_v1','zone_rules_derived_v1',
    'zone_rules_practical_v1','osm_amenities_v1',
    'operator_policy_exceptions_v1'
  ]));


create or replace function public._emit_evidence_from_operator_policy_exceptions(
  p_fid bigint default null
) returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with candidate_pairs as (
    -- All beach × exception combinations within the same operator polygon,
    -- where the beach name is at least somewhat similar to the exception
    -- beach_name. similarity > 0.4 prunes obvious mismatches.
    select bg.fid as gold_fid,
           ope.id as exception_id,
           ope.rule as raw_rule,
           ope.beach_name as exception_beach_name,
           ope.source_quote, ope.source_url,
           ope.operator_id,
           o.canonical_name as operator_name,
           similarity(lower(bg.name), lower(ope.beach_name)) as sim_score
      from public.beaches_gold bg
      join public.operators o on ST_Intersects(o.geom, bg.geom)
      join public.operator_policy_exceptions ope on ope.operator_id = o.id
     where bg.is_active
       and (p_fid is null or bg.fid = p_fid)
       and similarity(lower(bg.name), lower(ope.beach_name)) > 0.4
  ),
  best_match_per_beach as (
    select distinct on (gold_fid) *
      from candidate_pairs
     order by gold_fid, sim_score desc, exception_id
  ),
  payload as (
    select
      gold_fid, operator_name, source_url, exception_beach_name,
      sim_score, operator_id, raw_rule, source_quote,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed', case raw_rule
          when 'allowed'    then 'yes'
          when 'prohibited' then 'no'
          when 'no'         then 'no'
          when 'off_leash'  then 'yes'
          when 'restricted' then 'mixed'
          else null end,
        'leash_required', case raw_rule
          when 'off_leash' then 'off_leash'
          when 'allowed'   then 'on_leash'
          else null end,
        'off_leash_exists', case when raw_rule = 'off_leash' then 'true' else null end,
        'source_quote', source_quote,
        'matched_exception_name', exception_beach_name,
        'similarity_score', round(sim_score::numeric, 3)::text,
        'operator_id', operator_id::text
      )) as cv,
      case when sim_score > 0.7 then 0.80
           when sim_score > 0.5 then 0.70
           else 0.60 end as confidence
    from best_match_per_beach
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, cpad_unit_name, extraction_type, cpad_role, updated_at)
    select gold_fid, 'dogs', 'operator_policy_exceptions_v1', source_url,
           cv, confidence, false, operator_name, 'derived_url_crawl', 'beach_access', now()
      from payload where cv <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values,
          confidence     = excluded.confidence,
          source_url     = excluded.source_url,
          cpad_unit_name = excluded.cpad_unit_name,
          updated_at     = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
       or beach_enrichment_provenance.confidence is distinct from excluded.confidence
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upserted;
  return query select ins::bigint, upd::bigint, skp::bigint;
end $$;

grant execute on function public._emit_evidence_from_operator_policy_exceptions(bigint)
  to authenticated, service_role;

comment on function public._emit_evidence_from_operator_policy_exceptions(bigint) is
  'Emits BEP rows from operator_policy_exceptions. Per-named-beach
   overrides under an operator. Trigram similarity match scoped to the
   operator''s polygon.';

commit;
