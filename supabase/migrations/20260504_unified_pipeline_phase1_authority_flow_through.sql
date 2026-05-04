-- 20260504_unified_pipeline_phase1_authority_flow_through.sql
--
-- Confidence flow-through (URL discovery remediation #1, per
-- project_url_discovery_scope.md "Known gaps in MVP"). Today
-- discovered_urls.authority_score (0-4) doesn't propagate to
-- beach_policy_extractions or beach_enrichment_provenance, so when
-- multiple URLs of the same source family disagree, the populator
-- picks "first conclusive" by id order — a coin flip when sources
-- disagree.
--
-- This migration adds source_authority_score to beach_policy_extractions
-- so extract_for_beach can record per-extraction authority. The
-- populator will be updated separately to prefer high-authority
-- extractions and set beach_enrichment_provenance.confidence accordingly.

begin;

alter table public.beach_policy_extractions
  add column if not exists source_authority_score integer;

comment on column public.beach_policy_extractions.source_authority_score is
  'Authority score (0-4) of the URL this extraction came from, mirrored '
  'from discovered_urls.authority_score / extract_research_v2 AUTH_DOMAINS '
  'priors. Used by populate_from_unified_v1_gold to prefer high-authority '
  'sources when multiple URLs disagree on the same field. Added 2026-05-04 '
  'as URL discovery remediation #1.';

create index if not exists beach_policy_extractions_authority_idx
  on public.beach_policy_extractions(source_authority_score)
  where source_authority_score is not null;

-- Update populate_from_unified_v1_gold to prefer high-authority extractions
-- when picking the best parsed_value per (beach, field). Also set
-- beach_enrichment_provenance.confidence based on the chosen extraction's
-- authority — high authority → high confidence → wins resolver tiebreak.

create or replace function public.populate_from_unified_v1_gold(
  p_fid bigint default null
)
returns table(rows_inserted bigint, rows_updated bigint, beaches_touched bigint) as $$
declare
  ins_count bigint := 0;
  upd_count bigint := 0;
  beaches_count bigint := 0;
begin
  with extractions as (
    select e.arena_group_id as gold_fid,
           e.field_name,
           e.parsed_value,
           e.evidence_quote,
           e.raw_response,
           coalesce(e.source_authority_score, 1) as auth,
           public._unified_field_group(e.field_name) as field_group,
           row_number() over (
             partition by e.arena_group_id, e.field_name
             order by
               -- 1) prefer conclusive over unclear/null
               case
                 when e.parsed_value is null then 99
                 when lower(e.parsed_value) in ('unclear','unknown','null','none','') then 50
                 else 1
               end,
               -- 2) prefer higher authority
               coalesce(e.source_authority_score, 1) desc,
               -- 3) tiebreak: most recent insertion
               e.id desc
           ) as rnk
      from public.beach_policy_extractions e
      join public.beaches_gold g on g.fid = e.arena_group_id
     where e.variant_key = 'unified_v1'
       and e.arena_group_id is not null
       and (p_fid is null or e.arena_group_id = p_fid)
       and public._unified_field_group(e.field_name) is not null
  ),
  best_per_field as (
    select gold_fid, field_name, field_group, parsed_value, evidence_quote, raw_response, auth
      from extractions
     where rnk = 1
       and parsed_value is not null
       and lower(parsed_value) not in ('unclear','unknown','null','none','')
  ),
  per_field_payload as (
    select gold_fid, field_group, field_name, auth,
           case
             when field_name = 'dogs_off_leash_area' then
               jsonb_build_object(
                 'off_leash_exists',     public._safe_jsonb(raw_response)->'off_leash_area_exists',
                 'off_leash_area_name',  public._safe_jsonb(raw_response)->'area_name',
                 'off_leash_hours',      public._safe_jsonb(raw_response)->'hours',
                 'off_leash_notes',      public._safe_jsonb(raw_response)->'notes')
             when field_name = 'dogs_time_restrictions' then
               jsonb_build_object(
                 'time_has_restriction',  public._safe_jsonb(raw_response)->'has_time_restriction',
                 'time_allowed_hours',    public._safe_jsonb(raw_response)->'allowed_hours',
                 'time_prohibited_hours', public._safe_jsonb(raw_response)->'prohibited_hours',
                 'time_evidence',         public._safe_jsonb(raw_response)->'evidence')
             when field_name = 'dogs_seasonal_restrictions' then
               jsonb_build_object(
                 'seasonal_has',         public._safe_jsonb(raw_response)->'has_seasonal_restriction',
                 'seasonal_description', public._safe_jsonb(raw_response)->'description',
                 'seasonal_period',      public._safe_jsonb(raw_response)->'affected_period')
             when field_name = 'dogs_allowed_areas' then
               jsonb_build_object(
                 'areas_coverage',   public._safe_jsonb(raw_response)->'coverage',
                 'areas_zones',      public._safe_jsonb(raw_response)->'zones',
                 'areas_boundaries', public._safe_jsonb(raw_response)->'boundaries',
                 'areas_evidence',   public._safe_jsonb(raw_response)->'evidence')
             when field_name = 'hours_text' then
               jsonb_build_object(
                 'hours_open',        public._safe_jsonb(raw_response)->'open',
                 'hours_close',       public._safe_jsonb(raw_response)->'close',
                 'hours_notes',       public._safe_jsonb(raw_response)->'notes',
                 'hours_is_24_hours', public._safe_jsonb(raw_response)->'is_24_hours')
             when field_name = 'dogs_allowed' then
               jsonb_build_object('allowed', parsed_value, 'allowed_evidence', evidence_quote)
             when field_name = 'leash_policy' then
               jsonb_build_object('leash_required', parsed_value, 'leash_evidence', evidence_quote)
             when field_name = 'has_lifeguards' then
               jsonb_build_object('has_lifeguards', parsed_value::boolean)
             when field_name = 'has_restrooms' then
               jsonb_build_object('has_restrooms', parsed_value::boolean)
             when field_name = 'has_parking' then
               jsonb_build_object('has_parking', parsed_value::boolean)
             when field_name = 'has_showers' then
               jsonb_build_object('has_showers', parsed_value::boolean)
             when field_name = 'has_disabled_access' then
               jsonb_build_object('has_disabled_access', parsed_value::boolean)
           end as payload
      from best_per_field
  ),
  merged as (
    select gold_fid, field_group,
           jsonb_strip_nulls(
             coalesce(jsonb_object_agg(k, v) filter (where k is not null), '{}'::jsonb)
           ) as claimed_values,
           -- Confidence = best authority observed for this field group, mapped:
           --   auth 4 (canonical .gov) → 0.90
           --   auth 3 (.gov / .ca.us)  → 0.80
           --   auth 2 (good aggregator)→ 0.65
           --   auth 1 (random)         → 0.55
           --   auth 0 (yelp/tripadvisor)→ 0.40
           greatest(0.40, least(0.90, 0.50 + (max(auth) * 0.10))) as confidence,
           count(*) as field_count,
           max(auth) as max_auth
      from per_field_payload,
           lateral jsonb_each(payload) as kv(k, v)
     where v is not null and v <> 'null'::jsonb
     group by gold_fid, field_group
  ),
  upsert as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, claimed_values, confidence, notes, updated_at)
    select gold_fid, field_group, 'unified_v1', claimed_values, confidence,
           format('Aggregated from %s unified_v1 extractions (max_authority=%s)',
                  field_count, max_auth),
           now()
      from merged
     where claimed_values <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values,
          confidence = excluded.confidence,
          notes = excluded.notes,
          updated_at = now()
      where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
         or beach_enrichment_provenance.confidence is distinct from excluded.confidence
    returning (xmax = 0) as inserted, gold_fid
  )
  select count(*) filter (where inserted),
         count(*) filter (where not inserted),
         count(distinct gold_fid)
    into ins_count, upd_count, beaches_count
    from upsert;

  return query select ins_count, upd_count, beaches_count;
end;
$$ language plpgsql;

comment on function public.populate_from_unified_v1_gold(bigint) is
  'Phase 1 unified pipeline. Aggregates unified_v1 extractions per (beach, '
  'field_group) into beach_enrichment_provenance. URL discovery remediation '
  '#1 (2026-05-04): now prefers high-authority sources when multiple URLs '
  'disagree on a field. Authority score → confidence (0.40-0.90 range), '
  'so resolver tiebreak naturally favors high-authority sources within '
  'the same source_priority tier. Manual still wins via priority=1.';

commit;
