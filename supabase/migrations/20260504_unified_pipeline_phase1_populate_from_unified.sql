-- 20260504_unified_pipeline_phase1_populate_from_unified.sql
--
-- Wire the unified_v1 extraction output into the resolver/auto-promoter chain.
-- After extract_for_beach.py runs, beach_policy_extractions has rows with
-- variant_key='unified_v1' but no populator emits evidence — so the resolver
-- never sees them.
--
-- This populator reads unified_v1 rows, aggregates per (beach, field_group)
-- across all URLs, and emits to beach_enrichment_provenance with
-- source='unified_v1'.
--
-- Per-beach merge logic: take the FIRST non-unclear value per (beach, field).
-- Multiple URLs may produce the same field with same/different values; we
-- prefer conclusive (yes/no/true/false) over unclear/unknown/null.
--
-- Field → field_group mapping:
--   dogs:     dogs_allowed, leash_policy, dogs_off_leash_area,
--             dogs_time_restrictions, dogs_seasonal_restrictions,
--             dogs_allowed_areas
--   practical: hours_text, has_lifeguards, has_restrooms, has_parking,
--              has_showers, has_disabled_access

begin;

-- 1. Extend source CHECK to include 'unified_v1'
alter table public.beach_enrichment_provenance
  drop constraint if exists beach_enrichment_provenance_source_check;
alter table public.beach_enrichment_provenance
  add constraint beach_enrichment_provenance_source_check
  check (source = any (array[
    'manual', 'plz', 'cpad', 'tiger_places', 'ccc', 'llm', 'web_scrape',
    'research', 'csp_parks', 'park_operators', 'nps_places', 'tribal_lands',
    'military_bases', 'pad_us', 'sma_code_mappings', 'jurisdictions',
    'csp_places', 'name', 'governing_body', 'park_url',
    'park_url_buffer_attribution', 'old_school_llm', 'counties',
    'json_explode', 'unified_v1'
  ]));

-- 2. Slot unified_v1 in field-group priority. Same level as old_school_llm
--    pending its own calibration (the 2026-05-04 probe didn't produce
--    side-by-side comparison data due to a bug — see follow-up).
create or replace function public._gold_field_group_source_priority(
  p_field_group text,
  p_source text
) returns integer language sql immutable as $$
  select case
    when p_source = 'manual' then 1

    when p_field_group = 'dogs' then case p_source
      when 'llm'              then 2
      when 'research'         then 3
      when 'park_url'         then 4
      when 'unified_v1'       then 5  -- new; calibrate later
      when 'json_explode'     then 5
      when 'old_school_llm'   then 5
      when 'park_operators'   then 6
      when 'governing_body'   then 7
      when 'cpad'             then 8
      when 'jurisdictions'    then 9
      when 'counties'         then 10
      when 'nps_places'       then 11
      when 'military_bases'   then 12
      when 'tribal_lands'     then 12
      else                          99
    end

    when p_field_group = 'practical' then case p_source
      when 'llm'              then 2
      when 'park_url'         then 3
      when 'park_operators'   then 4
      when 'unified_v1'       then 5  -- new; calibrate later
      when 'old_school_llm'   then 5
      when 'json_explode'     then 5
      when 'ccc'              then 6
      when 'research'         then 7
      else                          99
    end

    when p_field_group = 'governance' then case p_source
      when 'park_url'                    then 2
      when 'park_operators'              then 2
      when 'park_url_buffer_attribution' then 3
      when 'csp_parks'                   then 4
      when 'nps_places'                  then 4
      when 'cpad'                        then 5
      when 'tiger_places'                then 6
      when 'governing_body'              then 7
      when 'name'                        then 8
      when 'tribal_lands'                then 9
      when 'military_bases'              then 9
      else                                    99
    end

    when p_field_group = 'access' then case p_source
      when 'plz'           then 2
      when 'cpad'          then 3
      when 'json_explode'  then 4
      when 'csp_parks'     then 5
      when 'military_bases' then 6
      else                       99
    end

    when p_field_group = 'polygon_containment' then case p_source
      when 'cpad'           then 2
      when 'jurisdictions'  then 3
      when 'counties'       then 4
      when 'military_bases' then 5
      when 'tribal_lands'   then 5
      else                       99
    end

    else 99
  end;
$$;

-- 3. Field → field_group mapper
create or replace function public._unified_field_group(p_field text)
returns text language sql immutable as $$
  select case p_field
    when 'dogs_allowed'                then 'dogs'
    when 'leash_policy'                then 'dogs'
    when 'dogs_off_leash_area'         then 'dogs'
    when 'dogs_time_restrictions'      then 'dogs'
    when 'dogs_seasonal_restrictions'  then 'dogs'
    when 'dogs_allowed_areas'          then 'dogs'
    when 'hours_text'                  then 'practical'
    when 'has_lifeguards'              then 'practical'
    when 'has_restrooms'               then 'practical'
    when 'has_parking'                 then 'practical'
    when 'has_showers'                 then 'practical'
    when 'has_disabled_access'         then 'practical'
    else null
  end;
$$;

-- 4. The populator
create or replace function public.populate_from_unified_v1_gold(
  p_fid bigint default null
)
returns table(rows_inserted bigint, rows_updated bigint, beaches_touched bigint) as $$
declare
  ins_count bigint := 0;
  upd_count bigint := 0;
  beaches_count bigint := 0;
begin
  -- Aggregate unified_v1 extractions per (beach, field_group):
  --   For each (beach, field), pick the FIRST conclusive parsed_value
  --   (skip unclear/unknown/null) plus one evidence_quote.
  --   Then merge all fields in a group into one claimed_values jsonb.
  with extractions as (
    select e.arena_group_id as gold_fid,
           e.field_name,
           e.parsed_value,
           e.evidence_quote,
           e.raw_response,
           public._unified_field_group(e.field_name) as field_group,
           row_number() over (
             partition by e.arena_group_id, e.field_name
             order by case
               when e.parsed_value is null then 99
               when lower(e.parsed_value) in ('unclear','unknown','null','none','') then 50
               else 1
             end,
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
    select gold_fid, field_name, field_group, parsed_value, evidence_quote, raw_response
      from extractions
     where rnk = 1
       and parsed_value is not null
       and lower(parsed_value) not in ('unclear','unknown','null','none','')
  ),
  -- Build claimed_values per (beach, field_group). For json_structured fields,
  -- parse raw_response and merge keys (similar to json_explode pattern).
  -- For enum fields, just use parsed_value.
  per_field_payload as (
    select gold_fid, field_group, field_name,
           case
             -- For structured fields, parse the JSON and merge with prefixed keys
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
             -- For enum/bool fields, just emit the field_name → parsed_value
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
           count(*) as field_count
      from per_field_payload,
           lateral jsonb_each(payload) as kv(k, v)
     where v is not null and v <> 'null'::jsonb
     group by gold_fid, field_group
  ),
  upsert as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, claimed_values, confidence, notes, updated_at)
    select gold_fid, field_group, 'unified_v1', claimed_values, 0.7,
           format('Aggregated from %s unified_v1 extractions', field_count),
           now()
      from merged
     where claimed_values <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values,
          confidence = excluded.confidence,
          notes = excluded.notes,
          updated_at = now()
      where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
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
  'Phase 1 unified pipeline (2026-05-04). Aggregates unified_v1 extractions '
  '(from extract_for_beach.py) per (beach, field_group) and emits to '
  'beach_enrichment_provenance with source=unified_v1. Picks the first '
  'conclusive parsed_value per (beach, field) across all URLs. Idempotent. '
  'Slot in priority: same as old_school_llm (5) for dogs and practical, '
  'pending calibration.';

commit;
