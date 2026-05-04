-- 20260504_unified_pipeline_phase1_json_exploder.sql
--
-- Unified pipeline phase 1, move 2 (per docs/unified-pipeline.md): recover
-- structured payloads from beach_policy_extractions.raw_response that the
-- existing parser flattened to a single scalar.
--
-- Today, json_structured + json_evidence_v2 LLM responses return multi-key
-- JSON (e.g. {open, close, notes, is_24_hours} for hours_text) but the
-- ingest path captures only one scalar in `parsed_value`. ~228 net-new
-- structured values across 57 beaches sit unused in raw_response — closing
-- the time_windows (7.9% catalog coverage) + seasonal_closures (14.2%) gaps
-- for $0.
--
-- Strategy: parse raw_response, aggregate per (beach, field_group) into
-- claimed_values jsonb, INSERT into beach_enrichment_provenance with
-- source='json_explode'. Resolver picks canonical by source priority.
--
-- Live target variants/fields (from project_json_structured_dropped_payload.md):
--   variant_key       field_name                    field_group
--   ────────────────  ─────────────────────────     ───────────
--   json_structured   dogs_off_leash_area           dogs
--   json_structured   dogs_time_restrictions        dogs
--   json_structured   dogs_seasonal_restrictions    dogs
--   json_structured   dogs_allowed_areas            dogs
--   json_structured   hours_text                    practical
--   json_evidence_v2  dogs_leash_required           dogs
--   json_evidence_v2  public_access                 access
--   json_evidence     dogs_allowed                  dogs
--
-- DEPRECATED variants (skipped — re-extract via redesigned prompts when needed):
--   json_structured/access_rule, access_text, has_parking, parking_type,
--   has_drinking_water, raw_address, dogs_leash_required (this one was
--   superseded by json_evidence_v2)

begin;

-- 1. Extend source CHECK to include 'json_explode'
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
    'json_explode'
  ]));

-- 2. Extend _gold_source_priority to rank json_explode at 5
--    (same level as old_school_llm; tiebreak by id-asc means earlier
--    extractions win, which is fine — json_explode rows will be inserted
--    after old_school_llm's existing rows so old_school_llm naturally
--    keeps canonical when both exist)
create or replace function public._gold_source_priority(p_source text)
returns integer language sql immutable as $$
  select case p_source
    when 'manual'         then 1
    when 'llm'            then 2
    when 'park_url'       then 3
    when 'park_operators' then 3
    when 'research'       then 4
    when 'old_school_llm' then 5
    when 'json_explode'   then 5
    when 'cpad'           then 6
    when 'jurisdictions'  then 7
    when 'counties'       then 8
    when 'nps_places'     then 9
    when 'military_bases' then 10
    when 'tribal_lands'   then 10
    else                       99
  end
$$;

-- 3a. Safe JSON cast helper — postgres ::jsonb cast raises on malformed
--     input; we want NULL instead so a few non-JSON LLM outputs don't break
--     the bulk explode.
create or replace function public._safe_jsonb(p_text text)
returns jsonb language plpgsql immutable as $$
begin
  if p_text is null then return null; end if;
  begin
    return p_text::jsonb;
  exception when others then
    return null;
  end;
end;
$$;

-- 3b. The exploder function — idempotent UPSERT-by-(gold_fid, field_group, source)
create or replace function public.explode_json_structured_to_provenance()
returns table(rows_inserted bigint, rows_updated bigint, beaches_touched bigint)
language plpgsql as $$
declare
  ins_count bigint := 0;
  upd_count bigint := 0;
  beaches_count bigint := 0;
begin

  -- Build merged claimed_values per (beach, field_group) by aggregating
  -- multi-key JSON payloads from beach_policy_extractions.raw_response.
  -- Use prefixed keys so multiple variants don't clobber each other:
  --   off_leash_*  ← dogs_off_leash_area
  --   time_*       ← dogs_time_restrictions
  --   seasonal_*   ← dogs_seasonal_restrictions
  --   areas_*      ← dogs_allowed_areas
  --   leash_*      ← dogs_leash_required (json_evidence_v2)
  --   allowed      ← dogs_allowed (json_evidence)
  --   hours_*      ← hours_text
  --   access_*     ← public_access (json_evidence_v2)

  with parsed as (
    select
      e.arena_group_id as gold_fid,
      e.variant_key,
      e.field_name,
      -- Safe parse; NULL on malformed JSON
      public._safe_jsonb(e.raw_response) as resp,
      e.evidence_quote,
      e.run_id
    from public.beach_policy_extractions e
    where e.variant_key in ('json_structured', 'json_evidence_v2', 'json_evidence')
      and e.arena_group_id is not null
      and e.raw_response is not null
      and e.raw_response ~ '^\s*\{'
  ),
  -- Per-row prefixed claim payload by (variant_key, field_name)
  prefixed as (
    select
      gold_fid,
      case field_name
        when 'dogs_off_leash_area'        then 'dogs'
        when 'dogs_time_restrictions'     then 'dogs'
        when 'dogs_seasonal_restrictions' then 'dogs'
        when 'dogs_allowed_areas'         then 'dogs'
        when 'dogs_leash_required'        then 'dogs'
        when 'dogs_allowed'               then 'dogs'
        when 'hours_text'                 then 'practical'
        when 'public_access'              then 'access'
      end as field_group,
      -- prefix all JSON keys so dogs-grouped multi-source keys don't clash
      (case field_name
         when 'dogs_off_leash_area' then jsonb_build_object(
           'off_leash_exists',     resp->'off_leash_area_exists',
           'off_leash_area_name',  resp->'area_name',
           'off_leash_hours',      resp->'hours',
           'off_leash_notes',      resp->'notes')
         when 'dogs_time_restrictions' then jsonb_build_object(
           'time_has_restriction',  resp->'has_time_restriction',
           'time_allowed_hours',    resp->'allowed_hours',
           'time_prohibited_hours', resp->'prohibited_hours',
           'time_evidence',         resp->'evidence')
         when 'dogs_seasonal_restrictions' then jsonb_build_object(
           'seasonal_has',           resp->'has_seasonal_restriction',
           'seasonal_description',   resp->'description',
           'seasonal_period',        resp->'affected_period')
         when 'dogs_allowed_areas' then jsonb_build_object(
           'areas_coverage',   resp->'coverage',
           'areas_zones',      resp->'zones',
           'areas_boundaries', resp->'boundaries',
           'areas_evidence',   resp->'evidence')
         when 'dogs_leash_required' then jsonb_build_object(
           'leash_required',     resp->'leash',
           'leash_sand_rule',    resp->'sand_rule',
           'leash_trail_rule',   resp->'trail_rule',
           'leash_parking_rule', resp->'parking_rule',
           'leash_evidence',     resp->'evidence')
         when 'dogs_allowed' then jsonb_build_object(
           'allowed',          resp->'dogs_allowed',
           'allowed_evidence', resp->'evidence')
         when 'hours_text' then jsonb_build_object(
           'hours_open',        resp->'open',
           'hours_close',       resp->'close',
           'hours_notes',       resp->'notes',
           'hours_is_24_hours', resp->'is_24_hours')
         when 'public_access' then jsonb_build_object(
           'access_status',  resp->'access',
           'access_barrier', resp->'barrier',
           'access_evidence', resp->'evidence')
       end) as payload
    from parsed
    where resp is not null
  ),
  -- Aggregate per (beach, field_group) into one merged jsonb
  merged as (
    select
      gold_fid,
      field_group,
      -- jsonb_object_agg flattens; we want all per-row payloads merged.
      -- Using a CTE with || via aggregation:
      jsonb_strip_nulls(
        coalesce(jsonb_object_agg(k, v) filter (where k is not null), '{}'::jsonb)
      ) as claimed_values,
      count(*) as source_row_count
    from prefixed,
         lateral jsonb_each(payload) as kv(k, v)
    where field_group is not null
      and v is not null
      and v <> 'null'::jsonb
    group by gold_fid, field_group
  ),
  -- UPSERT into provenance
  upsert as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, claimed_values, confidence, notes, updated_at)
    select
      m.gold_fid,
      m.field_group,
      'json_explode',
      m.claimed_values,
      0.7,
      format('Exploded from %s json_structured/json_evidence raw_response rows',
             m.source_row_count),
      now()
    from merged m
    where m.claimed_values <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values,
          confidence = excluded.confidence,
          notes = excluded.notes,
          updated_at = now()
      where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
         or beach_enrichment_provenance.notes is distinct from excluded.notes
    returning (xmax = 0) as inserted, gold_fid
  )
  select
    count(*) filter (where inserted),
    count(*) filter (where not inserted),
    count(distinct gold_fid)
  into ins_count, upd_count, beaches_count
  from upsert;

  return query select ins_count, upd_count, beaches_count;
end;
$$;

comment on function public.explode_json_structured_to_provenance() is
  'Phase 1 unified pipeline (2026-05-04). Recovers structured JSON payloads '
  'from beach_policy_extractions.raw_response that the existing parser flattened '
  'to one scalar. Aggregates multi-key payloads per (beach, field_group) into '
  'beach_enrichment_provenance with source=json_explode (priority 5, parallel '
  'to old_school_llm). Idempotent. See docs/unified-pipeline.md and '
  'project_json_structured_dropped_payload.md.';

commit;
