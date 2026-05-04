-- 20260503_harmony_phase6_3c1_research_gold.sql
--
-- Phase 6.3c (part 1) of the harmony migration. Un-defers
-- populate_from_research_gold — the simpler half of the deferred
-- 6.3c pair. The other half (populate_from_park_url_gold) needs
-- gold-spine equivalents of us_beach_points + beach_cpad_candidates
-- before the buffer-rescued attribution path can be ported; that
-- stays deferred.
--
-- Reads policy_research_extractions (which already has arena_group_id
-- = gold_fid — no namespace gymnastics). Emits dogs + practical
-- evidence keyed on gold_fid. Source name maps from origin:
--   v2_dog_policy_old → old_school_llm
--   v2_dog_policy_v2  → research
--   manual            → manual
--   (else)            → research
--
-- Gold-side unique constraint on (gold_fid, field_group, source) doesn't
-- allow multiple rows per source with different URLs, so we pick the
-- highest-confidence row per (beach, source) via row_number window.
-- Legacy side keeps multi-URL evidence; we accept narrower coverage on
-- gold.

begin;

create or replace function public.populate_from_research_gold(p_fid bigint default null)
returns int
language plpgsql
as $$
declare rows_touched int := 0;
begin
  with successful as (
    select * from public.policy_research_extractions
    where extraction_status in ('success', 'imported_legacy')
      and arena_group_id is not null
      and (p_fid is null or arena_group_id = p_fid)
  ),
  tagged as (
    select *,
      case origin
        when 'v2_dog_policy_old' then 'old_school_llm'
        when 'v2_dog_policy_v2'  then 'research'
        when 'manual'            then 'manual'
        else                          'research'
      end as evidence_source
    from successful
  ),
  -- Pick best row per (beach, source) since gold-side unique
  -- constraint can't include source_url. Highest confidence wins.
  ranked as (
    select *, row_number() over (
      partition by arena_group_id, evidence_source
      order by extraction_confidence desc nulls last
    ) as rnk
    from tagged
  ),
  best as (select * from ranked where rnk = 1),
  dogs_built as (
    select arena_group_id, primary_source_url, evidence_source, extraction_confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',          dogs_allowed,
        'leash_required',   dogs_leash_required,
        'restricted_hours', dogs_restricted_hours,
        'seasonal_rules',   dogs_seasonal_rules,
        'zone_description', dogs_zone_description,
        'notes',            dogs_policy_notes
      )) as v
    from best
  ),
  ins_dogs as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, source_url, updated_at)
    select arena_group_id, 'dogs', evidence_source,
      coalesce(extraction_confidence, 0.65),
      v, primary_source_url, now()
    from dogs_built
    where v <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          source_url     = excluded.source_url,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  ),
  practical_built as (
    select arena_group_id, primary_source_url, evidence_source, extraction_confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'hours_text',         hours_text,
        'open_time',          open_time::text,
        'close_time',         close_time::text,
        'has_parking',        has_parking,
        'parking_type',       parking_type,
        'parking_notes',      parking_notes,
        'has_restrooms',      has_restrooms,
        'has_showers',        has_showers,
        'has_drinking_water', has_drinking_water,
        'has_lifeguards',     has_lifeguards,
        'has_disabled_access',has_disabled_access,
        'has_food',           has_food,
        'has_fire_pits',      has_fire_pits,
        'has_picnic_area',    has_picnic_area
      )) as v
    from best
  ),
  ins_practical as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, source_url, updated_at)
    select arena_group_id, 'practical', evidence_source,
      coalesce(extraction_confidence, 0.65),
      v, primary_source_url, now()
    from practical_built
    where v <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          source_url     = excluded.source_url,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from ins_dogs union all select * from ins_practical
  ) _;

  return rows_touched;
end;
$$;

comment on function public.populate_from_research_gold(bigint) is
  'Phase 6.3c (part 1) of harmony migration. Un-deferred 2026-05-03. Reads policy_research_extractions (arena_group_id = gold_fid), emits dogs + practical evidence keyed on gold_fid. Source name mapped from origin field. Gold-side narrows multi-URL evidence to one row per (beach, source) via highest-confidence pick. populate_from_park_url_gold (part 2) remains deferred — needs gold equivalents of us_beach_points + beach_cpad_candidates for buffer-rescued attribution.';

commit;
