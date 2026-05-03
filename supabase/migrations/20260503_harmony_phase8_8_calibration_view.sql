-- 20260503_harmony_phase8_8_calibration_view.sql
--
-- Phase 8 slice 8: extractor calibration loop.
--
-- Compares per-source extraction evidence to curator truth (from
-- beach_policy_gold_set v3). Surfaces "for beaches the curator has
-- truthed, how often does each extractor's dogs_allowed claim match?"
--
-- Today this is dogs_allowed only — highest-value field, simplest
-- mapping (truth column verified_value vs evidence claimed_values
-- ->>'allowed', both text). Other fields can extend the same pattern
-- (a UNION ALL or a parameterized function) when needed.
--
-- The view + aggregation query together fulfill the third un-defer
-- trigger from project_harmony_pipeline_migration.md ("Calibration
-- loops comparing source-by-source extraction accuracy against truth
-- set"). Concrete value: tells Franz which extractor to trust most
-- per field.
--
-- First-run findings (2026-05-03, dogs_allowed):
--   old_school_llm: 5/46 = 10.9%   (poor)
--   park_url:       15/33 = 45.5%
--   research (v2):  16/33 = 48.5%
-- Most disagreement is VOCABULARY mismatch (curator 'unclear' vs
-- extractor 'restricted' is the most common pattern). Real disagreements
-- (extractor 'no' vs curator 'yes') are ~5/85 ≈ 6%. A vocabulary-
-- normalized view (yes/restricted/seasonal collapsed to "allowed") would
-- give significantly higher agreement %.

begin;

create or replace view public.gold_dogs_allowed_calibration as
with truth as (
  select arena_group_id as gold_fid,
         lower(trim(verified_value)) as truth_value,
         curator_confidence
    from public.beach_policy_gold_set
   where field_name = 'dogs_allowed'
     and verified_value is not null
),
evidence as (
  select e.gold_fid,
         e.source,
         lower(trim(e.claimed_values->>'allowed')) as extracted_value,
         e.confidence,
         e.is_canonical
    from public.beach_enrichment_provenance e
   where e.field_group = 'dogs'
     and e.gold_fid is not null
     and e.claimed_values ? 'allowed'
)
select e.gold_fid,
       g.name as beach_name,
       e.source,
       t.truth_value,
       t.curator_confidence,
       e.extracted_value,
       e.confidence as extraction_confidence,
       e.is_canonical,
       (t.truth_value = e.extracted_value) as agreement
  from truth t
  join evidence e on e.gold_fid = t.gold_fid
  join public.beaches_gold g on g.fid = e.gold_fid;

comment on view public.gold_dogs_allowed_calibration is
  'Phase 8 slice 8 of harmony migration. Per-(beach, source) comparison of dogs_allowed extraction vs curator gold-set truth. Used to score extractor accuracy. Today: dogs_allowed only — extends to other fields by following the same shape (truth from beach_policy_gold_set, extraction from beach_enrichment_provenance jsonb paths).';

commit;
