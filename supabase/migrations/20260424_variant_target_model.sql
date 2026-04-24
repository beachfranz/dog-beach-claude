-- Add target_model to extraction_prompt_variants (2026-04-24)
-- Route enum/bool shapes to Haiku (cheap, sufficient for single-word answers)
-- and text/structured_json to Sonnet (needs reasoning for prose + JSON).
--
-- Decision locked in with Franz 2026-04-24:
--   - Prompt caching: yes (implemented in the scraper runner, not schema)
--   - Variant batching: NO for now — keep one call per variant while we're
--     calibrating. Switch to batched JSON responses once we have 2 states
--     worth of calibration data and know which variants are winners.
--   - Haiku routing: yes for enum/bool, Sonnet for text/structured_json.

alter table public.extraction_prompt_variants
  add column if not exists target_model text not null default 'claude-sonnet-4-6';

-- Backfill based on expected_shape
update public.extraction_prompt_variants set target_model = 'claude-haiku-4-5-20251001'
where expected_shape in ('enum', 'bool');

update public.extraction_prompt_variants set target_model = 'claude-sonnet-4-6'
where expected_shape in ('text', 'structured_json');

-- Constraint: keep target_model to known Claude model IDs
alter table public.extraction_prompt_variants
  drop constraint if exists extraction_prompt_variants_target_model_chk;
alter table public.extraction_prompt_variants
  add  constraint extraction_prompt_variants_target_model_chk
  check (target_model in (
    'claude-opus-4-7',
    'claude-sonnet-4-6',
    'claude-haiku-4-5-20251001'
  ));

comment on column public.extraction_prompt_variants.target_model is
  'Which Claude model runs this variant. Enum/bool shapes default to Haiku (cheap, fast, enough for single-word answers); text/structured_json default to Sonnet (needs reasoning). Override at insert time if a specific variant performs better on a different tier.';
