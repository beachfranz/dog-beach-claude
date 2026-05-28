-- 20260527_dog_park_has_shade.sql
--
-- Add has_shade signal to dog_park_dog_policy. NULL = unknown
-- (neutral: neither positive nor negative on the score), to be
-- backfilled via curated extraction.

alter table public.dog_park_dog_policy
  add column if not exists has_shade boolean;

comment on column public.dog_park_dog_policy.has_shade is
  'True if park has meaningful shade (tree canopy or built shade '
  'structure covering a significant fraction of the play area). '
  'NULL = unknown. Drives the UV-scaled shade positive and the '
  'no-shade-in-sun gotcha in the v2 scoring model.';
