-- Add curator_confidence to beach_policy_gold_set.
-- Distinct from the field_name='confidence' truth row (which is what the
-- SOURCE EXTRACTOR said its confidence was — we're truthing that value).
-- This column captures how sure the HUMAN CURATOR is about the truth value
-- they just saved. Drives downstream weighting when scoring sources.

begin;

alter table public.beach_policy_gold_set
  add column if not exists curator_confidence text
    check (curator_confidence in ('high', 'medium', 'low') or curator_confidence is null);

comment on column public.beach_policy_gold_set.curator_confidence is
  'Self-reported confidence of the human curator in the saved truth value (high/medium/low). NULL means not specified.';

commit;
