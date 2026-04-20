-- Add dedup tracking columns to beaches_staging.
-- dedup_status: null = not yet reviewed | 'reviewed' = confirmed distinct | 'removed' = marked as duplicate
-- dedup_notes:  free-text explanation of the decision

ALTER TABLE public.beaches_staging
  ADD COLUMN IF NOT EXISTS dedup_status text,
  ADD COLUMN IF NOT EXISTS dedup_notes  text;

CREATE INDEX IF NOT EXISTS beaches_staging_dedup_idx ON public.beaches_staging (dedup_status);
