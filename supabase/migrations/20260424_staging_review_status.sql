-- Add review_status + review_notes to us_beach_points_staging (2026-04-24)
-- Borrows the row-level quality-flag pattern from beaches_staging_new.
-- Pipeline sets 'needs_review' when something looks off (multi-source
-- disagreement, low-confidence answer, geocode mismatch, etc.). Admin
-- UI lets humans flip to 'verified' or 'flagged'.

alter table public.us_beach_points_staging
  add column if not exists review_status text
    check (review_status is null or review_status in
      ('needs_review','verified','flagged'));

alter table public.us_beach_points_staging
  add column if not exists review_notes text;

create index if not exists ubps_review_status_idx
  on public.us_beach_points_staging(review_status)
  where review_status is not null;

comment on column public.us_beach_points_staging.review_status is
  'Row-level quality flag. NULL = pipeline has not flagged anything (default). needs_review = pipeline detected something to check (multi-source disagreement, low confidence, geocode mismatch). verified = human confirmed. flagged = human marked as bad data.';

comment on column public.us_beach_points_staging.review_notes is
  'Free-form notes about why this row needs review or what was verified. Set by pipeline (machine-generated reason) or admin UI (human-typed).';
