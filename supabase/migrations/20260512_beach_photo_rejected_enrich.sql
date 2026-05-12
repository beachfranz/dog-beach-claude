-- Enrich beach_photo_rejected with photo metadata at trash time, so the
-- Tier 2 photo-curation model can train on feature-parity labels.
--
-- Problem: tombstones today carry only (fid, source, external_id, rejected_at).
-- The actual photo's title / distance / relevance / name_match / photographer
-- were DELETE'd alongside the row. Without those features the negative class
-- is structurally distinguishable from positives via missingness alone —
-- label leakage (Polyzotis et al. 2018, training-serving skew).
--
-- Adds columns; defaults NULL. Existing 946 tombstones have NULL features
-- and can either be backfilled via Flickr/WC API re-fetch or dropped from
-- training. Forward: edge function admin-curate-beach captures these at
-- tombstone-write time.

alter table public.beach_photo_rejected
  add column if not exists title text,
  add column if not exists artist text,
  add column if not exists distance_m int,
  add column if not exists relevance_score numeric,
  add column if not exists name_match_score numeric,
  add column if not exists composite_score numeric,
  add column if not exists license text;

comment on column public.beach_photo_rejected.title is
  'Photo title captured at trash time; used as a feature for the Tier 2 photo-quality model.';
comment on column public.beach_photo_rejected.artist is
  'Photographer captured at trash time. Used by photographer-kept-rate feature + future blocklist learning.';
comment on column public.beach_photo_rejected.distance_m is
  'Distance to beach centroid at trash time. Could differ from any future loader run if the beach was moved.';
comment on column public.beach_photo_rejected.relevance_score is
  'Term-relevance score at trash time, copied from source_meta.';
comment on column public.beach_photo_rejected.name_match_score is
  'Name-match score at trash time, copied from source_meta.';
comment on column public.beach_photo_rejected.composite_score is
  'Composite score at trash time, copied from source_meta.';
comment on column public.beach_photo_rejected.license is
  'License code at trash time (e.g., CC-BY-2.0).';
