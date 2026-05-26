-- dog_park_photos — mirror of public.beach_photos for dog-park entities.
-- Same columns + same curation flow; FK target is dog_parks_gold.fid via
-- the per-pipeline convention column name dog_park_fid.

CREATE TABLE IF NOT EXISTS public.dog_park_photos (
  id             bigserial PRIMARY KEY,
  dog_park_fid   bigint NOT NULL REFERENCES public.dog_parks_gold(fid) ON DELETE CASCADE,
  source         text NOT NULL,
  external_id    text NOT NULL,
  image_url      text NOT NULL,
  thumb_url      text,
  attribution    text,
  license        text,
  captured_at    timestamptz,
  width          integer,
  height         integer,
  lat            double precision,
  lng            double precision,
  distance_m     integer,
  sort_order     integer,
  page_url       text,
  source_meta    jsonb,
  loaded_at      timestamptz NOT NULL DEFAULT now(),
  match_quality  text,
  curated_at     timestamptz,
  curated_by     text,
  hidden_at      timestamptz,
  hidden_by      text,
  UNIQUE (source, external_id, dog_park_fid)
);

CREATE INDEX IF NOT EXISTS dog_park_photos_fid_idx       ON public.dog_park_photos (dog_park_fid);
CREATE INDEX IF NOT EXISTS dog_park_photos_source_idx    ON public.dog_park_photos (source);
CREATE INDEX IF NOT EXISTS dog_park_photos_curated_idx   ON public.dog_park_photos (dog_park_fid) WHERE curated_at IS NOT NULL AND hidden_at IS NULL;

-- RLS: anon SELECTs curated, un-hidden rows only (mirrors beach_photos pattern).
ALTER TABLE public.dog_park_photos ENABLE ROW LEVEL SECURITY;

CREATE POLICY dog_park_photos_anon_read
  ON public.dog_park_photos FOR SELECT TO anon
  USING (curated_at IS NOT NULL AND hidden_at IS NULL);

GRANT SELECT ON public.dog_park_photos TO anon, authenticated;

NOTIFY pgrst, 'reload schema';
