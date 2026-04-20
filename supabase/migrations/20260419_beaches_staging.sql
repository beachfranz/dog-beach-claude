-- beaches_staging: enrichment and onboarding table
-- Records graduate from bronze → silver → gold → platinum before promotion to beaches.

CREATE TABLE IF NOT EXISTS public.beaches_staging (

  -- ── Identity ────────────────────────────────────────────────────────────────
  id              serial PRIMARY KEY,
  source_fid      integer,                      -- fid from source CSV
  location_id     text UNIQUE,                  -- slug, populated before promotion
  display_name    text NOT NULL,

  -- ── Geography (from reverse geocoding) ─────────────────────────────────────
  latitude        numeric NOT NULL,
  longitude       numeric NOT NULL,
  formatted_address text,
  street_number   text,
  route           text,
  city            text,
  county          text,
  state           text,
  country         text,
  zip             text,
  governing_jurisdiction text,                  -- federal | state | county | municipal
  governing_body  text,                         -- e.g. "California State Parks"

  -- ── Dog access policy (scoring engine fields) ───────────────────────────────
  dogs_allowed            boolean,
  dogs_prohibited_reason  text,
  access_rule             text,                 -- off_leash | on_leash | mixed | prohibited
  access_scope            text,                 -- full_beach | designated_area | partial
  zone_description        text,
  seasonal_start          text,                 -- e.g. "10-01" (MM-DD)
  seasonal_end            text,                 -- e.g. "05-31"
  dogs_prohibited_start   text,                 -- daily blackout start HH:MM
  dogs_prohibited_end     text,                 -- daily blackout end HH:MM
  day_restrictions        text,                 -- e.g. "weekdays only"
  allowed_hours_text      text,                 -- human-readable summary

  -- ── Policy research ─────────────────────────────────────────────────────────
  policy_source_url       text,
  policy_verified_date    date,
  policy_confidence       text,                 -- high | medium | low
  policy_notes            text,

  -- ── Data quality ────────────────────────────────────────────────────────────
  quality_tier    text NOT NULL DEFAULT 'bronze', -- bronze | silver | gold | platinum
  review_status   text NOT NULL DEFAULT 'OK',     -- OK | Needs Review
  review_notes    text,

  -- ── Promotion ───────────────────────────────────────────────────────────────
  promoted_at     timestamptz,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT beaches_staging_source_fid_unique UNIQUE (source_fid)
);

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END;
$$;

DROP TRIGGER IF EXISTS beaches_staging_updated_at ON public.beaches_staging;
CREATE TRIGGER beaches_staging_updated_at
  BEFORE UPDATE ON public.beaches_staging
  FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

-- ── beach_policy_research: raw research data linked to staging ───────────────

CREATE TABLE IF NOT EXISTS public.beach_policy_research (
  id              serial PRIMARY KEY,
  staging_id      integer NOT NULL REFERENCES public.beaches_staging(id) ON DELETE CASCADE,
  source_url      text,
  source_type     text,                         -- city_website | county_website | state_website | federal_website | other
  raw_text        text,                         -- raw extracted text from the source
  notes           text,
  researched_at   timestamptz NOT NULL DEFAULT now()
);

-- Indexes
CREATE INDEX IF NOT EXISTS beaches_staging_state_idx    ON public.beaches_staging (state);
CREATE INDEX IF NOT EXISTS beaches_staging_county_idx   ON public.beaches_staging (county);
CREATE INDEX IF NOT EXISTS beaches_staging_tier_idx     ON public.beaches_staging (quality_tier);
CREATE INDEX IF NOT EXISTS beaches_staging_review_idx   ON public.beaches_staging (review_status);
CREATE INDEX IF NOT EXISTS policy_research_staging_idx  ON public.beach_policy_research (staging_id);

-- RLS off for now (service role only during enrichment phase)
ALTER TABLE public.beaches_staging      DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.beach_policy_research DISABLE ROW LEVEL SECURITY;
