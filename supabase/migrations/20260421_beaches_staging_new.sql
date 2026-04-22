create table if not exists public.beaches_staging_new (
  id                      serial primary key,
  src_fid                 integer,                        -- fid from source CSV
  display_name            text not null,
  latitude                double precision,
  longitude               double precision,

  -- Address fields (from geocoding)
  raw_address             text,
  street_number           text,
  route                   text,
  city                    text,
  county                  text,
  state                   text,
  zip                     text,
  country                 text,

  -- Jurisdiction
  governing_jurisdiction           text,                  -- governing city | governing county | governing state | governing federal
  governing_body                   text,                  -- e.g. "City of San Clemente"
  governing_body_source            text,                  -- geocode | name_keyword | name_keyword+geocode
  governing_body_notes             text,                  -- narrative of how governing_body was determined
  governing_city                   text,
  governing_county                 text,
  governing_state                  text,
  governing_jurisdiction_ai        text,                  -- governing city | governing county | governing state | governing federal
  governing_jurisdiction_ai_confidence text,              -- high | low | unknown
  governing_body_agreement         text,                  -- agree | disagree | unresolved

  -- Pipeline status
  quality_tier            text not null default 'bronze', -- bronze | silver | gold
  dedup_status            text,                           -- null | duplicate | reviewed
  review_status           text,
  review_notes            text,
  geocode_status          text,                           -- OK | ZERO_RESULTS | ERROR
  geocode_quality         text,                           -- ROOFTOP | RANGE_INTERPOLATED | GEOMETRIC_CENTER | APPROXIMATE

  -- Phase 1 — dog policy classification
  dogs_allowed            boolean,
  access_rule             text,                           -- off_leash | on_leash | mixed | prohibited
  policy_source_url       text,
  policy_confidence       text,                           -- confirmed | probable | needs_review | unknown
  policy_notes            text,

  -- Phase 2 — structured extraction
  allowed_hours_text      text,
  seasonal_start          text,
  seasonal_end            text,
  dogs_prohibited_start   text,
  dogs_prohibited_end     text,
  day_restrictions        text,
  zone_description        text,
  dogs_prohibited_reason  text,

  created_at              timestamptz not null default now(),
  updated_at              timestamptz not null default now()
);

create index if not exists beaches_staging_new_state_idx
  on public.beaches_staging_new (state);

create index if not exists beaches_staging_new_county_idx
  on public.beaches_staging_new (state, county);

create index if not exists beaches_staging_new_quality_tier_idx
  on public.beaches_staging_new (quality_tier);

create index if not exists beaches_staging_new_src_fid_idx
  on public.beaches_staging_new (src_fid);
