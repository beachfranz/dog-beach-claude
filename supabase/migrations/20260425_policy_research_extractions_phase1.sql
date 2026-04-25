-- Phase 1 of POLICY_RESEARCH_MIGRATION (2026-04-25)
--
-- Create the policy_research_extractions table + indexes. Pure additive —
-- table starts empty. Mirrors park_url_extractions shape so populate_from_
-- research can use the same evidence-emission pattern as populate_from_park_url.
--
-- See docs/POLICY_RESEARCH_MIGRATION.md for the full design.

create table public.policy_research_extractions (
  id              bigserial primary key,
  fid             integer not null references public.locations_stage(fid) on delete cascade,
  extracted_at    timestamptz not null default now(),

  -- Outcome
  extraction_status text not null check (extraction_status in
    ('success','no_sources','llm_failed','low_confidence','imported_legacy')),

  -- Origin tag — distinguishes data lineage. Currently:
  --   'v2_dog_policy_old'  — imported from beaches_staging_new (legacy v2-* output)
  --   'v2_dog_policy_v2'   — output from the rewritten v2-* edge functions
  --   'manual'             — admin-entered
  -- Each origin maps to a different `source` value when populate_from_research
  -- emits evidence rows: 'old_school_llm', 'research', or 'manual'.
  origin text not null check (origin in
    ('v2_dog_policy_old','v2_dog_policy_v2','manual')),

  -- Research-specific provenance
  research_query   text,                -- search query used (Tavily, etc.)
  source_urls      text[],              -- all URLs consulted
  primary_source_url text,              -- the single most-weighted URL
  source_count     int,                 -- how many distinct sources contributed
  raw_inputs       jsonb,               -- {url: cleaned_text} for replay/audit
  extraction_model text,
  extraction_notes text,
  extraction_confidence numeric(3,2),

  -- Extracted fields (NEW pipeline column names + jsonb shapes)
  dogs_allowed          text check (dogs_allowed is null or dogs_allowed in
    ('yes','no','seasonal','restricted','unknown')),
  dogs_leash_required   text check (dogs_leash_required is null or dogs_leash_required in
    ('required','off_leash_ok','mixed','unknown')),
  dogs_restricted_hours jsonb,  -- [{"start":"HH:MM","end":"HH:MM"}]
  dogs_seasonal_rules   jsonb,  -- [{"from":"MM-DD","to":"MM-DD","notes":"..."}]
  dogs_zone_description text,
  dogs_policy_notes     text,

  hours_text         text,
  open_time          time,
  close_time         time,
  has_parking        boolean,
  parking_type       text check (parking_type is null or parking_type in
    ('lot','street','metered','mixed','none')),
  parking_notes      text,
  has_restrooms      boolean,
  has_showers        boolean,
  has_drinking_water boolean,
  has_lifeguards     boolean,
  has_disabled_access boolean,
  has_food           boolean,
  has_fire_pits      boolean,
  has_picnic_area    boolean,

  -- Idempotency: one row per (fid, primary_source_url, origin) so re-running
  -- the same extractor against the same source updates in place.
  unique (fid, primary_source_url, origin)
);

create index pre_fid_idx          on public.policy_research_extractions(fid);
create index pre_status_idx       on public.policy_research_extractions(extraction_status);
create index pre_origin_idx       on public.policy_research_extractions(origin);
create index pre_extracted_at_idx on public.policy_research_extractions(extracted_at desc);

comment on table public.policy_research_extractions is
  'NEW pipeline staging for LLM-extracted dog/practical policy data. Mirrors park_url_extractions shape so populate_from_research can use the same evidence-emission pattern as populate_from_park_url. Holds output from v2-* dog-policy edge functions (after rewrite). Backfilled from beaches_staging_new with origin=v2_dog_policy_old at migration time.';
