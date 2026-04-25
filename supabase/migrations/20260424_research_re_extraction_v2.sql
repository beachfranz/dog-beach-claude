-- Re-extraction columns on beach_policy_research (2026-04-24)
--
-- Why: the old pipeline asked the LLM for confidence as a binary
-- 'high'|'low' label, which we mapped to 0.80|0.50 by fiat. This
-- overstates the LLM's actual self-estimated certainty — when we
-- compare against park_url's calibrated 0.00-1.00 numeric confidence,
-- research wins lopsidedly even when the LLM was probably equally
-- uncertain.
--
-- Fix: re-run extraction over the existing raw_text dumps in
-- beach_policy_research using the same numeric-confidence prompt as
-- extract_from_park_url.py. New columns hold the re-extracted parsed
-- fields + a real numeric confidence. Apples-to-apples with park_url.
--
-- Coverage: ~316 research rows have raw_text. Other beaches got
-- research data via cross-beach sharing (same governing body) — they
-- keep the old confidence. Re-extraction script writes only to v2.

alter table public.beach_policy_research
  add column if not exists parsed_at_v2          timestamptz,
  add column if not exists extraction_model_v2   text,
  add column if not exists extraction_confidence_v2 numeric(3,2),
  add column if not exists extraction_notes_v2   text,
  -- Dogs
  add column if not exists dogs_allowed_v2          text,
  add column if not exists dogs_leash_required_v2   text,
  add column if not exists dogs_restricted_hours_v2 jsonb,
  add column if not exists dogs_seasonal_rules_v2   jsonb,
  add column if not exists dogs_zone_description_v2 text,
  add column if not exists dogs_policy_notes_v2     text,
  -- Practical
  add column if not exists hours_text_v2          text,
  add column if not exists open_time_v2           time,
  add column if not exists close_time_v2          time,
  add column if not exists has_parking_v2         boolean,
  add column if not exists parking_type_v2        text,
  add column if not exists parking_notes_v2       text,
  add column if not exists has_restrooms_v2       boolean,
  add column if not exists has_showers_v2         boolean,
  add column if not exists has_drinking_water_v2  boolean,
  add column if not exists has_lifeguards_v2      boolean,
  add column if not exists has_disabled_access_v2 boolean,
  add column if not exists has_food_v2            boolean,
  add column if not exists has_fire_pits_v2       boolean,
  add column if not exists has_picnic_area_v2     boolean;

comment on column public.beach_policy_research.extraction_confidence_v2 is
  'Re-extracted numeric confidence (0.00-1.00) using the same prompt as park_url scraping. Comparable to park_url.extraction_confidence. Older fields/v1 columns and beaches_staging_new.enrichment_confidence remain for backward compat.';

create index if not exists bpr_v2_parsed_idx
  on public.beach_policy_research(parsed_at_v2)
  where parsed_at_v2 is not null;

-- ── Update populate_from_research to prefer v2 fields when present ──────────
create or replace function public.populate_from_research(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with bsn as (
    select
      bsn.src_fid as fid,
      bsn.id      as bsn_id,
      -- Confidence: prefer v2 numeric if any beach_policy_research row for
      -- this beach has been re-extracted, else fall back to old text->num map
      coalesce(
        (select max(bpr.extraction_confidence_v2)
         from public.beach_policy_research bpr
         where bpr.staging_id = bsn.id and bpr.parsed_at_v2 is not null),
        public.bsn_confidence_to_numeric(bsn.enrichment_confidence)
      ) as confidence,
      -- For each field: prefer v2 from beach_policy_research; else old
      -- value from beaches_staging_new.
      coalesce(
        (select bpr.dogs_allowed_v2 from public.beach_policy_research bpr
         where bpr.staging_id = bsn.id and bpr.dogs_allowed_v2 is not null
         order by bpr.parsed_at_v2 desc nulls last limit 1),
        public.bsn_dogs_allowed_to_enum(bsn.dogs_allowed)
      ) as dogs_allowed,
      coalesce(
        (select bpr.dogs_leash_required_v2 from public.beach_policy_research bpr
         where bpr.staging_id = bsn.id and bpr.dogs_leash_required_v2 is not null
         order by bpr.parsed_at_v2 desc nulls last limit 1),
        public.bsn_leash_to_enum(bsn.dogs_leash_required)
      ) as leash_required,
      coalesce(
        (select bpr.dogs_restricted_hours_v2 from public.beach_policy_research bpr
         where bpr.staging_id = bsn.id and bpr.dogs_restricted_hours_v2 is not null
         order by bpr.parsed_at_v2 desc nulls last limit 1),
        bsn.dogs_daily_windows
      ) as dogs_restricted_hours,
      coalesce(
        (select bpr.dogs_seasonal_rules_v2 from public.beach_policy_research bpr
         where bpr.staging_id = bsn.id and bpr.dogs_seasonal_rules_v2 is not null
         order by bpr.parsed_at_v2 desc nulls last limit 1),
        bsn.dogs_seasonal_closures
      ) as dogs_seasonal_closures,
      coalesce(
        (select bpr.dogs_zone_description_v2 from public.beach_policy_research bpr
         where bpr.staging_id = bsn.id and bpr.dogs_zone_description_v2 is not null
         order by bpr.parsed_at_v2 desc nulls last limit 1),
        coalesce(bsn.dogs_allowed_areas, bsn.dogs_off_leash_area)
      ) as zone_description,
      coalesce(
        (select bpr.dogs_policy_notes_v2 from public.beach_policy_research bpr
         where bpr.staging_id = bsn.id and bpr.dogs_policy_notes_v2 is not null
         order by bpr.parsed_at_v2 desc nulls last limit 1),
        bsn.dogs_policy_notes
      ) as dogs_policy_notes,
      -- Practical: prefer v2
      coalesce(
        (select bpr.hours_text_v2 from public.beach_policy_research bpr
         where bpr.staging_id = bsn.id and bpr.hours_text_v2 is not null
         order by bpr.parsed_at_v2 desc nulls last limit 1),
        bsn.hours_text
      ) as hours_text,
      coalesce(
        (select bpr.has_parking_v2 from public.beach_policy_research bpr
         where bpr.staging_id = bsn.id and bpr.has_parking_v2 is not null
         order by bpr.parsed_at_v2 desc nulls last limit 1),
        bsn.has_parking
      ) as has_parking,
      coalesce(
        (select public.bsn_parking_type_to_enum(bpr.parking_type_v2)
           from public.beach_policy_research bpr
         where bpr.staging_id = bsn.id and bpr.parking_type_v2 is not null
         order by bpr.parsed_at_v2 desc nulls last limit 1),
        public.bsn_parking_type_to_enum(bsn.parking_type)
      ) as parking_type,
      coalesce(
        (select bpr.parking_notes_v2 from public.beach_policy_research bpr
         where bpr.staging_id = bsn.id and bpr.parking_notes_v2 is not null
         order by bpr.parsed_at_v2 desc nulls last limit 1),
        bsn.parking_notes
      ) as parking_notes,
      bsn.has_restrooms, bsn.has_showers, bsn.has_lifeguards, bsn.has_picnic_area,
      bsn.has_food, bsn.has_drinking_water, bsn.has_fire_pits, bsn.has_disabled_access
    from public.beaches_staging_new bsn
    join public.locations_stage s on s.fid = bsn.src_fid
    where bsn.src_fid is not null
      and (p_fid is null or bsn.src_fid = p_fid)
  ),
  dogs_built as (
    select fid, confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',          dogs_allowed,
        'leash_required',   leash_required,
        'restricted_hours', dogs_restricted_hours,
        'seasonal_rules',
          case
            when dogs_seasonal_closures is null
              or jsonb_typeof(dogs_seasonal_closures) <> 'array'
              or jsonb_array_length(dogs_seasonal_closures) = 0
            then null
            -- v2 already in {from,to,notes}; v1 is {start,end,reason}.
            -- Detect by key presence and transform if needed.
            when dogs_seasonal_closures->0 ? 'from' then dogs_seasonal_closures
            else (
              select jsonb_agg(jsonb_build_object(
                'from', e->>'start', 'to', e->>'end', 'notes', e->>'reason'
              ))
              from jsonb_array_elements(dogs_seasonal_closures) e
            )
          end,
        'zone_description', zone_description,
        'notes',            dogs_policy_notes
      )) as v
    from bsn
  ),
  ins_dogs as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'dogs', 'research', confidence, v, now()
    from dogs_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source) do update
      set confidence = excluded.confidence, claimed_values = excluded.claimed_values,
          updated_at = now(), is_canonical = false
    returning 1
  ),
  practical_built as (
    select fid, confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'hours_text',         hours_text,
        'has_parking',        has_parking,
        'parking_type',       parking_type,
        'parking_notes',      parking_notes,
        'has_restrooms',      has_restrooms,
        'has_showers',        has_showers,
        'has_lifeguards',     has_lifeguards,
        'has_picnic_area',    has_picnic_area,
        'has_food',           has_food,
        'has_drinking_water', has_drinking_water,
        'has_fire_pits',      has_fire_pits,
        'has_disabled_access',has_disabled_access
      )) as v
    from bsn
  ),
  ins_practical as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, updated_at)
    select fid, 'practical', 'research', confidence, v, now()
    from practical_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source) do update
      set confidence = excluded.confidence, claimed_values = excluded.claimed_values,
          updated_at = now(), is_canonical = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from ins_dogs union all select * from ins_practical
  ) _;
  return rows_touched;
end;
$$;
