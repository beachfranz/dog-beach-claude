-- Phase 3 of POLICY_RESEARCH_MIGRATION (2026-04-25)
--
-- One-time backfill: copy 953 rows of LLM-extracted policy data from
-- beaches_staging_new into policy_research_extractions with
-- origin='v2_dog_policy_old'. Re-shapes seasonal_closures jsonb from
-- {start,end,reason} to {from,to,notes} to match the NEW pipeline shape.
--
-- Idempotent via ON CONFLICT DO NOTHING on (fid, primary_source_url, origin)
-- so re-running is safe.
--
-- Pure additive at this stage: populate_from_research still reads from
-- beaches_staging_new (until Phase 4 swaps it). No effect on existing
-- canonical resolutions.

insert into public.policy_research_extractions (
  fid, extracted_at, extraction_status, origin,
  primary_source_url, source_urls, source_count,
  extraction_confidence, extraction_notes,
  dogs_allowed, dogs_leash_required,
  dogs_restricted_hours, dogs_seasonal_rules, dogs_zone_description,
  dogs_policy_notes, hours_text,
  has_parking, parking_type, parking_notes,
  has_restrooms, has_showers, has_drinking_water, has_lifeguards,
  has_disabled_access, has_food, has_fire_pits, has_picnic_area
)
select
  bsn.src_fid,
  coalesce(bsn.dogs_policy_updated_at, now()),
  case when bsn.dogs_allowed is not null then 'success'::text
       else 'imported_legacy'::text end,
  'v2_dog_policy_old',
  bsn.dogs_policy_source_url,
  case when bsn.dogs_policy_source_url is not null
       then ARRAY[bsn.dogs_policy_source_url]
       else NULL::text[] end,
  case when bsn.dogs_policy_source_url is not null then 1 else 0 end,
  public.bsn_confidence_to_numeric(bsn.enrichment_confidence),
  'imported from beaches_staging_new on ' || now()::date,
  public.bsn_dogs_allowed_to_enum(bsn.dogs_allowed),
  public.bsn_leash_to_enum(bsn.dogs_leash_required),
  bsn.dogs_daily_windows,
  -- Re-shape OLD seasonal_closures {start,end,reason} → NEW {from,to,notes}
  case
    when bsn.dogs_seasonal_closures is null
      or jsonb_typeof(bsn.dogs_seasonal_closures) <> 'array'
      or jsonb_array_length(bsn.dogs_seasonal_closures) = 0
    then null
    else (
      select jsonb_agg(jsonb_build_object(
        'from',  e->>'start',
        'to',    e->>'end',
        'notes', e->>'reason'
      ))
      from jsonb_array_elements(bsn.dogs_seasonal_closures) e
    )
  end,
  coalesce(bsn.dogs_allowed_areas, bsn.dogs_off_leash_area),
  bsn.dogs_policy_notes,
  bsn.hours_text,
  bsn.has_parking, public.bsn_parking_type_to_enum(bsn.parking_type), bsn.parking_notes,
  bsn.has_restrooms, bsn.has_showers, bsn.has_drinking_water, bsn.has_lifeguards,
  bsn.has_disabled_access, bsn.has_food, bsn.has_fire_pits, bsn.has_picnic_area
from public.beaches_staging_new bsn
join public.locations_stage s on s.fid = bsn.src_fid
where bsn.src_fid is not null
  and bsn.dogs_allowed is not null
on conflict (fid, primary_source_url, origin) do nothing;
