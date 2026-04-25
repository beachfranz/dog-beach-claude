-- populate_from_research — migrate parsed dog/practical fields from
-- beaches_staging_new (the OLD pipeline's already-parsed LLM-research
-- output) into us_beach_points_staging via the side provenance table.
-- (2026-04-24)
--
-- The OLD pipeline's research → parse loop already produced 953 rows
-- with dogs_allowed and 800+ rows with practical fields. This populator
-- imports those into the new staging via beach_enrichment_provenance
-- without re-running any LLM calls. Bridge: beaches_staging_new.src_fid
-- = us_beach_points_staging.fid.
--
-- Distinct from populate_from_cpad/ccc/etc: those are spatial joins
-- against external sources. This is a SCHEMA migration from the old
-- staging table — same data, new home.

-- ── Add 'research' to the source CHECK enum ─────────────────────────────────
alter table public.beach_enrichment_provenance
  drop constraint if exists beach_enrichment_provenance_source_check;

alter table public.beach_enrichment_provenance
  add constraint beach_enrichment_provenance_source_check
  check (source in (
    'manual','plz','cpad','tiger_places','ccc','llm','web_scrape',
    'research',          -- structured tiered research (research_prompts × tier)
    'csp_parks','park_operators','nps_places','tribal_lands','military_bases',
    'pad_us','sma_code_mappings','jurisdictions',
    'csp_places'
  ));

-- ── Helper: map old dogs_leash_required text → new enum ─────────────────────
create or replace function public.bsn_leash_to_enum(p_v text)
returns text
language sql immutable
as $$
  select case lower(trim(coalesce(p_v, '')))
    when 'yes'   then 'required'
    when 'no'    then 'off_leash_ok'
    when 'mixed' then 'mixed'
    else null
  end;
$$;

-- ── Helper: map enrichment_confidence text → numeric ────────────────────────
create or replace function public.bsn_confidence_to_numeric(p_v text)
returns numeric
language sql immutable
as $$
  select case lower(trim(coalesce(p_v, '')))
    when 'high'   then 0.80::numeric
    when 'medium' then 0.65::numeric
    when 'low'    then 0.50::numeric
    else               0.55::numeric  -- default when unset
  end;
$$;

-- ── Main populator ──────────────────────────────────────────────────────────
create or replace function public.populate_from_research(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int;
begin
  with bsn as (
    select
      bsn.src_fid as fid,
      public.bsn_confidence_to_numeric(bsn.enrichment_confidence) as confidence,
      bsn.dogs_allowed,
      public.bsn_leash_to_enum(bsn.dogs_leash_required) as leash_required,
      bsn.dogs_daily_windows,
      bsn.dogs_seasonal_closures,
      coalesce(bsn.dogs_allowed_areas, bsn.dogs_off_leash_area) as zone_description,
      bsn.dogs_policy_notes,
      bsn.hours_text,
      bsn.has_parking, bsn.parking_type, bsn.parking_notes,
      bsn.has_restrooms, bsn.has_showers, bsn.has_lifeguards, bsn.has_picnic_area,
      bsn.has_food, bsn.has_drinking_water, bsn.has_fire_pits, bsn.has_disabled_access
    from public.beaches_staging_new bsn
    -- Only rows that bridge to us_beach_points_staging
    join public.us_beach_points_staging s on s.fid = bsn.src_fid
    where bsn.src_fid is not null
      and (p_fid is null or bsn.src_fid = p_fid)
  ),
  -- Build dogs claimed_values
  dogs_built as (
    select fid, confidence,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',          dogs_allowed,
        'leash_required',   leash_required,
        'restricted_hours', dogs_daily_windows,
        -- Transform old seasonal_closures shape {start,end,reason}
        -- to new seasonal_rules shape {from,to,notes}
        'seasonal_rules',
          case
            when dogs_seasonal_closures is null
              or jsonb_typeof(dogs_seasonal_closures) <> 'array'
              or jsonb_array_length(dogs_seasonal_closures) = 0
            then null
            else (
              select jsonb_agg(jsonb_build_object(
                'from',  e->>'start',
                'to',    e->>'end',
                'notes', e->>'reason'
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
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  ),
  -- Build practical claimed_values (open/close_time aren't in bsn — leave for LLM)
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
      set confidence     = excluded.confidence,
          claimed_values = excluded.claimed_values,
          updated_at     = now(),
          is_canonical   = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from ins_dogs union all select * from ins_practical
  ) _;

  return rows_touched;
end;
$$;

comment on function public.populate_from_research(int) is
  'Layer 2: migrate parsed dog/practical fields from beaches_staging_new (old pipeline LLM-research output) into beach_enrichment_provenance for us_beach_points_staging. Bridge: bsn.src_fid = ubps.fid. Confidence from enrichment_confidence text (high=0.80, medium=0.65, low=0.50, default=0.55). Transforms old dogs_seasonal_closures {start,end,reason} → new seasonal_rules {from,to,notes}. dogs_daily_windows transferred as-is into restricted_hours.';
