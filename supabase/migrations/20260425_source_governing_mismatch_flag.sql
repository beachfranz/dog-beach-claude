-- source_governing_mismatch flag (2026-04-25)
--
-- Adds a third review flag to populate_from_park_url(), alongside the
-- existing multi_cpad_disagreement detection. Fires when:
--
--   - A beach has at least one successful park_url evidence row
--   - The cpad_unit_name on that evidence row differs from the
--     strict-containing CPAD (the polygon ST_Contains'ing the beach
--     point — typically the smallest such polygon by area)
--
-- This catches the "containing CPAD has park_url=NULL, fell back to a
-- nearby CPAD's URL" cases (~75 beaches today, 13% of successful extracts).
-- Empirically the most consequential subset is when the source and
-- container differ in governance level (43 of those 75) — but the flag
-- fires on any unit-name mismatch since some same-level mismatches
-- (e.g., adjacent state parks) are also worth a human eyeball.
--
-- Beaches with NO containing CPAD don't fire this flag — that's not a
-- mismatch, it's a buffer-rescued attribution and is expected.

create or replace function public.populate_from_park_url(p_fid int default null)
returns int
language plpgsql
as $$
declare rows_touched int := 0;
begin
  with successful as (
    select * from public.park_url_extractions
    where extraction_status = 'success'
      and (p_fid is null or fid = p_fid)
  ),
  dogs_built as (
    select fid, source_url, scraped_at, extraction_confidence,
           cpad_unit_name, extraction_type,
      jsonb_strip_nulls(jsonb_build_object(
        'allowed',          dogs_allowed,
        'leash_required',   dogs_leash_required,
        'restricted_hours', dogs_restricted_hours,
        'seasonal_rules',   dogs_seasonal_rules,
        'zone_description', dogs_zone_description,
        'notes',            dogs_policy_notes
      )) as v
    from successful
  ),
  ins_dogs as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, source_url,
       cpad_unit_name, extraction_type, updated_at)
    select fid, 'dogs', 'park_url',
      coalesce(extraction_confidence, 0.85),
      v, source_url, cpad_unit_name, extraction_type, now()
    from dogs_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source, coalesce(source_url, '')) do update
      set confidence      = excluded.confidence,
          claimed_values  = excluded.claimed_values,
          cpad_unit_name  = excluded.cpad_unit_name,
          extraction_type = excluded.extraction_type,
          updated_at      = now(),
          is_canonical    = false
    returning 1
  ),
  practical_built as (
    select fid, source_url, extraction_confidence,
           cpad_unit_name, extraction_type,
      jsonb_strip_nulls(jsonb_build_object(
        'hours_text',         hours_text,
        'open_time',          open_time::text,
        'close_time',         close_time::text,
        'has_parking',        has_parking,
        'parking_type',       parking_type,
        'parking_notes',      parking_notes,
        'has_restrooms',      has_restrooms,
        'has_showers',        has_showers,
        'has_drinking_water', has_drinking_water,
        'has_lifeguards',     has_lifeguards,
        'has_disabled_access',has_disabled_access,
        'has_food',           has_food,
        'has_fire_pits',      has_fire_pits,
        'has_picnic_area',    has_picnic_area
      )) as v
    from successful
  ),
  ins_practical as (
    insert into public.beach_enrichment_provenance
      (fid, field_group, source, confidence, claimed_values, source_url,
       cpad_unit_name, extraction_type, updated_at)
    select fid, 'practical', 'park_url',
      coalesce(extraction_confidence, 0.85),
      v, source_url, cpad_unit_name, extraction_type, now()
    from practical_built
    where v <> '{}'::jsonb
    on conflict (fid, field_group, source, coalesce(source_url, '')) do update
      set confidence      = excluded.confidence,
          claimed_values  = excluded.claimed_values,
          cpad_unit_name  = excluded.cpad_unit_name,
          extraction_type = excluded.extraction_type,
          updated_at      = now(),
          is_canonical    = false
    returning 1
  )
  select count(*) into rows_touched from (
    select * from ins_dogs union all select * from ins_practical
  ) _;

  -- ── Flag 1: Multi-CPAD disagreement ───────────────────────────────────
  -- ≥2 successful park_url evidence rows from DIFFERENT cpad_unit_names
  -- in the SAME field_group for one beach.
  with disagreeing_beaches as (
    select e.fid,
           e.field_group,
           array_agg(distinct e.cpad_unit_name order by e.cpad_unit_name) as units
    from public.beach_enrichment_provenance e
    where e.source = 'park_url'
      and e.cpad_unit_name is not null
      and (p_fid is null or e.fid = p_fid)
    group by e.fid, e.field_group
    having count(distinct e.cpad_unit_name) > 1
  ),
  per_beach as (
    select fid,
           string_agg(field_group || ':[' || array_to_string(units, ', ') || ']',
                      '; ' order by field_group) as detail
    from disagreeing_beaches
    group by fid
  )
  update public.locations_stage s
     set review_status = 'needs_review',
         review_notes  = case
           when s.review_notes is null or s.review_notes = '' then
             'multi_cpad_disagreement: ' || pb.detail
           when s.review_notes ilike '%multi_cpad_disagreement%' then
             regexp_replace(s.review_notes,
                            'multi_cpad_disagreement:[^|]*',
                            'multi_cpad_disagreement: ' || pb.detail)
           else
             s.review_notes || ' | multi_cpad_disagreement: ' || pb.detail
         end
    from per_beach pb
   where s.fid = pb.fid;

  -- ── Flag 2: Source-governing mismatch ─────────────────────────────────
  -- Source CPAD (the URL we pulled from) differs from the strict-
  -- containing CPAD (the polygon the beach point sits inside). Fires
  -- only when the beach IS inside a CPAD polygon — buffer-rescued
  -- attributions (no containing polygon) don't count as mismatches.
  with containing_unit as (
    select distinct on (b.fid) b.fid, c.unit_name as contain_unit
    from public.us_beach_points b
    join public.cpad_units c on ST_Contains(c.geom, b.geom::geometry)
    where p_fid is null or b.fid = p_fid
    order by b.fid, ST_Area(c.geom::geography) asc
  ),
  mismatched_beaches as (
    select e.fid,
           cu.contain_unit,
           array_agg(distinct e.cpad_unit_name order by e.cpad_unit_name) as source_units
    from public.beach_enrichment_provenance e
    join containing_unit cu on cu.fid = e.fid
    where e.source = 'park_url'
      and e.cpad_unit_name is not null
      and e.cpad_unit_name <> cu.contain_unit
      and (p_fid is null or e.fid = p_fid)
    group by e.fid, cu.contain_unit
  ),
  mismatch_per_beach as (
    select fid,
           'contains:' || contain_unit ||
           ' / source:[' || array_to_string(source_units, ', ') || ']' as detail
    from mismatched_beaches
  )
  update public.locations_stage s
     set review_status = 'needs_review',
         review_notes  = case
           when s.review_notes is null or s.review_notes = '' then
             'source_governing_mismatch: ' || mp.detail
           when s.review_notes ilike '%source_governing_mismatch%' then
             regexp_replace(s.review_notes,
                            'source_governing_mismatch:[^|]*',
                            'source_governing_mismatch: ' || mp.detail)
           else
             s.review_notes || ' | source_governing_mismatch: ' || mp.detail
         end
    from mismatch_per_beach mp
   where s.fid = mp.fid;

  return rows_touched;
end;
$$;

comment on function public.populate_from_park_url(int) is
  'Layer 2 populator: emit dogs + practical evidence from park_url_extractions where extraction_status=success. One evidence row per (fid, field_group, source, source_url). Carries cpad_unit_name + extraction_type. Two review flags: (1) multi_cpad_disagreement when ≥2 successful pulls from different CPADs in the same field_group; (2) source_governing_mismatch when source CPAD differs from the strict-containing CPAD.';

-- Backfill: re-run on every beach so existing data picks up the new flag
select public.populate_from_park_url(NULL);
