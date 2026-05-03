-- arena_beach_metadata v2 — simplify to is_canon-only.
--
-- Each field has exactly one is_canon=true variant (curated 2026-05-01:
-- enum/bool → Haiku direct_*, text → Sonnet direct_q/summary_q).
-- Use that variant's parsed_value as the canonical answer.
-- Multi-variant voting was producing noise because variants with different
-- shapes (enum vs text vs json) produce different parsed_value formats for
-- the same field — there's nothing to "vote" on.
--
-- Junk filter still applied to avoid surfacing 'unclear', 'unknown', etc.
-- Once we have multi-source data per beach, calibration-weighted voting
-- becomes meaningful and we can revisit.

drop view if exists public.arena_beach_metadata cascade;

create view public.arena_beach_metadata as
with canon as (
  select e.arena_group_id, v.field_name, e.parsed_value
    from public.beach_policy_extractions e
    join public.extraction_prompt_variants v on v.id = e.variant_id
   where v.is_canon = true
     and e.parse_succeeded
     and e.arena_group_id is not null
     and lower(coalesce(e.parsed_value, '')) not in
         ('unclear', 'unknown', '', 'none', 'none.', 'null',
          'no dog-specific text found', 'no designated off-leash zones')
),
pivoted as (
  select arena_group_id,
         max(case when field_name = 'dogs_allowed'                 then parsed_value end) as dogs_allowed,
         max(case when field_name = 'dogs_leash_required'          then parsed_value end) as dogs_leash_required,
         max(case when field_name = 'dogs_allowed_areas'           then parsed_value end) as dogs_allowed_areas,
         max(case when field_name = 'dogs_off_leash_area'          then parsed_value end) as dogs_off_leash_area,
         max(case when field_name = 'dogs_policy_notes'            then parsed_value end) as dogs_policy_notes,
         max(case when field_name = 'dogs_seasonal_restrictions'   then parsed_value end) as dogs_seasonal_restrictions,
         max(case when field_name = 'dogs_time_restrictions'       then parsed_value end) as dogs_time_restrictions,
         max(case when field_name = 'access_rule'                  then parsed_value end) as access_rule,
         max(case when field_name = 'access_text'                  then parsed_value end) as access_text,
         max(case when field_name = 'has_parking'                  then parsed_value end) as has_parking,
         max(case when field_name = 'parking_type'                 then parsed_value end) as parking_type,
         max(case when field_name = 'has_drinking_water'           then parsed_value end) as has_drinking_water,
         max(case when field_name = 'hours_text'                   then parsed_value end) as hours_text,
         max(case when field_name = 'raw_address'                  then parsed_value end) as extracted_address
    from canon
   group by arena_group_id
)
select a.fid                       as arena_group_id,
       a.name                      as beach_name,
       a.address                   as arena_address,
       a.nav_lat,
       a.nav_lon,
       a.park_name,
       a.county_name,
       p.dogs_allowed,
       p.dogs_leash_required,
       p.dogs_allowed_areas,
       p.dogs_off_leash_area,
       p.dogs_policy_notes,
       p.dogs_seasonal_restrictions,
       p.dogs_time_restrictions,
       p.access_rule,
       p.access_text,
       p.has_parking,
       p.parking_type,
       p.has_drinking_water,
       p.hours_text,
       p.extracted_address,
       coalesce(p.extracted_address, a.address) as best_address
  from public.arena a
  left join pivoted p on p.arena_group_id = a.fid
 where a.is_active = true
   and a.fid = a.group_id;

comment on view public.arena_beach_metadata is
  'Consumer surface for detail.html. One row per active arena beach group. '
  'Uses is_canon=true variant per field; junk values filtered. '
  'Read this view, not beach_policy_consensus, for arena-aware UX.';
