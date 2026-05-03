-- arena_beach_metadata — consumer-ready beach detail surface.
--
-- One row per active arena beach group (= one row per "real beach").
-- Columns:
--   arena_group_id, name, address, nav_lat, nav_lon, park_name (from arena)
--   14 policy/amenity fields (from beach_policy_extractions, shape-aware)
--
-- Shape-aware consensus:
--   enum / bool fields → majority vote across variants, junk values filtered out
--   text / structured_json fields → use is_canon=true variant's parsed_value
--                                  (text variants always disagree at wording
--                                   level; majority-voting prose makes no sense)
--
-- Junk filter for enum/bool consensus (case-insensitive):
--   'unclear', 'unknown', '', 'none', 'none.', 'null'
--   These mean "the page didn't say"; never count as a vote.
--
-- This replaces the older `beach_policy_consensus` view as the consumer
-- surface. beach_policy_consensus stays in place for now (legacy reads
-- via us_beach_points.fid still work).

create or replace view public.arena_beach_metadata as
with text_canonical as (
  select e.arena_group_id, v.field_name, e.parsed_value as canonical_value
    from public.beach_policy_extractions e
    join public.extraction_prompt_variants v on v.id = e.variant_id
   where v.is_canon = true
     and v.expected_shape in ('text', 'structured_json')
     and e.parse_succeeded
     and e.arena_group_id is not null
),
enum_vote_counts as (
  select e.arena_group_id, v.field_name, e.parsed_value, count(*) as votes
    from public.beach_policy_extractions e
    join public.extraction_prompt_variants v on v.id = e.variant_id
   where v.expected_shape in ('enum', 'bool')
     and e.parse_succeeded
     and e.arena_group_id is not null
     and lower(coalesce(e.parsed_value, '')) not in
         ('unclear', 'unknown', '', 'none', 'none.', 'null')
   group by e.arena_group_id, v.field_name, e.parsed_value
),
enum_canonical as (
  select distinct on (arena_group_id, field_name)
         arena_group_id, field_name, parsed_value as canonical_value
    from enum_vote_counts
   order by arena_group_id, field_name, votes desc, parsed_value asc
),
all_canonical as (
  select * from text_canonical
  union all
  select * from enum_canonical
),
pivoted as (
  select arena_group_id,
         max(case when field_name = 'dogs_allowed'                 then canonical_value end) as dogs_allowed,
         max(case when field_name = 'dogs_leash_required'          then canonical_value end) as dogs_leash_required,
         max(case when field_name = 'dogs_allowed_areas'           then canonical_value end) as dogs_allowed_areas,
         max(case when field_name = 'dogs_off_leash_area'          then canonical_value end) as dogs_off_leash_area,
         max(case when field_name = 'dogs_policy_notes'            then canonical_value end) as dogs_policy_notes,
         max(case when field_name = 'dogs_seasonal_restrictions'   then canonical_value end) as dogs_seasonal_restrictions,
         max(case when field_name = 'dogs_time_restrictions'       then canonical_value end) as dogs_time_restrictions,
         max(case when field_name = 'access_rule'                  then canonical_value end) as access_rule,
         max(case when field_name = 'access_text'                  then canonical_value end) as access_text,
         max(case when field_name = 'has_parking'                  then canonical_value end) as has_parking,
         max(case when field_name = 'parking_type'                 then canonical_value end) as parking_type,
         max(case when field_name = 'has_drinking_water'           then canonical_value end) as has_drinking_water,
         max(case when field_name = 'hours_text'                   then canonical_value end) as hours_text,
         max(case when field_name = 'raw_address'                  then canonical_value end) as extracted_address
    from all_canonical
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
       coalesce(p.extracted_address, a.address) as best_address
  from public.arena a
  left join pivoted p on p.arena_group_id = a.fid
 where a.is_active = true
   and a.fid = a.group_id;  -- group leaders only (one row per beach)

comment on view public.arena_beach_metadata is
  'Consumer-ready beach surface. One row per active arena group (=real beach). '
  'Shape-aware consensus: enum/bool = majority vote (junk filtered), '
  'text/structured_json = is_canon variant''s parsed value. Read for detail.html.';
