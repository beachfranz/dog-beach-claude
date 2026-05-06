-- 20260507_practical_emitters.sql
--
-- Two new emitters for the practical field_group, following the BEP
-- emitter template established for dogs (cpad/pad_us/operator/zone_rules/
-- off_leash). Both write votes for has_parking, has_restrooms, has_showers,
-- has_picnic_area, has_drinking_water, has_food, has_lifeguards, has_fire_pits.
--
-- 1) zone_rules_practical_v1
--    Section PRESENCE in zone_rules implies the amenity exists. One-sided
--    positive signal — section absence does NOT imply amenity absence.
--    Source confidence 0.70 (per-unit-equivalent strong positive).
--
-- 2) osm_amenities_v1
--    Spatial intersect with osm_amenities (within 150m of the beach geom)
--    implies the amenity exists. Also one-sided positive — OSM coverage
--    is uneven; absence isn't reliable. Source confidence 0.65 (community-
--    contributed data, occasional mistagging).
--
-- Both run alongside existing old_school_llm + park_url + unified_v1
-- voters. Net effect: more redundant positive evidence per amenity →
-- more robust consensus on the practical field_group. Today's coverage
-- (30-57% per field) should rise toward 70-85% after both fan-out.

begin;

-- Allow new source values
alter table public.beach_enrichment_provenance
  drop constraint beach_enrichment_provenance_source_check;
alter table public.beach_enrichment_provenance
  add constraint beach_enrichment_provenance_source_check
  check (source = any (array[
    'manual','manual_curator','plz','cpad','tiger_places','ccc','llm',
    'web_scrape','research','csp_parks','park_operators','nps_places',
    'tribal_lands','military_bases','pad_us','sma_code_mappings',
    'jurisdictions','csp_places','name','governing_body','park_url',
    'park_url_buffer_attribution','park_url_governance','old_school_llm',
    'counties','json_explode','unified_v1','city_policy','county_policy',
    'text_repass_v1','cpad_unit_dogs_policy_v1','pad_us_dogs_policy_v1',
    'operator_dogs_policy_v1','zone_rules_derived_v1',
    'zone_rules_practical_v1','osm_amenities_v1'
  ]));

-- ----------------------------------------------------------------------
-- Emitter 1: zone_rules_practical_v1
-- ----------------------------------------------------------------------
create or replace function public._emit_evidence_from_zone_rules_practical(
  p_fid bigint default null
) returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with parent as (
    -- highest-confidence BEP row carrying zone_rules per beach
    select distinct on (e.gold_fid)
      e.gold_fid, e.confidence as parent_conf, e.source as parent_source,
      e.source_url as parent_url, e.claimed_values->'zone_rules' as zr
    from public.beach_enrichment_provenance e
    where e.field_group = 'dogs'
      and e.claimed_values ? 'zone_rules'
      and e.claimed_values->'zone_rules' is not null
      and (p_fid is null or e.gold_fid = p_fid)
    order by e.gold_fid, e.confidence desc nulls last
  ),
  sections_v2 as (
    select p.gold_fid, p.parent_conf, p.parent_source, p.parent_url,
           section.key as section_name
      from parent p,
           jsonb_array_elements(p.zr->'seasons') s,
           jsonb_array_elements(s.value->'regions') r,
           jsonb_each(r.value->'sections') section
     where p.zr ? 'seasons'
  ),
  sections_v1 as (
    select p.gold_fid, p.parent_conf, p.parent_source, p.parent_url,
           section.key
      from parent p,
           jsonb_array_elements(p.zr->'regions') r,
           jsonb_each(r.value->'sections') section
     where not (p.zr ? 'seasons') and (p.zr ? 'regions')
  ),
  sections as (select * from sections_v2 union all select * from sections_v1),
  per_beach as (
    select
      gold_fid,
      max(parent_conf) as parent_conf,
      max(parent_source) as parent_source,
      max(parent_url) as parent_url,
      bool_or(section_name = 'parking_lot')        as has_parking,
      bool_or(section_name = 'restrooms_showers')  as has_restrooms_showers_section,
      bool_or(section_name = 'picnic_area')        as has_picnic
    from sections group by gold_fid
  ),
  payload as (
    select
      gold_fid, parent_conf, parent_source, parent_url,
      jsonb_strip_nulls(jsonb_build_object(
        -- One-sided positive: only emit 'true' when present, omit otherwise
        'has_parking',     case when has_parking then 'true' else null end,
        'has_restrooms',   case when has_restrooms_showers_section then 'true' else null end,
        'has_showers',     case when has_restrooms_showers_section then 'true' else null end,
        'has_picnic_area', case when has_picnic then 'true' else null end,
        'derivation_parent', parent_source
      )) as cv
    from per_beach
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select
      p.gold_fid, 'practical', 'zone_rules_practical_v1', p.parent_url,
      p.cv, p.parent_conf, false, 'derived_url_crawl', 'beach_access', now()
    from payload p
    where (p.cv - 'derivation_parent') <> '{}'::jsonb   -- emit only when at least one positive vote
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values,
          confidence     = excluded.confidence,
          source_url     = excluded.source_url,
          updated_at     = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
       or beach_enrichment_provenance.confidence is distinct from excluded.confidence
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upserted;
  return query select ins::bigint, upd::bigint, skp::bigint;
end $$;
grant execute on function public._emit_evidence_from_zone_rules_practical(bigint)
  to authenticated, service_role;

-- ----------------------------------------------------------------------
-- Emitter 2: osm_amenities_v1
-- ----------------------------------------------------------------------
-- Spatial intersect: any osm_amenities row of canonical type X within
-- 150m of beach geom → vote 'has_X = true'.
--
-- 150m chosen to cover beach-adjacent parking lots / restroom buildings
-- that are "at" the beach but not inside its footprint. Tighter than
-- the 200m typical for CCC access-point matching.
create or replace function public._emit_evidence_from_osm_amenities(
  p_fid bigint default null
) returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  -- Index-friendly: ST_DWithin(geom, geom, ~deg) hits the GIST index.
  -- 0.0015 deg ≈ 130-165m at CA latitude (32-42°N). Acceptable approximation
  -- for "is this amenity at this beach?" — beach polygons span tens of meters
  -- so the latitude/longitude asymmetry is negligible vs the polygon scale.
  with hits as (
    select bg.fid as gold_fid,
           bool_or(o.amenity_type = 'parking')        as has_parking,
           bool_or(o.amenity_type = 'toilets')        as has_toilets,
           bool_or(o.amenity_type = 'shower')         as has_shower,
           bool_or(o.amenity_type = 'picnic_area')    as has_picnic,
           bool_or(o.amenity_type = 'drinking_water') as has_drinking_water,
           bool_or(o.amenity_type = 'food')           as has_food,
           bool_or(o.amenity_type = 'lifeguard')      as has_lifeguard,
           bool_or(o.amenity_type = 'fire_pit')       as has_fire_pit
      from public.beaches_gold bg
      join public.osm_amenities o
        on ST_DWithin(bg.geom, o.geom, 0.0015)
     where bg.is_active
       and (p_fid is null or bg.fid = p_fid)
     group by bg.fid
  ),
  payload as (
    select gold_fid,
      jsonb_strip_nulls(jsonb_build_object(
        'has_parking',        case when has_parking        then 'true' else null end,
        'has_restrooms',      case when has_toilets        then 'true' else null end,
        'has_showers',        case when has_shower         then 'true' else null end,
        'has_picnic_area',    case when has_picnic         then 'true' else null end,
        'has_drinking_water', case when has_drinking_water then 'true' else null end,
        'has_food',           case when has_food           then 'true' else null end,
        'has_lifeguards',     case when has_lifeguard      then 'true' else null end,
        'has_fire_pits',      case when has_fire_pit       then 'true' else null end
      )) as cv
    from hits
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'practical', 'osm_amenities_v1',
           'https://www.openstreetmap.org/', cv,
           0.65,        -- OSM community-contributed; positive signal but not authoritative
           false, 'derived_url_crawl', 'beach_access', now()
    from payload where cv <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values, updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted), 0
    into ins, upd, skp from upserted;
  return query select ins::bigint, upd::bigint, skp::bigint;
end $$;
grant execute on function public._emit_evidence_from_osm_amenities(bigint)
  to authenticated, service_role;

commit;
