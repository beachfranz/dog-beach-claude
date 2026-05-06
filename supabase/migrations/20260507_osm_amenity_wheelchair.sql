-- 20260507_osm_amenity_wheelchair.sql
--
-- Pin #11b: extend _emit_evidence_from_osm_amenities to also derive
-- has_disabled_access from the `wheelchair` tag on osm_amenities rows
-- within 150m of the beach.
--
-- Signal: 9,070 wheelchair-tagged amenity rows (8,117 yes / 465 limited
-- / 372 no / 114 designated). 37 beaches have a wheelchair-positive
-- amenity within 150m.
--
-- Derivation:
--   any nearby wheelchair=yes/designated  → has_disabled_access=true
--   any nearby wheelchair=no AND no positive → has_disabled_access=false
--   else null
--
-- Same emitter source (osm_amenities_v1), incremental claim_key. Keeps
-- the BEP row count flat — one row per beach, more keys in claimed_values.

begin;

create or replace function public._emit_evidence_from_osm_amenities(
  p_fid bigint default null
) returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with hits as (
    select bg.fid as gold_fid,
           bool_or(o.amenity_type = 'parking')        as has_parking,
           bool_or(o.amenity_type = 'toilets')        as has_toilets,
           bool_or(o.amenity_type = 'shower')         as has_shower,
           bool_or(o.amenity_type = 'picnic_area')    as has_picnic,
           bool_or(o.amenity_type = 'drinking_water') as has_drinking_water,
           bool_or(o.amenity_type = 'food')           as has_food,
           bool_or(o.amenity_type = 'lifeguard')      as has_lifeguard,
           bool_or(o.amenity_type = 'fire_pit')       as has_fire_pit,
           -- Wheelchair signal (NEW): derive has_disabled_access from
           -- wheelchair tags on nearby amenities.
           bool_or(o.raw_tags->>'wheelchair' in ('yes','designated')) as has_wc_positive,
           bool_or(o.raw_tags->>'wheelchair' = 'no')                  as has_wc_negative
      from public.beaches_gold bg
      join public.osm_amenities o on ST_DWithin(bg.geom, o.geom, 0.0015)
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
        'has_fire_pits',      case when has_fire_pit       then 'true' else null end,
        -- has_disabled_access: positive overrides negative
        'has_disabled_access',
          case when has_wc_positive then 'true'
               when has_wc_negative then 'false'
               else null end
      )) as cv
    from hits
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, cpad_role, updated_at)
    select gold_fid, 'practical', 'osm_amenities_v1',
           'https://www.openstreetmap.org/', cv, 0.65,
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

commit;
