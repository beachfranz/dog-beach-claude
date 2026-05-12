-- 20260512_emit_wheelchair_from_osm_landing.sql
--
-- Phase A of the ADA scope (project_ada_accessibility_scope.md).
-- `_emit_evidence_from_osm_amenities` already derives has_disabled_access
-- from wheelchair tags on osm_amenities — but ONLY on osm_amenities.
-- osm_landing has 119 wheelchair=yes + 39 wheelchair=limited features
-- (named beach polygons, dog parks, etc.) that we ignore today.
--
-- Per state coverage was 80 BEP rows for has_disabled_access on
-- 972-null beach_amenities. Adding osm_landing coverage closes a real
-- chunk of that gap.
--
-- Phase A scope: just expand wheelchair-source set. The per-feature
-- split (accessible_parking vs accessible_restrooms) ships in Phase B
-- when accessibility_features JSONB lands.

begin;

create or replace function public._emit_evidence_from_osm_amenities(p_fid bigint default null)
returns table(rows_inserted bigint, rows_updated bigint, rows_skipped bigint)
language plpgsql
as $function$
declare ins int := 0; upd int := 0; skp int := 0;
begin
  with hits_amenities as (
    select bg.fid as gold_fid,
           bool_or(o.amenity_type = 'parking')        as has_parking,
           bool_or(o.amenity_type = 'toilets')        as has_toilets,
           bool_or(o.amenity_type = 'shower')         as has_shower,
           bool_or(o.amenity_type = 'picnic_area')    as has_picnic,
           bool_or(o.amenity_type = 'drinking_water') as has_drinking_water,
           bool_or(o.amenity_type = 'food')           as has_food,
           bool_or(o.amenity_type = 'lifeguard')      as has_lifeguard,
           bool_or(o.amenity_type = 'fire_pit')       as has_fire_pit,
           bool_or(o.raw_tags->>'wheelchair' in ('yes','designated')) as has_wc_positive,
           bool_or(o.raw_tags->>'wheelchair' = 'no')                  as has_wc_negative
      from public.beaches_gold bg
      join public.osm_amenities o on ST_DWithin(bg.geom, o.geom, 0.0015)
     where bg.is_active and (p_fid is null or bg.fid = p_fid)
     group by bg.fid
  ),
  -- NEW: osm_landing wheelchair signal. Beach polygons, dog parks,
  -- and other landscape features sometimes carry wheelchair tags that
  -- describe the feature's overall accessibility (vs the amenity-level
  -- signal in osm_amenities). 200m radius keeps it tight to the beach.
  hits_landing as (
    select bg.fid as gold_fid,
           bool_or(o.tags->>'wheelchair' in ('yes','designated','limited'))
             filter (where o.tags->>'wheelchair' in ('yes','designated','limited')) as wc_positive,
           bool_or(o.tags->>'wheelchair' = 'no')
             filter (where o.tags->>'wheelchair' = 'no')                            as wc_negative
      from public.beaches_gold bg
      join public.osm_landing o
        on o.is_active and o.geom_full is not null and (o.tags ? 'wheelchair')
       and ST_DWithin(bg.geom::geography, o.geom_full::geography, 200)
     where bg.is_active and (p_fid is null or bg.fid = p_fid)
     group by bg.fid
  ),
  combined as (
    select coalesce(a.gold_fid, l.gold_fid)                     as gold_fid,
           a.has_parking, a.has_toilets, a.has_shower, a.has_picnic,
           a.has_drinking_water, a.has_food, a.has_lifeguard, a.has_fire_pit,
           coalesce(a.has_wc_positive, false) or coalesce(l.wc_positive, false) as wc_positive,
           coalesce(a.has_wc_negative, false) or coalesce(l.wc_negative, false) as wc_negative
      from hits_amenities a
      full outer join hits_landing l on a.gold_fid = l.gold_fid
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
        'has_disabled_access',
          case when wc_positive then 'true'
               when wc_negative then 'false'
               else null end
      )) as cv
    from combined
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
end $function$;

commit;
