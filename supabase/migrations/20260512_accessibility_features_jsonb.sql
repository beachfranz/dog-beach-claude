-- 20260512_accessibility_features_jsonb.sql
--
-- Phase B of the ADA scope (project_ada_accessibility_scope.md).
-- Adds the multi-dimensional accessibility_features JSONB column on
-- beach_amenities + a new `accessibility` BEP field_group with:
--   - osm-derived populator (this slice)
--   - park_url LLM extractor (slice 2, separate migration)
--   - manual_curator override path (built-in via priority 1)
--
-- Shape on beach_amenities.accessibility_features:
--   {
--     "accessible_parking":   true | false | null,
--     "accessible_restrooms": true | false | null,
--     "path_to_sand":         "boardwalk" | "ramp" | "mat" | "stairs_only" | "none" | null,
--     "beach_mat":            true | false | null,
--     "wheelchair_rental":    { "available": bool, "provider": str, "url": str } | null,
--     "accessible_viewing":   true | false | null,
--     "notes":                "free-text" | null,
--     "_source":              "osm_amenities_v2" | "park_url" | "manual_curator" | null,
--     "_updated_at":          ISO timestamp string
--   }
--
-- Source priority within accessibility field_group:
--   manual_curator: 1   (curator override — wins everything)
--   park_url:       2   (operator's own ADA page — second-most authoritative)
--   osm_amenities_v2: 5 (crowd-derived from wheelchair tags)
--   else: 99

begin;

alter table public.beach_amenities
  add column if not exists accessibility_features        jsonb,
  add column if not exists accessibility_features_updated_at timestamptz;

comment on column public.beach_amenities.accessibility_features is
  'Multi-dimensional accessibility signal. Keys: accessible_parking, '
  'accessible_restrooms, path_to_sand (boardwalk|ramp|mat|stairs_only|none), '
  'beach_mat, wheelchair_rental (object), accessible_viewing, notes. '
  'Per project_ada_accessibility_scope.md.';

-- Extend the priority function to include the new accessibility field_group.
create or replace function public._gold_field_group_source_priority(p_field_group text, p_source text)
returns integer
language sql
immutable
as $function$
  select case
    when p_source = 'manual' then 1
    when p_source = 'manual_curator' then 1
    when p_field_group = 'dogs' then case p_source
      when 'llm'              then 2
      when 'research'         then 3
      when 'park_url'         then 4
      when 'city_policy'      then 4
      when 'county_policy'    then 4
      when 'unified_v1'       then 5
      when 'json_explode'     then 5
      when 'old_school_llm'   then 5
      when 'park_operators'   then 6
      when 'governing_body'   then 7
      when 'cpad'             then 8
      when 'jurisdictions'    then 9
      when 'counties'         then 10
      when 'nps_places'       then 11
      when 'military_bases'   then 12
      when 'tribal_lands'     then 12
      else                          99
    end
    when p_field_group = 'practical' then case p_source
      when 'llm'              then 2
      when 'park_url'         then 3
      when 'park_operators'   then 4
      when 'unified_v1'       then 5
      when 'old_school_llm'   then 5
      when 'json_explode'     then 5
      when 'ccc'              then 6
      when 'research'         then 7
      else                          99
    end
    when p_field_group = 'governance' then case p_source
      when 'park_url'                          then 2
      when 'park_operators'                    then 2
      when 'park_url_buffer_attribution'       then 3
      when 'csp_parks'                         then 4
      when 'nps_places'                        then 4
      when 'cpad'                              then 5
      when 'tiger_places'                      then 6
      when 'governing_body'                    then 7
      when 'name'                              then 8
      when 'tribal_lands'                      then 9
      when 'military_bases'                    then 9
      when 'polygon_containment_governance_v1' then 10
      else                                          99
    end
    when p_field_group = 'access' then case p_source
      when 'plz'           then 2
      when 'cpad'          then 3
      when 'json_explode'  then 4
      when 'csp_parks'     then 5
      when 'military_bases' then 6
      else                       99
    end
    when p_field_group = 'polygon_containment' then case p_source
      when 'cpad'           then 2
      when 'jurisdictions'  then 3
      when 'counties'       then 4
      when 'military_bases' then 5
      when 'tribal_lands'   then 5
      else                       99
    end
    when p_field_group = 'accessibility' then case p_source
      when 'park_url'        then 2
      when 'park_operators'  then 3
      when 'osm_amenities_v2' then 5
      else                         99
    end
    else 99
  end;
$function$;

-- Populator: derives accessibility BEP rows from OSM amenity-type-specific wheelchair tags.
create or replace function public.populate_accessibility_from_osm_amenities(p_fid bigint default null)
returns table(rows_emitted bigint)
language plpgsql
security definer
set search_path to 'public', 'pg_catalog'
as $function$
declare v_inserted int := 0;
begin
  with hits as (
    select bg.fid as gold_fid,
           bool_or(o.amenity_type = 'parking' and o.raw_tags->>'wheelchair' in ('yes','designated'))
             as accessible_parking_yes,
           bool_or(o.amenity_type = 'parking' and o.raw_tags->>'wheelchair' = 'no')
             as accessible_parking_no,
           bool_or(o.amenity_type = 'toilets' and o.raw_tags->>'wheelchair' in ('yes','designated'))
             as accessible_restrooms_yes,
           bool_or(o.amenity_type = 'toilets' and o.raw_tags->>'wheelchair' = 'no')
             as accessible_restrooms_no
      from public.beaches_gold bg
      join public.osm_amenities o on ST_DWithin(bg.geom, o.geom, 0.0015)
     where bg.is_active and (p_fid is null or bg.fid = p_fid)
     group by bg.fid
  ),
  payload as (
    select gold_fid,
      jsonb_strip_nulls(jsonb_build_object(
        'accessible_parking',
          case when accessible_parking_yes then to_jsonb(true)
               when accessible_parking_no  then to_jsonb(false)
               else null end,
        'accessible_restrooms',
          case when accessible_restrooms_yes then to_jsonb(true)
               when accessible_restrooms_no  then to_jsonb(false)
               else null end
      )) as cv
    from hits
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, source_url, claimed_values, confidence,
       is_canonical, extraction_type, updated_at)
    select gold_fid, 'accessibility', 'osm_amenities_v2',
           'https://www.openstreetmap.org/', cv, 0.65,
           false, 'derived_url_crawl', now()
    from payload where cv <> '{}'::jsonb
    on conflict (gold_fid, field_group, source) do update
      set claimed_values = excluded.claimed_values, updated_at = now()
    where beach_enrichment_provenance.claimed_values is distinct from excluded.claimed_values
    returning 1
  )
  select count(*) into v_inserted from upserted;
  return query select v_inserted::bigint;
end $function$;

-- Promoter: reads canonical BEP for accessibility, writes to beach_amenities.accessibility_features.
create or replace function public.promote_canonical_accessibility_to_beach_amenities(p_fid bigint default null)
returns table(rows_inserted bigint, rows_updated bigint)
language plpgsql
as $function$
declare ins int := 0; upd int := 0;
begin
  with canon as (
    select e.gold_fid, e.source, e.claimed_values, e.confidence, e.updated_at
      from public.beach_enrichment_provenance e
     where e.field_group='accessibility' and e.is_canonical=true
       and (p_fid is null or e.gold_fid = p_fid)
  ),
  enriched as (
    select c.gold_fid,
      c.claimed_values
        || jsonb_build_object('_source', c.source,
                              '_updated_at', c.updated_at::text) as features,
      c.confidence
      from canon c
  ),
  upserted as (
    insert into public.beach_amenities
      (arena_group_id, accessibility_features, accessibility_features_updated_at,
       source, curated_at)
    select e.gold_fid, e.features, now(),
           coalesce((select source from public.beach_amenities ba
                      where ba.arena_group_id=e.gold_fid),
                    'auto_promoted_from_consensus'),
           now()
      from enriched e
    on conflict (arena_group_id) do update
      set accessibility_features = excluded.accessibility_features,
          accessibility_features_updated_at = now()
    where beach_amenities.source is distinct from 'manual_curator'
      and beach_amenities.accessibility_features is distinct from excluded.accessibility_features
    returning (xmax = 0) as inserted
  )
  select count(*) filter (where inserted), count(*) filter (where not inserted)
    into ins, upd from upserted;
  return query select ins::bigint, upd::bigint;
end $function$;

grant execute on function public.populate_accessibility_from_osm_amenities(bigint) to service_role;
grant execute on function public.promote_canonical_accessibility_to_beach_amenities(bigint) to service_role;

commit;
