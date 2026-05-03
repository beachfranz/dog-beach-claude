-- Add geom_full to public.osm_landing.
--
-- The Overpass `out geom` output stores way coordinates in a
-- `geometry` array and relation outer rings in `members[].geometry`.
-- Parsing that into a PostGIS Polygon/MultiPolygon is straightforward
-- in Python (way_to_wkt / relation_to_wkt in fetch_osm_beach_polygons_ca.py).
-- Cleanest path: compute the polygon at fetch time, persist it on
-- landing as a PostGIS column, and have promote just copy it over.
--
-- The raw `geometry` jsonb stays as the canonical Overpass record.
-- geom_full is a derived column; keeping both lets us recompute later
-- without re-fetching.

alter table public.osm_landing
  add column if not exists geom_full geometry(Geometry, 4326);

-- Backfill geom_full on landing rows that already have a geometry
-- jsonb shape we can interpret. The backfill below handles ways
-- (LineString / Polygon coords as a list of {lat,lon}); relations
-- with multipolygon members are left null and re-fetched by future
-- pipeline runs.
--
-- Skip: backfill at this layer is impractical because the existing
-- backfill row used st_asgeojson(osm_features.geom_full) which is
-- already a PostGIS-shaped GeoJSON, not the Overpass {lat,lon}
-- list shape. For now, just leave landing.geom_full null on
-- backfill rows; future fetcher runs through the new path will
-- populate it.


-- Update promote: read landing.geom_full directly when present.
create or replace function public.promote_osm_features_from_landing()
returns jsonb
language plpgsql
security definer
as $function$
declare
  v_inserted int := 0;
  v_updated  int := 0;
begin
  with latest as (
    select distinct on (type, id)
           type, id, fetched_at, lat, lon, geometry, geom_full, tags
      from public.osm_landing
     order by type, id, fetched_at desc
  ),
  shaped as (
    select
      type as osm_type,
      id   as osm_id,
      tags->>'name' as name,
      tags,
      case when type = 'node' and lat is not null and lon is not null
           then ST_SetSRID(ST_MakePoint(lon, lat), 4326)
      end as geom_node,
      geom_full,
      fetched_at,
      case
        when (tags->>'natural') = 'beach'    and (tags->>'dog') = 'yes'      then 'dog_friendly_beach'
        when (tags->>'natural') = 'beach'                                    then 'beach'
        when (tags->>'leisure') = 'dog_park'                                 then 'dog_park'
        when (tags->>'leisure') = 'park'     and (tags->>'dog') = 'yes'      then 'dog_friendly_park'
        when (tags->>'leisure') = 'park'                                     then 'park'
        when (tags->>'natural') in ('coastline','sand','wood','scrub','grass','grassland','shoreline','meadow','tree_row')
                                                                              then tags->>'natural'
        when (tags->>'leisure') = 'nature_reserve'                           then 'nature_reserve'
        else 'unknown'
      end as feature_type
    from latest
  ),
  upserted as (
    insert into public.osm_features
           (osm_type, osm_id, name, tags, geom, geom_full, feature_type, loaded_at)
    select osm_type, osm_id, name, tags, geom_node, geom_full, feature_type, fetched_at
      from shaped
    on conflict (osm_type, osm_id) do update set
      name         = excluded.name,
      tags         = excluded.tags,
      geom         = coalesce(excluded.geom, public.osm_features.geom),
      geom_full    = coalesce(excluded.geom_full, public.osm_features.geom_full),
      feature_type = excluded.feature_type,
      loaded_at    = excluded.loaded_at
    returning xmax = 0 as is_insert
  )
  select
    count(*) filter (where is_insert),
    count(*) filter (where not is_insert)
   into v_inserted, v_updated
   from upserted;

  return jsonb_build_object('inserted', v_inserted, 'updated', v_updated);
end;
$function$;
