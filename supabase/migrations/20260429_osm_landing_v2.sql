-- v2 of OSM landing: matches Overpass JSON output shape, renamed to
-- public.osm_landing. The previous attempt (osm_features_landing) had
-- columns derived from the data — wrong abstraction. Landing should
-- be the rawest sensible representation; transformation happens during
-- promote.
--
-- Overpass native JSON shape for elements:
--   { "type": "node",     "id": N, "lat": F, "lon": F, "tags": {...} }
--   { "type": "way",      "id": N, "geometry": [{lat,lon},...], "tags": {...} }
--   { "type": "relation", "id": N, "members": [...],            "tags": {...} }
--
-- We persist that shape with a fetched_at timestamp + fetched_by tag.

drop table if exists public.osm_features_landing cascade;
drop function if exists public.promote_osm_features_from_landing();

create table public.osm_landing (
  fetched_at  timestamptz not null default now(),
  fetched_by  text,                       -- which script/query produced this row
  type        text not null check (type in ('node','way','relation')),
  id          bigint not null,
  lat         double precision,           -- nodes only
  lon         double precision,           -- nodes only
  geometry    jsonb,                      -- ways/relations with `out geom`
  members     jsonb,                      -- relations
  tags        jsonb,
  primary key (type, id, fetched_at)
);
create index osm_landing_id_idx        on public.osm_landing (type, id);
create index osm_landing_fetched_at_idx on public.osm_landing (fetched_at desc);
create index osm_landing_natural_idx   on public.osm_landing ((tags->>'natural'));
create index osm_landing_leisure_idx   on public.osm_landing ((tags->>'leisure'));

comment on table public.osm_landing is
  'Raw Overpass output. Each row mirrors a single Overpass element exactly (type/id/lat/lon/geometry/tags). Every fetch creates a new row per (type, id, fetched_at). public.osm_features consumes via promote_osm_features_from_landing().';


-- Promote: take latest landing row per (type, id), derive feature_type
-- from tags, upsert into osm_features. Preserves downstream enrichment
-- (operator_id, admin_inactive, cleaning_status).

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
           type, id, fetched_at, lat, lon, geometry, tags
      from public.osm_landing
     order by type, id, fetched_at desc
  ),
  shaped as (
    select
      type as osm_type,
      id   as osm_id,
      tags->>'name' as name,
      tags,
      -- centroid: nodes use lat/lon, ways/relations would already have
      -- a representative point; for the simple v1 we just build a
      -- Point from lat/lon for nodes. Promote logic preserves whatever
      -- geom the existing osm_features row has for non-node updates.
      case when type = 'node' and lat is not null and lon is not null
           then ST_SetSRID(ST_MakePoint(lon, lat), 4326)
      end as geom_node,
      -- full polygon: for ways with `out geom`, build polygon from the
      -- coordinate array. Skipped for v1 — the existing fetcher already
      -- writes geom_full directly; future fetchers writing through
      -- landing will need this branch fleshed out.
      null::geometry as geom_full_new,
      fetched_at,
      -- feature_type derivation matches existing data conventions
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
           (osm_type, osm_id, name, tags, geom, feature_type, loaded_at)
    select osm_type, osm_id, name, tags, geom_node, feature_type, fetched_at
      from shaped
    on conflict (osm_type, osm_id) do update set
      name         = excluded.name,
      tags         = excluded.tags,
      geom         = coalesce(excluded.geom, public.osm_features.geom),
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

comment on function public.promote_osm_features_from_landing() is
  'Promotes the latest osm_landing row per (type, id) into public.osm_features. Derives feature_type from tags. Preserves downstream enrichment (operator_id, admin_inactive, cleaning_status, geom_full from polygon fetcher).';


-- Backfill landing from current osm_features.
-- For each existing row, synthesize an Overpass-shaped landing row.
-- nodes: lat/lon from geom centroid; ways/relations: geometry from geom_full.

insert into public.osm_landing (fetched_at, fetched_by, type, id, lat, lon, geometry, tags)
select
  coalesce(loaded_at, '2026-04-01'::timestamptz),
  'backfill_from_osm_features',
  osm_type,
  osm_id,
  case when osm_type = 'node' then st_y(geom) end as lat,
  case when osm_type = 'node' then st_x(geom) end as lon,
  case when osm_type in ('way','relation') and geom_full is not null
       then to_jsonb(st_asgeojson(geom_full)::json)
  end as geometry,
  tags
  from public.osm_features
on conflict (type, id, fetched_at) do nothing;
