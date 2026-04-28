-- RPC: OSM beach polygons within an arbitrary bbox, joined to operators.
-- Used by the corridor view (Seal Beach / Huntington Beach / Newport
-- Beach) to render polygons colored by operator. Also returns a label
-- centroid (ST_PointOnSurface — guaranteed inside the polygon) so the
-- page can place one tooltip per polygon.

create or replace function public.osm_beach_polys_with_operator_in_bbox(
  p_west  float8,
  p_south float8,
  p_east  float8,
  p_north float8
)
returns table (
  osm_type               text,
  osm_id                 bigint,
  name                   text,
  feature_type           text,
  operator_id            bigint,
  operator_canonical     text,
  operator_level         text,
  operator_slug          text,
  managing_agency_source text,
  label_lat              float8,
  label_lng              float8,
  geojson                jsonb
)
language sql stable security definer
as $$
  select f.osm_type,
         f.osm_id,
         f.name,
         f.feature_type,
         f.operator_id,
         op.canonical_name,
         op.level,
         op.slug,
         f.managing_agency_source,
         st_y(st_pointonsurface(f.geom_full)),
         st_x(st_pointonsurface(f.geom_full)),
         st_asgeojson(f.geom_full)::jsonb
  from public.osm_features f
  left join public.operators op on op.id = f.operator_id
  where f.feature_type in ('beach','dog_friendly_beach')
    and f.geom_full is not null
    and (f.admin_inactive is null or f.admin_inactive = false)
    and st_intersects(
          f.geom_full,
          st_makeenvelope(p_west, p_south, p_east, p_north, 4326)
        );
$$;

grant execute on function public.osm_beach_polys_with_operator_in_bbox(
  float8, float8, float8, float8
) to anon, authenticated;
