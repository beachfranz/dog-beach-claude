-- County-scoped variant of osm_beach_polys_with_operator_in_bbox.
-- Same shape; spatial filter is the actual county polygon (not a bbox)
-- so beaches just outside a rectangle but inside the county boundary
-- are caught.

drop function if exists public.osm_beach_polys_with_operator_in_county(text);

create or replace function public.osm_beach_polys_with_operator_in_county(
  p_geoid text
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
  admin_inactive         boolean,
  label_lat              float8,
  label_lng              float8,
  geojson                jsonb
)
language sql stable security definer
as $$
  select f.osm_type, f.osm_id, f.name, f.feature_type,
         f.operator_id, op.canonical_name, op.level, op.slug,
         f.managing_agency_source,
         coalesce(f.admin_inactive, false),
         st_y(st_pointonsurface(f.geom_full)),
         st_x(st_pointonsurface(f.geom_full)),
         st_asgeojson(f.geom_full)::jsonb
  from public.osm_features f
  left join public.operators op on op.id = f.operator_id
  cross join (select geom from public.counties where geoid = p_geoid) c
  where f.feature_type in ('beach','dog_friendly_beach')
    and f.geom_full is not null
    and st_intersects(f.geom_full, c.geom);
$$;

grant execute on function public.osm_beach_polys_with_operator_in_county(text)
  to anon, authenticated;
