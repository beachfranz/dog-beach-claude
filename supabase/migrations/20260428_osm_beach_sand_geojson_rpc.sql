-- Returns OSM beach + sand polygons for a given county as simplified
-- GeoJSON. Powers admin/oc-beach-sand-polygons.html.

create or replace function public.osm_beach_sand_geojson(
  p_county text default 'Orange'
) returns table (
  osm_type        text,
  osm_id          bigint,
  name            text,
  name_source     text,   -- 'osm' | 'borrowed_from_beach' | null
  feature_type    text,
  operator_id     bigint,
  operator_name   text,
  geom_json       jsonb
) language sql stable security definer as $$
  select o.osm_type, o.osm_id,
         coalesce(nullif(o.name, ''), borrowed.name) as name,
         case
           when o.name is not null and o.name <> '' then 'osm'
           when borrowed.name is not null            then 'borrowed_from_beach'
           else null
         end as name_source,
         o.feature_type,
         o.operator_id, op.canonical_name,
         st_asgeojson(st_simplifypreservetopology(o.geom_full, 0.0001))::jsonb
    from public.osm_features o
    left join public.operators op on op.id = o.operator_id
    -- For unnamed polygons, borrow a named beach polygon's name when:
    --   1. It overlaps or touches (st_intersects); OR
    --   2. It's within ~200m
    -- Prefer overlap/containment over distance. Same-county only.
    left join lateral (
      select b.name,
             case when st_intersects(b.geom_full, o.geom_full) then 0 else 1 end as rank,
             st_distance(b.geom, o.geom) as dist
        from public.osm_features b
       where b.feature_type in ('beach','dog_friendly_beach')
         and b.name is not null and b.name <> ''
         and b.geom_full is not null
         and b.county_name_tiger = o.county_name_tiger
         and (b.osm_type, b.osm_id) <> (o.osm_type, o.osm_id)
         and (
           st_intersects(b.geom_full, o.geom_full)
           or st_dwithin(b.geom, o.geom, 0.002)   -- ~200m bbox
         )
       order by rank asc, dist asc
       limit 1
    ) borrowed on (o.name is null or o.name = '')
   where o.feature_type in ('beach','sand','dog_friendly_beach')
     and o.geom_full is not null
     and o.county_name_tiger = p_county
     and (o.admin_inactive is null or o.admin_inactive = false);
$$;

grant execute on function public.osm_beach_sand_geojson(text) to anon, authenticated;
