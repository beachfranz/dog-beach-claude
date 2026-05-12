-- 20260512_get_landscape_features_rpcs.sql
--
-- Two RPCs that serve the data generate_beach_descriptions.py was
-- previously fetching live from Overpass. Now that load_state.py
-- ingests landscape features + dog-friendly POIs into osm_landing,
-- description gen can query them locally (no more 504s mid-run).
--
-- Pre-baked caps:
--   physical_features: top 5 by distance, within 300m
--   dog_friendly_pois: top 3 by distance, within 3km, with self-name
--     exclusion (the beach's own dog_park entry doesn't count as a POI)
--
-- Tag classes returned by get_physical_features_for_beach:
--   natural=cliff|reef|peninsula|cape|bay|arch|stack|sand_dunes|dune|cave_entrance|wetland
--   man_made=pier|jetty|breakwater|groyne|lighthouse|bridge
--   waterway=stream|river (named)
--   leisure=marina|swimming_area|fishing
--   tourism=viewpoint (named)
--   historic=lighthouse
--   highway=cycleway (named)
--
-- Tag classes returned by get_dog_friendly_pois_for_beach:
--   amenity=cafe|restaurant|pub|fast_food (with dog=yes)
--   leisure=dog_park (named) — excluding self-name match

begin;

drop function if exists public.get_physical_features_for_beach(bigint, integer, integer);
create or replace function public.get_physical_features_for_beach(
  p_fid bigint,
  p_radius_m integer default 300,
  p_limit    integer default 5
)
returns table(
  osm_type   text,
  osm_id     bigint,
  kind       text,
  name       text,
  distance_m integer
)
language sql
stable
security definer
set search_path to 'public', 'pg_catalog'
as $$
  select o.type as osm_type,
         o.id   as osm_id,
         coalesce(
           o.tags->>'natural',
           o.tags->>'man_made',
           o.tags->>'waterway',
           o.tags->>'leisure',
           o.tags->>'tourism',
           o.tags->>'historic',
           o.tags->>'highway'
         ) as kind,
         o.tags->>'name' as name,
         st_distance(o.geom_full::geography, g.geom::geography)::integer as distance_m
    from public.beaches_gold g
    join public.osm_landing o on
         o.is_active
     and o.geom_full is not null
     and st_dwithin(o.geom_full::geography, g.geom::geography, p_radius_m)
     and (
       o.tags->>'natural' in
         ('cliff','reef','peninsula','cape','bay','arch','stack',
          'sand_dunes','dune','cave_entrance','wetland')
       or o.tags->>'man_made' in
         ('pier','jetty','breakwater','groyne','lighthouse','bridge')
       or (o.tags->>'waterway' in ('stream','river')
           and (o.tags->>'name') is not null)
       or o.tags->>'leisure' in ('marina','swimming_area','fishing')
       or (o.tags->>'tourism' = 'viewpoint' and (o.tags->>'name') is not null)
       or o.tags->>'historic' = 'lighthouse'
       or (o.tags->>'highway' = 'cycleway' and (o.tags->>'name') is not null)
     )
   where g.fid = p_fid
   order by st_distance(o.geom_full, g.geom)
   limit p_limit;
$$;

drop function if exists public.get_dog_friendly_pois_for_beach(bigint, integer, integer);
create or replace function public.get_dog_friendly_pois_for_beach(
  p_fid bigint,
  p_radius_m integer default 3000,
  p_limit    integer default 3
)
returns table(
  osm_type   text,
  osm_id     bigint,
  kind       text,
  name       text,
  distance_m integer
)
language sql
stable
security definer
set search_path to 'public', 'pg_catalog'
as $$
  with beach as (
    select fid, geom, coalesce(display_name_override, name) as bname
      from public.beaches_gold where fid = p_fid
  )
  select o.type as osm_type,
         o.id   as osm_id,
         coalesce(o.tags->>'amenity', o.tags->>'leisure') as kind,
         o.tags->>'name' as name,
         st_distance(o.geom_full::geography, b.geom::geography)::integer as distance_m
    from beach b
    join public.osm_landing o on
         o.is_active
     and o.geom_full is not null
     and (o.tags->>'name') is not null
     and st_dwithin(o.geom_full::geography, b.geom::geography, p_radius_m)
     and (
       (o.tags->>'amenity' in ('cafe','restaurant','pub','fast_food')
        and o.tags->>'dog' = 'yes')
       or o.tags->>'leisure' = 'dog_park'
     )
     -- Exclude the beach's own dog_park entry (OSM sometimes tags
     -- the same area as leisure=dog_park; we don't want to recommend
     -- the beach to itself)
     and lower(o.tags->>'name') not like '%' || lower(replace(b.bname, ' Dog Beach', '')) || '%'
     -- Exclude one-off events that get tagged as dog_park
     and not (lower(o.tags->>'name') ~ 'fest|festival|championship|\m20\d{2}\M')
   order by st_distance(o.geom_full, b.geom)
   limit p_limit;
$$;

grant execute on function public.get_physical_features_for_beach(bigint, integer, integer)
  to anon, authenticated, service_role;
grant execute on function public.get_dog_friendly_pois_for_beach(bigint, integer, integer)
  to anon, authenticated, service_role;

commit;
