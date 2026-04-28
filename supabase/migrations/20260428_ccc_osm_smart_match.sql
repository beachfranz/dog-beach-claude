-- Smart CCC↔OSM match: link CCC access points to OSM beach polygons
-- via cleaned-name trigram + 5km KNN, recording the relationship in
-- feature_associations.
--
-- Why this exists: CCC's lat/lng is the access point (parking lot,
-- trailhead) — typically 500m-3km from the beach polygon's centroid.
-- The standard cascade missed Bean Hollow / Carmel River SB / Emma
-- Wood SB / etc. because the strict point-in-polygon and 500m
-- proximity rules can't bridge the access-point-to-beach gap.
--
-- Cleaning strips parentheticals + common suffixes ("State Beach",
-- "Park", "Access", "Trail", "Campground", "North"/"South" qualifiers)
-- before trigram, so verbose CCC names match terse OSM names.
--
-- 253 expected rows statewide on first run.

create or replace function public.clean_beach_name(n text) returns text
language sql immutable as $$
  select lower(trim(regexp_replace(
    regexp_replace(
      regexp_replace(coalesce(n,''), '\s*\([^)]*\)\s*', ' ', 'g'),
      '\m(state\s+beach|state\s+park|state|beach|park|access|trail|campground|county\s+park|north|south|east|west)\M', ' ', 'gi'
    ),
    '\s+', ' ', 'g'
  )));
$$;


insert into public.feature_associations
  (a_source, a_id, b_source, b_id, relationship, note)
select 'ccc',
       m.ccc_id::text,
       'osm',
       m.osm_type || '/' || m.osm_id::text,
       'same_beach',
       format('ccc_osm_smart_match clean_sim=%s dist_m=%s',
              round(m.clean_sim::numeric, 2),
              round(m.dist_m::numeric))
from (
  select distinct on (c.objectid)
    c.objectid as ccc_id,
    o.osm_type, o.osm_id,
    similarity(public.clean_beach_name(c.name), public.clean_beach_name(o.name)) as clean_sim,
    st_distance(c.geom::geography, o.geom::geography) as dist_m
  from public.ccc_access_points c
  cross join lateral (
    select osm_type, osm_id, name, geom
    from public.osm_features
    where feature_type in ('beach','dog_friendly_beach')
      and (admin_inactive is null or admin_inactive=false)
      and name is not null and name <> ''
      and length(public.clean_beach_name(name)) > 2
      and st_dwithin(c.geom::geography, geom::geography, 5000)
    order by similarity(public.clean_beach_name(c.name), public.clean_beach_name(name)) desc nulls last,
             st_distance(c.geom::geography, geom::geography) asc
    limit 1
  ) o
  where (c.archived is null or c.archived <> 'Yes')
    and (c.admin_inactive is null or c.admin_inactive=false)
    and c.inferred_type in ('beach','named_beach')
    and c.latitude is not null
    and length(public.clean_beach_name(c.name)) > 2
) m
where m.clean_sim >= 0.7
on conflict (a_source, a_id, b_source, b_id, relationship) do nothing;
