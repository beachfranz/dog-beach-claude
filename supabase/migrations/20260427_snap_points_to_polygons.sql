-- Phase 3: snap point geom into the closest OSM beach polygon.
--
-- Two independent snaps, each reversible via geom_original.
--
-- IMPORTANT: CCC points are NEVER snapped. CCC's published lat/lng
-- represents an *access point* (parking lot, trailhead, path entry) —
-- it's deliberately not on the beach itself. Moving it into the sand
-- corrupts the access semantic. Reverted 2026-04-27 after the first
-- snap pass, and excluded from this migration going forward.
--
-- Rule for OSM beaches (own polygons): if the centroid sits outside
-- its own polygon (which happens for crescent-shaped coves where the
-- geometric center is over water), move it to ST_PointOnSurface — a
-- point guaranteed to lie inside the polygon.
--
-- Rule for UBP: find the nearest OSM beach polygon within 500m. If the
-- point is already inside, don't move it. Otherwise snap to
-- ST_ClosestPoint of the polygon (boundary point — counts as "inside"
-- for visual purposes). 500m cap keeps us from teleporting a point
-- that just happens to be near a beach.

-- ── OSM beaches: snap their own centroids inside their own polygons ──
update public.osm_features t
   set geom_original = t.geom,
       geom          = st_pointonsurface(t.geom_full),
       latitude      = st_y(st_pointonsurface(t.geom_full)),
       longitude     = st_x(st_pointonsurface(t.geom_full))
 where t.feature_type in ('beach','dog_friendly_beach')
   and t.geom_full is not null
   and t.geom_original is null
   and not st_contains(t.geom_full, t.geom);

-- (CCC snap intentionally removed — see header. CCC lat/lng is an
-- access-point coordinate, not a beach coordinate; snapping breaks the
-- semantic.)

-- ── UBP (CA only): snap to nearest OSM beach polygon within 500m ──
with sub as (
  select u.id, p.geom_full, p.geom_original_check
  from public.us_beach_points u
  cross join lateral (
    select p.geom_full,
           st_contains(p.geom_full, u.geom) as geom_original_check
    from public.osm_features p
    where p.feature_type in ('beach','dog_friendly_beach')
      and p.geom_full is not null
      and st_dwithin(u.geom::geography, p.geom_full::geography, 500)
    order by p.geom_full <-> u.geom
    limit 1
  ) p
  where u.state = 'CA' and u.geom_original is null
)
update public.us_beach_points u
   set geom_original = u.geom,
       geom          = st_closestpoint(sub.geom_full, u.geom)
  from sub
 where u.id = sub.id
   and not sub.geom_original_check;
