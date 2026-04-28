-- Parallel view to beach_locations: CCC access infrastructure that
-- is NOT a beach itself. Stairways, paths, vista points, harbors,
-- piers, lighthouses, campgrounds, parks, etc. — the "how do you
-- get to / experience the coast" layer.
--
-- Rationale: beach_locations should stay narrow (the actual beaches).
-- But CCC's access-type entries are valuable signal in their own
-- right — an access stairway with dogs_verdict='yes' tells the dog
-- app something useful that doesn't belong in a beach inventory.
--
-- Disjoint from beach_locations.ccc_only by inferred_type filter:
-- beach_locations keeps inferred_type in ('beach','named_beach');
-- this view keeps everything else.

create or replace view public.beach_access_features
with (security_invoker = true)
as
select
  'ccc/' || c.objectid::text                    as origin_key,
  'ccc'::text                                   as origin_source,
  c.name,
  coalesce(c.inferred_type, 'unknown')          as feature_type,
  c.geom,
  c.operator_id,
  c.managing_agency_source                      as operator_source,
  c.admin_inactive,
  c.dogs_verdict,
  c.dogs_verdict_confidence,
  c.description,
  c.phone,
  c.photo_1,
  c.google_maps_location,
  c.county_name_tiger                           as county
from public.ccc_access_points c
where (c.archived is null or c.archived <> 'Yes')
  and (c.admin_inactive is null or c.admin_inactive = false)
  and c.latitude is not null and c.longitude is not null
  and coalesce(c.inferred_type, 'unknown') not in ('beach', 'named_beach');

grant select on public.beach_access_features to anon, authenticated;

comment on view public.beach_access_features is
  'Parallel layer to beach_locations: CCC access infrastructure that is NOT a beach itself — stairways, paths, vista points, harbors, piers, lighthouses, campgrounds, parks, etc. Disjoint from beach_locations by inferred_type. Carries CCC-specific fields (dogs_verdict, description, phone, photo_1, google_maps_location) so consumers can join the access layer to nearby beaches by proximity. Anon-readable.';
