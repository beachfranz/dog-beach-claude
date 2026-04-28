-- Fix the OSM-feature cascade to use polygon intersection instead of
-- centroid point-in-polygon for Pass 1 (CPAD) and Pass 3 (TIGER place).
--
-- Beach polygons are thin coast-hugging ribbons. Their centroids
-- frequently fall on the wet sand, OUTSIDE any CPAD polygon — even
-- when the polygon clearly overlaps a CPAD state-beach unit. Centroid
-- PIP missed Bolsa Chica State Beach, Crystal Cove, Pelican Point and
-- ~200 other obvious cases.
--
-- ST_Intersects(geom_full) catches any overlap; smallest CPAD/place
-- area still wins for most-specific attribution.
--
-- Applies only to osm_features (only table with polygon geom_full).
-- ccc_access_points / us_beach_points / locations_stage are point
-- features — centroid PIP stays correct for them.
--
-- Re-resolves only rows where operator_id is currently null. Existing
-- attributions are preserved.

-- ── Pass 1: CPAD via polygon intersection ───────────────────────────
update public.osm_features o
set operator_id = sub.op_id,
    managing_agency_source = 'cpad'
from (
  select distinct on (o2.osm_type, o2.osm_id)
         o2.osm_type, o2.osm_id, op.id as op_id
  from public.osm_features o2
  join public.cpad_units cu on st_intersects(cu.geom, o2.geom_full)
  join public.operators op on op.slug = public.slugify_agency(cu.mng_agncy)
  where o2.operator_id is null
    and o2.geom_full is not null
    and cu.mng_agncy is not null
  order by o2.osm_type, o2.osm_id, st_area(cu.geom) asc
) sub
where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;

-- ── Pass 3: TIGER place via polygon intersection ────────────────────
update public.osm_features o
set operator_id = sub.op_id,
    managing_agency_source = 'tiger_c1'
from (
  select distinct on (o2.osm_type, o2.osm_id)
         o2.osm_type, o2.osm_id, op.id as op_id
  from public.osm_features o2
  join public.jurisdictions j on st_intersects(j.geom, o2.geom_full)
  join public.operators op on op.jurisdiction_id = j.id
  where o2.operator_id is null
    and o2.geom_full is not null
    and j.state = 'CA' and j.place_type like 'C%'
  order by o2.osm_type, o2.osm_id, st_area(j.geom) asc
) sub
where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;

-- Refresh denormalized counts on operators
update public.operators op
set osm_feature_count = (
  select count(*) from public.osm_features where operator_id = op.id
);
