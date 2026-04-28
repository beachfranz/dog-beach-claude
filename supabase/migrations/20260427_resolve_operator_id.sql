-- Resolve operator_id on every beach source row via the 3-pass cascade.
-- Same logic as the prior free-text cascade but writes operators(id) FK
-- via slug-based joins.
--
-- Pass 1 — CPAD point-in-polygon: smallest containing polygon's
--          mng_agncy → operator via slugify match.
-- Pass 2 — OSM operator tag (osm_features only): tags->>'operator' →
--          operator via slugify match.
-- Pass 3 — TIGER place fallback: smallest containing incorporated
--          place → operator via jurisdiction_id FK.
--
-- managing_agency_source records which pass won. Re-running this
-- migration is safe: each pass updates only rows that are still null.
-- Reset operator_id first if you want a clean re-resolve.

-- ── Pass 1: CPAD ────────────────────────────────────────────────────
-- ccc_access_points
update public.ccc_access_points c
set operator_id = sub.op_id,
    managing_agency_source = 'cpad'
from (
  select distinct on (c2.objectid) c2.objectid as id, op.id as op_id
  from public.ccc_access_points c2
  join public.cpad_units cu on st_contains(cu.geom, c2.geom)
  join public.operators op on op.slug = public.slugify_agency(cu.mng_agncy)
  where c2.operator_id is null
    and cu.mng_agncy is not null
  order by c2.objectid, st_area(cu.geom) asc
) sub
where c.objectid = sub.id;

-- us_beach_points
update public.us_beach_points u
set operator_id = sub.op_id,
    managing_agency_source = 'cpad'
from (
  select distinct on (u2.fid) u2.fid as id, op.id as op_id
  from public.us_beach_points u2
  join public.cpad_units cu on st_contains(cu.geom, u2.geom)
  join public.operators op on op.slug = public.slugify_agency(cu.mng_agncy)
  where u2.operator_id is null
    and cu.mng_agncy is not null
  order by u2.fid, st_area(cu.geom) asc
) sub
where u.fid = sub.id;

-- osm_features
update public.osm_features o
set operator_id = sub.op_id,
    managing_agency_source = 'cpad'
from (
  select distinct on (o2.osm_type, o2.osm_id)
         o2.osm_type, o2.osm_id, op.id as op_id
  from public.osm_features o2
  join public.cpad_units cu on st_contains(cu.geom, o2.geom)
  join public.operators op on op.slug = public.slugify_agency(cu.mng_agncy)
  where o2.operator_id is null
    and cu.mng_agncy is not null
  order by o2.osm_type, o2.osm_id, st_area(cu.geom) asc
) sub
where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;

-- locations_stage (geom is geography; source attribution belongs in
-- beach_enrichment_provenance, not on locations_stage itself).
update public.locations_stage s
set operator_id = sub.op_id
from (
  select distinct on (s2.fid) s2.fid as id, op.id as op_id
  from public.locations_stage s2
  join public.cpad_units cu on st_contains(cu.geom, s2.geom::geometry)
  join public.operators op on op.slug = public.slugify_agency(cu.mng_agncy)
  where s2.operator_id is null
    and cu.mng_agncy is not null
  order by s2.fid, st_area(cu.geom) asc
) sub
where s.fid = sub.id;


-- ── Pass 2: OSM operator tag (osm_features only) ────────────────────
update public.osm_features o
set operator_id = sub.op_id,
    managing_agency_source = 'osm_tag'
from (
  select distinct on (o2.osm_type, o2.osm_id)
         o2.osm_type, o2.osm_id, op.id as op_id
  from public.osm_features o2
  join public.operators op on op.slug = public.slugify_agency(o2.tags->>'operator')
  where o2.operator_id is null
    and o2.tags ? 'operator'
    and o2.tags->>'operator' <> ''
  order by o2.osm_type, o2.osm_id, op.id
) sub
where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;


-- ── Pass 3: TIGER place fallback ────────────────────────────────────
-- ccc_access_points
update public.ccc_access_points c
set operator_id = sub.op_id,
    managing_agency_source = 'tiger_c1'
from (
  select distinct on (c2.objectid) c2.objectid as id, op.id as op_id
  from public.ccc_access_points c2
  join public.jurisdictions j on st_contains(j.geom, c2.geom)
  join public.operators op on op.jurisdiction_id = j.id
  where c2.operator_id is null
    and j.state = 'CA' and j.place_type like 'C%'
  order by c2.objectid, st_area(j.geom) asc
) sub
where c.objectid = sub.id;

-- us_beach_points
update public.us_beach_points u
set operator_id = sub.op_id,
    managing_agency_source = 'tiger_c1'
from (
  select distinct on (u2.fid) u2.fid as id, op.id as op_id
  from public.us_beach_points u2
  join public.jurisdictions j on st_contains(j.geom, u2.geom)
  join public.operators op on op.jurisdiction_id = j.id
  where u2.operator_id is null
    and j.state = 'CA' and j.place_type like 'C%'
  order by u2.fid, st_area(j.geom) asc
) sub
where u.fid = sub.id;

-- osm_features
update public.osm_features o
set operator_id = sub.op_id,
    managing_agency_source = 'tiger_c1'
from (
  select distinct on (o2.osm_type, o2.osm_id)
         o2.osm_type, o2.osm_id, op.id as op_id
  from public.osm_features o2
  join public.jurisdictions j on st_contains(j.geom, o2.geom)
  join public.operators op on op.jurisdiction_id = j.id
  where o2.operator_id is null
    and j.state = 'CA' and j.place_type like 'C%'
  order by o2.osm_type, o2.osm_id, st_area(j.geom) asc
) sub
where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;

-- locations_stage (geom is geography; no managing_agency_source column)
update public.locations_stage s
set operator_id = sub.op_id
from (
  select distinct on (s2.fid) s2.fid as id, op.id as op_id
  from public.locations_stage s2
  join public.jurisdictions j on st_contains(j.geom, s2.geom::geometry)
  join public.operators op on op.jurisdiction_id = j.id
  where s2.operator_id is null
    and j.state = 'CA' and j.place_type like 'C%'
  order by s2.fid, st_area(j.geom) asc
) sub
where s.fid = sub.id;


-- ── Refresh denormalized counts on operators ────────────────────────
update public.operators op set
  ccc_point_count   = (select count(*) from public.ccc_access_points where operator_id = op.id),
  usbeach_count     = (select count(*) from public.us_beach_points    where operator_id = op.id),
  osm_feature_count = (select count(*) from public.osm_features       where operator_id = op.id);
