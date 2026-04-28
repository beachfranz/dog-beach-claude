-- Two new fallback passes for the operator cascade, applied to rows
-- still null after the original cascade (CPAD → OSM tag → TIGER place
-- strict containment).
--
-- Pass 3-buffered: TIGER place via ST_DWithin (200m for points, 0.002°
-- ≈ 200m for polygons). Beach centroids that sit just seaward of the
-- city boundary now attribute to the city.
--
-- Pass 4: TIGER county fallback. Coastal stretches in unincorporated
-- land (Big Sur, Lost Coast, Mendocino/Humboldt rural coast) attribute
-- to the county operator.
--
-- Manual overrides preserved throughout. Each UPDATE statement runs
-- independently to fit Cloudflare's 100s gateway timeout when invoked
-- via `supabase db query`.

-- ── Pass 3-buffered: OSM polygons ───────────────────────────────────
update public.osm_features o
set operator_id = sub.op_id,
    managing_agency_source = 'tiger_c1_buffered'
from (
  select distinct on (o2.osm_type, o2.osm_id)
    o2.osm_type, o2.osm_id, op.id as op_id
  from public.osm_features o2
  join public.jurisdictions j on st_dwithin(j.geom, o2.geom_full, 0.002)
  join public.operators op on op.jurisdiction_id = j.id
  where o2.operator_id is null
    and o2.feature_type in ('beach','dog_friendly_beach')
    and (o2.admin_inactive is null or o2.admin_inactive = false)
    and o2.geom_full is not null
    and j.state = 'CA' and j.place_type like 'C%'
  order by o2.osm_type, o2.osm_id, st_area(j.geom) asc
) sub
where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;

-- ── Pass 3-buffered: UBP points ─────────────────────────────────────
update public.us_beach_points u
set operator_id = sub.op_id,
    managing_agency_source = 'tiger_c1_buffered'
from (
  select distinct on (u2.fid)
    u2.fid, op.id as op_id
  from public.us_beach_points u2
  join public.jurisdictions j
    on st_dwithin(j.geom::geography, u2.geom::geography, 200)
  join public.operators op on op.jurisdiction_id = j.id
  where u2.operator_id is null
    and u2.state = 'CA'
    and (u2.admin_inactive is null or u2.admin_inactive = false)
    and j.state = 'CA' and j.place_type like 'C%'
  order by u2.fid, st_area(j.geom) asc
) sub
where u.fid = sub.fid;

-- ── Pass 3-buffered: CCC points ─────────────────────────────────────
update public.ccc_access_points c
set operator_id = sub.op_id,
    managing_agency_source = 'tiger_c1_buffered'
from (
  select distinct on (c2.objectid)
    c2.objectid, op.id as op_id
  from public.ccc_access_points c2
  join public.jurisdictions j
    on st_dwithin(j.geom::geography, c2.geom::geography, 200)
  join public.operators op on op.jurisdiction_id = j.id
  where c2.operator_id is null
    and (c2.archived is null or c2.archived <> 'Yes')
    and (c2.admin_inactive is null or c2.admin_inactive = false)
    and j.state = 'CA' and j.place_type like 'C%'
  order by c2.objectid, st_area(j.geom) asc
) sub
where c.objectid = sub.objectid;

-- ── Pass 4 county: OSM polygons ─────────────────────────────────────
update public.osm_features o
set operator_id = sub.op_id,
    managing_agency_source = 'tiger_county'
from (
  select distinct on (o2.osm_type, o2.osm_id)
    o2.osm_type, o2.osm_id, op.id as op_id
  from public.osm_features o2
  join public.counties c on st_intersects(c.geom, o2.geom_full)
  join public.operators op on op.county_geoid = c.geoid
  where o2.operator_id is null
    and o2.feature_type in ('beach','dog_friendly_beach')
    and (o2.admin_inactive is null or o2.admin_inactive = false)
    and o2.geom_full is not null
    and c.geoid like '06%'
  order by o2.osm_type, o2.osm_id, st_area(c.geom) asc
) sub
where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;

-- ── Pass 4 county: UBP points ───────────────────────────────────────
update public.us_beach_points u
set operator_id = sub.op_id,
    managing_agency_source = 'tiger_county'
from (
  select distinct on (u2.fid)
    u2.fid, op.id as op_id
  from public.us_beach_points u2
  join public.counties c on st_contains(c.geom, u2.geom)
  join public.operators op on op.county_geoid = c.geoid
  where u2.operator_id is null
    and u2.state = 'CA'
    and (u2.admin_inactive is null or u2.admin_inactive = false)
    and c.geoid like '06%'
  order by u2.fid, st_area(c.geom) asc
) sub
where u.fid = sub.fid;

-- ── Pass 4 county: CCC points ───────────────────────────────────────
update public.ccc_access_points cc
set operator_id = sub.op_id,
    managing_agency_source = 'tiger_county'
from (
  select distinct on (c2.objectid)
    c2.objectid, op.id as op_id
  from public.ccc_access_points c2
  join public.counties co on st_contains(co.geom, c2.geom)
  join public.operators op on op.county_geoid = co.geoid
  where c2.operator_id is null
    and (c2.archived is null or c2.archived <> 'Yes')
    and (c2.admin_inactive is null or c2.admin_inactive = false)
    and co.geoid like '06%'
  order by c2.objectid, st_area(co.geom) asc
) sub
where cc.objectid = sub.objectid;

-- Refresh denormalized counts on operators
update public.operators op set
  ccc_point_count   = (select count(*) from public.ccc_access_points where operator_id = op.id),
  usbeach_count     = (select count(*) from public.us_beach_points    where operator_id = op.id),
  osm_feature_count = (select count(*) from public.osm_features       where operator_id = op.id);
