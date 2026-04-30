-- Bundles the existing operator-resolution cascade (originally one-shot
-- migrations 20260427_resolve_operator_id.sql and
-- 20260428_operator_cascade_pass4_county.sql) into a single callable
-- function so it can be wrapped by a Dagster asset and re-run whenever
-- a new beach source row needs attribution.
--
-- Idempotent: every UPDATE only touches operator_id IS NULL rows.
-- Manual pins via set_beach_operator() and any tiger_c1 fixed value
-- are preserved.
--
-- Returns a jsonb summary of how many rows each pass touched, plus
-- the final source-distribution.

create or replace function public.resolve_all_operator_ids()
returns jsonb
language plpgsql
security definer
as $function$
declare
  v_p1_ccc int := 0;  v_p1_ubp int := 0;  v_p1_osm int := 0;
  v_p2_osm int := 0;
  v_p3_ccc int := 0;  v_p3_ubp int := 0;  v_p3_osm int := 0;
  v_p3b_ccc int := 0; v_p3b_ubp int := 0; v_p3b_osm int := 0;
  v_p4_ccc int := 0;  v_p4_ubp int := 0;  v_p4_osm int := 0;
  v_dist jsonb;
begin
  -- Pass 1: CPAD smallest-containing polygon
  with sub as (
    select distinct on (c2.objectid) c2.objectid as id, op.id as op_id
      from public.ccc_access_points c2
      join public.cpad_units cu on st_contains(cu.geom, c2.geom)
      join public.operators op on op.slug = public.slugify_agency(cu.mng_agncy)
     where c2.operator_id is null and cu.mng_agncy is not null
     order by c2.objectid, st_area(cu.geom) asc
  ) update public.ccc_access_points c
       set operator_id = sub.op_id, managing_agency_source = 'cpad'
      from sub where c.objectid = sub.id;
  get diagnostics v_p1_ccc = row_count;

  with sub as (
    select distinct on (u2.fid) u2.fid as id, op.id as op_id
      from public.us_beach_points u2
      join public.cpad_units cu on st_contains(cu.geom, u2.geom)
      join public.operators op on op.slug = public.slugify_agency(cu.mng_agncy)
     where u2.operator_id is null and cu.mng_agncy is not null
     order by u2.fid, st_area(cu.geom) asc
  ) update public.us_beach_points u
       set operator_id = sub.op_id, managing_agency_source = 'cpad'
      from sub where u.fid = sub.id;
  get diagnostics v_p1_ubp = row_count;

  with sub as (
    select distinct on (o2.osm_type, o2.osm_id) o2.osm_type, o2.osm_id, op.id as op_id
      from public.osm_features o2
      join public.cpad_units cu on st_contains(cu.geom, o2.geom)
      join public.operators op on op.slug = public.slugify_agency(cu.mng_agncy)
     where o2.operator_id is null and cu.mng_agncy is not null
     order by o2.osm_type, o2.osm_id, st_area(cu.geom) asc
  ) update public.osm_features o
       set operator_id = sub.op_id, managing_agency_source = 'cpad'
      from sub where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;
  get diagnostics v_p1_osm = row_count;

  -- Pass 2: OSM operator tag (osm_features only)
  with sub as (
    select distinct on (o2.osm_type, o2.osm_id) o2.osm_type, o2.osm_id, op.id as op_id
      from public.osm_features o2
      join public.operators op on op.slug = public.slugify_agency(o2.tags->>'operator')
     where o2.operator_id is null
       and o2.tags ? 'operator' and o2.tags->>'operator' <> ''
     order by o2.osm_type, o2.osm_id, op.id
  ) update public.osm_features o
       set operator_id = sub.op_id, managing_agency_source = 'osm_tag'
      from sub where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;
  get diagnostics v_p2_osm = row_count;

  -- Pass 3: TIGER place strict containment
  with sub as (
    select distinct on (c2.objectid) c2.objectid as id, op.id as op_id
      from public.ccc_access_points c2
      join public.jurisdictions j on st_contains(j.geom, c2.geom)
      join public.operators op on op.jurisdiction_id = j.id
     where c2.operator_id is null and j.state = 'CA' and j.place_type like 'C%'
     order by c2.objectid, st_area(j.geom) asc
  ) update public.ccc_access_points c
       set operator_id = sub.op_id, managing_agency_source = 'tiger_c1'
      from sub where c.objectid = sub.id;
  get diagnostics v_p3_ccc = row_count;

  with sub as (
    select distinct on (u2.fid) u2.fid as id, op.id as op_id
      from public.us_beach_points u2
      join public.jurisdictions j on st_contains(j.geom, u2.geom)
      join public.operators op on op.jurisdiction_id = j.id
     where u2.operator_id is null and j.state = 'CA' and j.place_type like 'C%'
     order by u2.fid, st_area(j.geom) asc
  ) update public.us_beach_points u
       set operator_id = sub.op_id, managing_agency_source = 'tiger_c1'
      from sub where u.fid = sub.id;
  get diagnostics v_p3_ubp = row_count;

  with sub as (
    select distinct on (o2.osm_type, o2.osm_id) o2.osm_type, o2.osm_id, op.id as op_id
      from public.osm_features o2
      join public.jurisdictions j on st_contains(j.geom, o2.geom)
      join public.operators op on op.jurisdiction_id = j.id
     where o2.operator_id is null and j.state = 'CA' and j.place_type like 'C%'
     order by o2.osm_type, o2.osm_id, st_area(j.geom) asc
  ) update public.osm_features o
       set operator_id = sub.op_id, managing_agency_source = 'tiger_c1'
      from sub where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;
  get diagnostics v_p3_osm = row_count;

  -- Pass 3-buffered: TIGER place ST_DWithin 200m
  with sub as (
    select distinct on (c2.objectid) c2.objectid as id, op.id as op_id
      from public.ccc_access_points c2
      join public.jurisdictions j
        on st_dwithin(j.geom::geography, c2.geom::geography, 200)
      join public.operators op on op.jurisdiction_id = j.id
     where c2.operator_id is null
       and (c2.archived is null or c2.archived <> 'Yes')
       and (c2.admin_inactive is null or c2.admin_inactive = false)
       and j.state = 'CA' and j.place_type like 'C%'
     order by c2.objectid, st_area(j.geom) asc
  ) update public.ccc_access_points c
       set operator_id = sub.op_id, managing_agency_source = 'tiger_c1_buffered'
      from sub where c.objectid = sub.id;
  get diagnostics v_p3b_ccc = row_count;

  with sub as (
    select distinct on (u2.fid) u2.fid as id, op.id as op_id
      from public.us_beach_points u2
      join public.jurisdictions j
        on st_dwithin(j.geom::geography, u2.geom::geography, 200)
      join public.operators op on op.jurisdiction_id = j.id
     where u2.operator_id is null and u2.state = 'CA'
       and (u2.admin_inactive is null or u2.admin_inactive = false)
       and j.state = 'CA' and j.place_type like 'C%'
     order by u2.fid, st_area(j.geom) asc
  ) update public.us_beach_points u
       set operator_id = sub.op_id, managing_agency_source = 'tiger_c1_buffered'
      from sub where u.fid = sub.id;
  get diagnostics v_p3b_ubp = row_count;

  with sub as (
    select distinct on (o2.osm_type, o2.osm_id) o2.osm_type, o2.osm_id, op.id as op_id
      from public.osm_features o2
      join public.jurisdictions j on st_dwithin(j.geom, o2.geom_full, 0.002)
      join public.operators op on op.jurisdiction_id = j.id
     where o2.operator_id is null
       and o2.feature_type in ('beach','dog_friendly_beach')
       and (o2.admin_inactive is null or o2.admin_inactive = false)
       and o2.geom_full is not null
       and j.state = 'CA' and j.place_type like 'C%'
     order by o2.osm_type, o2.osm_id, st_area(j.geom) asc
  ) update public.osm_features o
       set operator_id = sub.op_id, managing_agency_source = 'tiger_c1_buffered'
      from sub where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;
  get diagnostics v_p3b_osm = row_count;

  -- Pass 4: TIGER county fallback
  with sub as (
    select distinct on (c2.objectid) c2.objectid as id, op.id as op_id
      from public.ccc_access_points c2
      join public.counties co on st_contains(co.geom, c2.geom)
      join public.operators op on op.county_geoid = co.geoid
     where c2.operator_id is null
       and (c2.archived is null or c2.archived <> 'Yes')
       and (c2.admin_inactive is null or c2.admin_inactive = false)
       and co.geoid like '06%'
     order by c2.objectid, st_area(co.geom) asc
  ) update public.ccc_access_points cc
       set operator_id = sub.op_id, managing_agency_source = 'tiger_county'
      from sub where cc.objectid = sub.id;
  get diagnostics v_p4_ccc = row_count;

  with sub as (
    select distinct on (u2.fid) u2.fid as id, op.id as op_id
      from public.us_beach_points u2
      join public.counties c on st_contains(c.geom, u2.geom)
      join public.operators op on op.county_geoid = c.geoid
     where u2.operator_id is null and u2.state = 'CA'
       and (u2.admin_inactive is null or u2.admin_inactive = false)
       and c.geoid like '06%'
     order by u2.fid, st_area(c.geom) asc
  ) update public.us_beach_points u
       set operator_id = sub.op_id, managing_agency_source = 'tiger_county'
      from sub where u.fid = sub.id;
  get diagnostics v_p4_ubp = row_count;

  with sub as (
    select distinct on (o2.osm_type, o2.osm_id) o2.osm_type, o2.osm_id, op.id as op_id
      from public.osm_features o2
      join public.counties c on st_intersects(c.geom, o2.geom_full)
      join public.operators op on op.county_geoid = c.geoid
     where o2.operator_id is null
       and o2.feature_type in ('beach','dog_friendly_beach')
       and (o2.admin_inactive is null or o2.admin_inactive = false)
       and o2.geom_full is not null
       and c.geoid like '06%'
     order by o2.osm_type, o2.osm_id, st_area(c.geom) asc
  ) update public.osm_features o
       set operator_id = sub.op_id, managing_agency_source = 'tiger_county'
      from sub where o.osm_type = sub.osm_type and o.osm_id = sub.osm_id;
  get diagnostics v_p4_osm = row_count;

  -- Refresh denormalized operator counts
  update public.operators op set
    ccc_point_count   = (select count(*) from public.ccc_access_points where operator_id = op.id),
    usbeach_count     = (select count(*) from public.us_beach_points    where operator_id = op.id),
    osm_feature_count = (select count(*) from public.osm_features       where operator_id = op.id);

  -- Final source distribution across UBP+CCC (the two that feed beach_locations)
  select jsonb_object_agg(coalesce(src, 'null'), n)
    into v_dist
    from (
      select managing_agency_source as src, count(*) as n from public.us_beach_points
       where state = 'CA' and (admin_inactive is null or admin_inactive = false)
       group by 1
      union all
      select managing_agency_source as src, count(*) as n from public.ccc_access_points
       where (archived is null or archived <> 'Yes')
         and (admin_inactive is null or admin_inactive = false)
       group by 1
    ) x;

  return jsonb_build_object(
    'pass1_cpad',           jsonb_build_object('ccc', v_p1_ccc, 'ubp', v_p1_ubp, 'osm', v_p1_osm),
    'pass2_osm_tag',        jsonb_build_object('osm', v_p2_osm),
    'pass3_tiger_c1',       jsonb_build_object('ccc', v_p3_ccc, 'ubp', v_p3_ubp, 'osm', v_p3_osm),
    'pass3b_tiger_buffered',jsonb_build_object('ccc', v_p3b_ccc, 'ubp', v_p3b_ubp, 'osm', v_p3b_osm),
    'pass4_tiger_county',   jsonb_build_object('ccc', v_p4_ccc, 'ubp', v_p4_ubp, 'osm', v_p4_osm),
    'final_source_distribution', v_dist
  );
end;
$function$;

comment on function public.resolve_all_operator_ids() is
  'Re-runs the operator-resolution cascade (CPAD → OSM tag → TIGER place strict → TIGER place 200m buffer → county fallback) on rows with operator_id IS NULL. Idempotent. Returns jsonb summary of which pass updated which counts. Wrapped by Dagster asset operator_id_resolve_run.';
