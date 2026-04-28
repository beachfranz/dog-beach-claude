-- Give operators their own first-class geometry. Per-operator footprint
-- comes from one of three sources, in priority order:
--   1. jurisdictions.geom — for city operators that match a TIGER place
--   2. counties.geom      — for county operators
--   3. ST_Multi(ST_Union(cpad_units.geom)) — for everyone else with a
--                           cpad_agncy_name (state agencies, federal
--                           agencies, special districts, private, etc.)
--
-- Operators with neither a jurisdiction/county FK nor CPAD evidence
-- (most federal/tribal seed entries) get NULL geom until we attach
-- PAD-US or hand-curated layers.

alter table public.operators
  add column if not exists geom geometry(MultiPolygon, 4326),
  add column if not exists footprint_area_km2 numeric;

create index if not exists operators_geom_gix on public.operators using gist (geom);


-- One-shot refresh function. Run after CPAD or jurisdictions reload.
create or replace function public.refresh_operator_geometries()
returns int
language plpgsql security definer as $$
declare
  n int := 0;
begin
  -- Pass 1: city operators — borrow jurisdictions.geom
  update public.operators op
  set geom = j.geom
  from public.jurisdictions j
  where op.jurisdiction_id = j.id;

  -- Pass 2: county operators — borrow counties.geom
  update public.operators op
  set geom = c.geom
  from public.counties c
  where op.county_geoid = c.geoid;

  -- Pass 3: CPAD-derived operators (no jurisdiction, no county) —
  -- ST_Multi(ST_Union(...)) of their CPAD polygons. ST_Union is slower
  -- than ST_Collect but produces a clean MultiPolygon (no overlapping
  -- duplicates from CPAD's polygon-per-parcel breakdown).
  update public.operators op
  set geom = sub.g
  from (
    select op2.id,
           st_multi(st_union(cu.geom)) as g
    from public.operators op2
    join public.cpad_units cu on cu.mng_agncy = op2.cpad_agncy_name
    where op2.geom is null
      and op2.cpad_agncy_name is not null
    group by op2.id
  ) sub
  where op.id = sub.id;

  -- Refresh denormalized area
  update public.operators op
  set footprint_area_km2 = round((st_area(geom::geography) / 1e6)::numeric, 0)
  where geom is not null;

  select count(*) into n from public.operators where geom is not null;
  return n;
end;
$$;

revoke all on function public.refresh_operator_geometries() from public, anon, authenticated;
grant execute on function public.refresh_operator_geometries() to service_role;

-- Run the refresh now (statement runs as postgres in migration; no anon timeout).
select public.refresh_operator_geometries();


-- Update the polygon-cache refresh to source from operators.geom rather
-- than from cpad_units directly. operators.geom is now the authoritative
-- footprint per operator; the cache is just a simplified rendering view.
create or replace function public.refresh_operator_polygons_topn(p_n int default 10)
returns int
language plpgsql security definer as $$
declare
  n int;
begin
  delete from public.operator_polygons_topn;
  insert into public.operator_polygons_topn
    (rank, operator_id, slug, canonical_name, level, subtype,
     poly_count, area_km2, geojson)
  select row_number() over (order by op.footprint_area_km2 desc nulls last)::int as rank,
         op.id, op.slug, op.canonical_name, op.level, op.subtype,
         coalesce((select count(*)::int
                   from public.cpad_units cu
                   where cu.mng_agncy = op.cpad_agncy_name), 0) as poly_count,
         op.footprint_area_km2,
         st_asgeojson(st_simplify(op.geom, 0.005))::jsonb
  from public.operators op
  where op.geom is not null
  order by op.footprint_area_km2 desc nulls last
  limit p_n;
  get diagnostics n = row_count;
  return n;
end;
$$;

select public.refresh_operator_polygons_topn(10);

-- Anon can read the geom from operators directly (no statement timeout
-- risk for individual rows; just don't ORDER BY a complex expression).
-- Already granted SELECT on operators earlier.
