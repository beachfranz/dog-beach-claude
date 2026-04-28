-- RPC: operators clipped to a single county. Returns one row per
-- operator whose footprint intersects the county, with geometry
-- intersected to the county boundary. Each row also carries a label
-- point (centroid of the clipped geom) so the page can place a single
-- tooltip per operator instead of one per polygon part.
--
-- Cached as a materialized result by county_geoid since intersection
-- and centroid calc are heavy. Refresh on demand.

create table if not exists public.operator_polygons_by_county_cache (
  county_geoid    text   not null,
  operator_id     bigint not null references public.operators(id),
  slug            text   not null,
  canonical_name  text   not null,
  level           text,
  subtype         text,
  area_km2        numeric,
  label_lat       double precision,
  label_lng       double precision,
  geojson         jsonb  not null,
  refreshed_at    timestamptz not null default now(),
  primary key (county_geoid, operator_id)
);

create index if not exists operator_polygons_by_county_geoid_idx
  on public.operator_polygons_by_county_cache(county_geoid);

grant select on public.operator_polygons_by_county_cache to anon, authenticated;
alter table public.operator_polygons_by_county_cache disable row level security;


create or replace function public.refresh_operator_polygons_for_county(p_geoid text)
returns int
language plpgsql security definer as $$
declare
  n int;
  v_county_geom geometry;
begin
  select geom into v_county_geom from public.counties where geoid = p_geoid;
  if v_county_geom is null then
    raise exception 'No county with geoid %', p_geoid;
  end if;

  delete from public.operator_polygons_by_county_cache where county_geoid = p_geoid;

  insert into public.operator_polygons_by_county_cache
    (county_geoid, operator_id, slug, canonical_name, level, subtype,
     area_km2, label_lat, label_lng, geojson)
  select
    p_geoid,
    op.id,
    op.slug,
    op.canonical_name,
    op.level,
    op.subtype,
    round((st_area(clipped::geography) / 1e6)::numeric, 2) as area_km2,
    st_y(label_pt) as label_lat,
    st_x(label_pt) as label_lng,
    coalesce(
      st_asgeojson(st_simplify(clipped, 0.0005))::jsonb,
      st_asgeojson(clipped)::jsonb
    )
  from (
    select op.id, op.slug, op.canonical_name, op.level, op.subtype,
           st_intersection(op.geom, v_county_geom) as clipped,
           st_pointonsurface(st_intersection(op.geom, v_county_geom)) as label_pt
    from public.operators op
    where op.geom is not null
      and st_intersects(op.geom, v_county_geom)
  ) op
  where not st_isempty(clipped);
  get diagnostics n = row_count;
  return n;
end;
$$;

revoke all on function public.refresh_operator_polygons_for_county(text) from public, anon, authenticated;
grant execute on function public.refresh_operator_polygons_for_county(text) to service_role;

-- Initial population for Orange County (CA, geoid 06059)
select public.refresh_operator_polygons_for_county('06059');
