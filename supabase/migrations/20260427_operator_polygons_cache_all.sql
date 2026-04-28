-- Expand the polygon-render cache from top-10 to all operators with
-- geometry. Color-by-level on the page replaces color-by-rank.
--
-- Rename operator_polygons_topn → operator_polygons_cache to reflect
-- the new semantics. Drop the rank column (no longer meaningful when
-- everyone is included; sort by area_km2 if you want a leaderboard).

drop table if exists public.operator_polygons_topn cascade;

create table public.operator_polygons_cache (
  operator_id     bigint primary key references public.operators(id),
  slug            text not null,
  canonical_name  text not null,
  level           text,
  subtype         text,
  poly_count      int not null,
  area_km2        numeric,
  geojson         jsonb not null,
  refreshed_at    timestamptz not null default now()
);

create index if not exists operator_polygons_cache_level_idx
  on public.operator_polygons_cache(level);
create index if not exists operator_polygons_cache_area_idx
  on public.operator_polygons_cache(area_km2 desc nulls last);

grant select on public.operator_polygons_cache to anon, authenticated;
alter table public.operator_polygons_cache disable row level security;


-- Drop the old top-N refresh function; replace with all-operators
drop function if exists public.refresh_operator_polygons_topn(int);

create or replace function public.refresh_operator_polygons_cache()
returns int
language plpgsql security definer as $$
declare n int;
begin
  delete from public.operator_polygons_cache;
  insert into public.operator_polygons_cache
    (operator_id, slug, canonical_name, level, subtype,
     poly_count, area_km2, geojson)
  select op.id, op.slug, op.canonical_name, op.level, op.subtype,
         coalesce((select count(*)::int from public.cpad_units cu
                   where cu.mng_agncy = op.cpad_agncy_name), 0),
         op.footprint_area_km2,
         coalesce(
           st_asgeojson(st_simplify(op.geom, 0.005))::jsonb,
           st_asgeojson(op.geom)::jsonb
         )
  from public.operators op
  where op.geom is not null;
  get diagnostics n = row_count;
  return n;
end;
$$;

revoke all on function public.refresh_operator_polygons_cache() from public, anon, authenticated;
grant execute on function public.refresh_operator_polygons_cache() to service_role;

select public.refresh_operator_polygons_cache();
