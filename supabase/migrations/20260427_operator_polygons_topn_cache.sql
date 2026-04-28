-- Materialize top-N operator polygons. The on-the-fly RPC hit the
-- 8-second anon statement timeout because ST_Simplify + ST_Collect +
-- ST_AsGeoJSON across thousands of CPAD polygons is too heavy per
-- request. Cache the result; the admin page hits this table directly.

create table if not exists public.operator_polygons_topn (
  rank           int primary key,
  operator_id    bigint not null references public.operators(id),
  slug           text not null,
  canonical_name text not null,
  level          text,
  subtype        text,
  poly_count     int not null,
  area_km2       numeric not null,
  geojson        jsonb not null,
  refreshed_at   timestamptz not null default now()
);

create index if not exists operator_polygons_topn_op_idx
  on public.operator_polygons_topn(operator_id);

-- One-shot refresh function. Call after operators or cpad_units change.
create or replace function public.refresh_operator_polygons_topn(p_n int default 10)
returns int
language plpgsql security definer as $$
declare
  n int;
begin
  delete from public.operator_polygons_topn;
  with ranked as (
    select op.id,
           op.slug,
           op.canonical_name,
           op.level,
           op.subtype,
           count(*)::int as polys,
           sum(st_area(cu.geom::geography)) as area_m2,
           st_collect(st_simplify(cu.geom, 0.005)) as g
    from public.operators op
    join public.cpad_units cu on cu.mng_agncy = op.cpad_agncy_name
    where op.cpad_agncy_name is not null
    group by op.id, op.slug, op.canonical_name, op.level, op.subtype
    order by area_m2 desc
    limit p_n
  ),
  numbered as (
    select row_number() over (order by area_m2 desc) as rank, *
    from ranked
  )
  insert into public.operator_polygons_topn
    (rank, operator_id, slug, canonical_name, level, subtype,
     poly_count, area_km2, geojson)
  select rank::int, id, slug, canonical_name, level, subtype,
         polys, round((area_m2/1e6)::numeric, 0),
         st_asgeojson(g)::jsonb
  from numbered;
  get diagnostics n = row_count;
  return n;
end;
$$;

revoke all on function public.refresh_operator_polygons_topn(int) from public, anon, authenticated;
grant execute on function public.refresh_operator_polygons_topn(int) to service_role;

-- Anon reads the cached rows directly
grant select on public.operator_polygons_topn to anon, authenticated;
alter table public.operator_polygons_topn disable row level security;

-- Drop the old per-request RPC (it timed out under anon)
drop function if exists public.top_operator_polygons(int);

-- Initial population (runs as postgres via the migration; no anon timeout)
select public.refresh_operator_polygons_topn(10);
