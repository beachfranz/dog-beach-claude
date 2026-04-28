-- RPC for the top-N operators map. Returns one MultiPolygon (collected,
-- not unioned — much faster) per top-N operator ranked by total CPAD-
-- managed area in CA. Geometries pre-simplified to ~500m tolerance to
-- keep payload manageable for a statewide view.

create or replace function public.top_operator_polygons(p_n int default 10)
returns table (
  operator_id    bigint,
  slug           text,
  canonical_name text,
  level          text,
  subtype        text,
  poly_count     int,
  area_km2       numeric,
  geojson        jsonb
)
language sql stable security definer
as $$
  with ranked as (
    select op.id,
           op.slug,
           op.canonical_name,
           op.level,
           op.subtype,
           count(*) as polys,
           sum(st_area(cu.geom::geography)) as area_m2,
           st_collect(st_simplify(cu.geom, 0.005)) as g
    from public.operators op
    join public.cpad_units cu on cu.mng_agncy = op.cpad_agncy_name
    where op.cpad_agncy_name is not null
    group by op.id, op.slug, op.canonical_name, op.level, op.subtype
    order by area_m2 desc
    limit p_n
  )
  select id, slug, canonical_name, level, subtype, polys::int,
         round((area_m2/1e6)::numeric, 0),
         st_asgeojson(g)::jsonb
  from ranked;
$$;

grant execute on function public.top_operator_polygons(int) to anon, authenticated;
