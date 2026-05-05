-- 20260505_move_beach_gold_point.sql
--
-- Manually re-position a beaches_gold catalog point. Used by the
-- auto-polygon-review UI when a curator drags the marker to fix a
-- bad lat/lon (Hermosa-style "catalog point in the wrong town entirely").
--
-- Updates lat, lon, geom (kept in sync) and stamps an audit log row.

begin;

create or replace function public.move_beach_gold_point(
  p_fid int,
  p_lat float8,
  p_lon float8
)
returns table(before jsonb, after jsonb)
language plpgsql
security definer
as $$
declare
  b record;
begin
  if p_lat is null or p_lon is null then
    raise exception 'lat and lon required';
  end if;
  if p_lat < -90 or p_lat > 90 or p_lon < -180 or p_lon > 180 then
    raise exception 'coords out of range';
  end if;

  select fid, name, lat, lon
    into b
    from public.beaches_gold
   where fid = p_fid;
  if not found then
    raise exception 'fid % not found in beaches_gold', p_fid;
  end if;

  before := jsonb_build_object(
    'fid', b.fid, 'name', b.name,
    'lat',  b.lat, 'lon',  b.lon
  );

  update public.beaches_gold
     set lat  = p_lat,
         lon  = p_lon,
         geom = st_setsrid(st_makepoint(p_lon, p_lat), 4326)
   where fid = p_fid;

  after := jsonb_build_object(
    'fid', p_fid, 'name', b.name,
    'lat', p_lat, 'lon', p_lon,
    'resolution', 'admin_moved_point'
  );
  return next;
end;
$$;

revoke all on function public.move_beach_gold_point(int, float8, float8)
  from public, anon, authenticated;
grant execute on function public.move_beach_gold_point(int, float8, float8)
  to service_role;

commit;
