-- 20260506_save_truth_polygon_rpc.sql
--
-- RPC for the admin-save-truth-polygon edge function. Takes GeoJSON
-- text, converts to PostGIS geometry, upserts on (fid). Replaces any
-- prior truth polygon for the same beach (re-drawing is the workflow).

begin;

create or replace function public.exec_truth_polygon_upsert(
  p_fid       int,
  p_geom_json text,
  p_drawn_by  text default null,
  p_notes     text default null
)
returns table (
  id          bigint,
  fid         int,
  area_m2     double precision,
  drawn_at    timestamptz,
  drawn_by    text,
  notes       text
)
language plpgsql
security definer
as $$
declare
  v_id bigint;
begin
  -- Ensure the fid actually exists in beaches_gold (FK would block otherwise,
  -- but this gives a clearer error).
  if not exists (select 1 from public.beaches_gold where beaches_gold.fid = p_fid) then
    raise exception 'fid % not found in beaches_gold', p_fid;
  end if;

  insert into public.beach_polygons_truth as t
    (fid, geom, drawn_by, notes, source)
  values (
    p_fid,
    st_setsrid(st_geomfromgeojson(p_geom_json), 4326),
    coalesce(p_drawn_by, 'curator'),
    p_notes,
    'admin_ui'
  )
  on conflict (fid) do update set
    geom       = excluded.geom,
    drawn_by   = excluded.drawn_by,
    notes      = excluded.notes,
    drawn_at   = now(),
    source     = 'admin_ui'
  returning t.id into v_id;

  return query
    select t.id, t.fid, t.area_m2, t.drawn_at, t.drawn_by, t.notes
      from public.beach_polygons_truth t
     where t.id = v_id;
end;
$$;

revoke all on function public.exec_truth_polygon_upsert(int, text, text, text)
  from public, anon, authenticated;
grant  execute on function public.exec_truth_polygon_upsert(int, text, text, text)
  to service_role;

commit;
