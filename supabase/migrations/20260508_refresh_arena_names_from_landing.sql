-- 20260508_refresh_arena_names_from_landing.sql
--
-- Phase 3: sync arena.name from osm_landing when contributors update the
-- name on the source side. Today, arena.name is set once at promote time
-- and stays stuck; this function brings it forward.

begin;

create or replace function public.refresh_arena_names_from_osm_landing()
returns table(arena_rows_updated bigint)
language plpgsql
security definer
as $function$
declare
  v_updated bigint := 0;
begin
  perform set_config('app.arena_clustering_active', 'true', true);

  with latest_osm as (
    select distinct on (type, id)
           type, id, coalesce(name, tags->>'name', '') as new_name
      from public.osm_landing
     where (name is not null and name <> '') or (tags->>'name' is not null)
     order by type, id, fetched_at desc
  ),
  applied as (
    update public.arena a
       set name = lo.new_name,
           updated_at = now()
      from latest_osm lo
     where a.source_code = 'osm'
       and a.source_id = 'osm/' || lo.type || '/' || lo.id::text
       and lo.new_name <> ''
       and (a.name is null or a.name <> lo.new_name)
       and a.is_active = true
    returning 1
  )
  select count(*) into v_updated from applied;

  perform set_config('app.arena_clustering_active', 'false', true);

  return query select v_updated;
end;
$function$;

grant execute on function public.refresh_arena_names_from_osm_landing() to anon, authenticated, service_role;

commit;
