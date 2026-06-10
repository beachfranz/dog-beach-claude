-- 20260610g_nws_srf_helpers.sql
--
-- Two small RPCs the refresh-nws-srf edge function needs:
--
--   1. _nws_zone_set_geom_from_geojson(p_zone_id, p_geojson)
--      Sets nws_zones.geom for a zone whose row was inserted by the edge fn
--      without a geom value. PostgREST cannot directly evaluate
--      ST_GeomFromGeoJSON in a regular INSERT/UPDATE — wrapping it in an
--      RPC is the simplest path.
--
--   2. _rebind_beach_coastal_zones()
--      Fires a no-op UPDATE on every active beach so tg_compute_coastal_zone
--      re-PIPs against nws_zones. Called by refresh-nws-srf after any new
--      zone is warmed, so the cached coastal_zone_id column flips from NULL
--      to the matching zone without manual intervention.

begin;

create or replace function public._nws_zone_set_geom_from_geojson(
  p_zone_id text,
  p_geojson text
)
returns void
language sql
security definer
set search_path = 'public'
as $$
  update public.nws_zones
     set geom = st_geomfromgeojson(p_geojson)
   where zone_id = p_zone_id;
$$;

grant execute on function public._nws_zone_set_geom_from_geojson(text, text)
  to service_role;

comment on function public._nws_zone_set_geom_from_geojson is
  'Sets nws_zones.geom from a raw GeoJSON string. Called by the refresh-nws-srf '
  'edge fn after lazy-inserting a zone row (PostgREST cannot inline ST_GeomFromGeoJSON).';


drop function if exists public._rebind_beach_coastal_zones();
drop function if exists public._rebind_beach_coastal_zones(text);

create or replace function public._rebind_beach_coastal_zones(
  p_state text default null
)
returns integer
language plpgsql
security definer
set search_path = 'public'
set statement_timeout = '10min'
as $$
declare
  v_count integer;
begin
  -- Direct PIP — bypasses tg_compute_coastal_zone (which only fires on
  -- INSERT or lat/lon actual change, not a no-op write). For each active
  -- coastal-state beach whose binding is NULL, find the nearest forecast
  -- zone within 200m. Cost: one geography KNN per affected beach.
  --
  -- p_state filter lets the caller batch by state — PostgREST applies a
  -- ~60s wall-clock timeout per request that the function-level
  -- statement_timeout doesn't override. Per-state batches stay well
  -- inside the cap.
  update public.beaches_gold bg
     set coastal_zone_id = (
       select z.zone_id
         from public.nws_zones z
        where z.zone_type = 'forecast'
          and z.geom is not null
          and st_dwithin(
                z.geom::geography,
                st_setsrid(st_makepoint(bg.lon::float, bg.lat::float), 4326)::geography,
                200
              )
        order by st_distance(
                   z.geom::geography,
                   st_setsrid(st_makepoint(bg.lon::float, bg.lat::float), 4326)::geography
                 )
        limit 1
     )
   where bg.is_active = true
     and bg.coastal_zone_id is null
     and bg.lat is not null and bg.lon is not null
     and (p_state is null or bg.state = p_state)
     and bg.state in ('CA','OR','WA','HI','MA','RI','NH','ME','MD','VA','DE',
                      'NC','SC','GA','FL','AL','MS','LA','TX','NJ','NY','CT');
  get diagnostics v_count = row_count;
  return v_count;
end $$;

grant execute on function public._rebind_beach_coastal_zones(text)
  to service_role;

comment on function public._rebind_beach_coastal_zones is
  'No-op UPDATE on active beaches in coastal states with NULL coastal_zone_id; '
  're-fires tg_compute_coastal_zone so newly-warmed nws_zones now match. '
  'Called by refresh-nws-srf after lazy-warming new coastal forecast zones.';

commit;
