-- Phase 14h — repoint dog-park RPCs from dropped public.operator (singular)
--                                       to     public.operators (plural).
--
-- find.html ("dog parks" toggle) calls nearest_dog_parks via PostgREST.
-- After Phase 5a (89d11d8) dropped public.operator, the RPC errors with
-- "relation public.operator does not exist" — find.html shows no dog parks.
-- Same shape: find_dog_parks (alt RPC) + get_nearest_dog_park_for_beach
-- (beach.html "nearby alternative dog parks" panel).
--
-- This is the bug Step 1's Bucket B audit missed because I scoped the
-- audit to functions joining `public.operators` (plural) — these three
-- referenced the singular table and slipped through the regex.
--
-- Three column changes:
--   public.operator     → public.operators
--   op.name             → op.canonical_name
--   (op.short_name      → unchanged; plural table has it)
--
-- Plus AND op.is_canonical = true on the JOIN (per Step 1 pattern).

BEGIN;

-- ─────────────────────────────────────────────────────────────────────
-- 1. nearest_dog_parks (the one find.html calls)
-- ─────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.nearest_dog_parks(
  p_lat double precision DEFAULT NULL::double precision,
  p_lng double precision DEFAULT NULL::double precision,
  p_limit integer DEFAULT 10,
  p_state text DEFAULT NULL::text,
  p_date date DEFAULT NULL::date
)
RETURNS TABLE(fid bigint, name text, state text, address_city text, lat double precision, lon double precision, distance_miles double precision, has_fence boolean, has_drinking_water boolean, surface text, leash_policy text, hours_text text, hours_open_time text, hours_close_time text, additional_rules text, source text, source_url text, consensus_confidence numeric, operator_id bigint, operator_name text, operator_short text, composite_score numeric, day_status_v2 text, composite_score_v2 integer, best_window_label text, best_window_status text, go_hours_count integer, summary_weather text, weather_code integer, avg_temp numeric, avg_wind numeric, score_v2 integer, score_v2_components jsonb, drive_minutes integer)
LANGUAGE sql
STABLE PARALLEL SAFE
AS $function$
  with user_pt as (
    select case when p_lat is not null and p_lng is not null
                then st_setsrid(st_makepoint(p_lng, p_lat), 4326)::geography
                else null::geography end as pt,
           coalesce(p_date, current_date) as d
  )
  select
    g.fid, g.name, g.state, g.address_city, g.lat, g.lon,
    case when up.pt is not null
         then st_distance(up.pt, g.geom::geography) / 1609.344
         else null::double precision
    end as distance_miles,
    g.has_fence, g.has_drinking_water, g.surface,
    p.leash_policy, p.hours_text, p.hours_open_time, p.hours_close_time, p.additional_rules,
    p.source, p.source_url, p.consensus_confidence,
    p.operator_id, o.canonical_name as operator_name, o.short_name as operator_short,   -- ★ canonical_name + plural
    hs.composite_score,
    dr.day_status_v2, dr.composite_score_v2,
    dr.best_window_label, dr.best_window_status, dr.go_hours_count,
    dr.summary_weather, dr.weather_code, dr.avg_temp, dr.avg_wind,
    (sv.payload->>'score')::int           as score_v2,
    sv.payload                            as score_v2_components,
    (sv.payload->>'drive_minutes')::int   as drive_minutes
  from public.dog_parks_gold g
  join public.dog_park_dog_policy p on p.dog_park_fid = g.fid
  cross join user_pt up
  left join public.operators o on o.id = p.operator_id and o.is_canonical = true   -- ★ plural + is_canonical filter
  left join public.dog_park_day_recommendations dr
         on dr.dog_park_fid = g.fid and dr.local_date = up.d
  left join lateral (
    select round(avg(h.hour_score_v2)::numeric, 0) as composite_score
      from public.dog_park_day_hourly_scores h
     where h.dog_park_fid = g.fid
       and h.local_date = up.d
       and h.hour_score_v2 is not null
  ) hs on true
  left join lateral (
    select public.compute_dog_park_score_v2(g.fid, p_lat, p_lng, up.d) as payload
  ) sv on true
  where g.is_active = true
    and (p_state is null or g.state = p_state)
  order by
    case when up.pt is not null then st_distance(up.pt, g.geom::geography) end nulls last,
    coalesce((sv.payload->>'score')::int, 0) desc,
    g.name
  limit greatest(coalesce(p_limit, 10), 1);
$function$;

-- ─────────────────────────────────────────────────────────────────────
-- 2. find_dog_parks (alt RPC for find.html)
-- ─────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.find_dog_parks(
  p_date date,
  p_lat double precision DEFAULT NULL::double precision,
  p_lng double precision DEFAULT NULL::double precision,
  p_leash text DEFAULT 'any'::text,
  p_limit integer DEFAULT NULL::integer,
  p_scored_only boolean DEFAULT false,
  p_surface text DEFAULT NULL::text,
  p_fence boolean DEFAULT NULL::boolean,
  p_water boolean DEFAULT NULL::boolean
)
RETURNS TABLE(dog_park_fid bigint, name text, display_name text, latitude double precision, longitude double precision, surface text, has_fence boolean, has_drinking_water boolean, double_gate boolean, small_dog_area boolean, large_dog_area boolean, lighting boolean, leash_policy text, off_leash_flag boolean, hours_text text, hours_open_time text, hours_close_time text, additional_rules text, description text, source text, source_url text, operator_id bigint, operator_short text, distance_m double precision, day_status_v2 text, composite_score_v2 integer, best_window_label text, best_window_status text, summary_weather text, weather_code integer, avg_temp numeric, avg_wind numeric, avg_uv numeric, go_hours_count integer, composite_score numeric)
LANGUAGE sql
STABLE SECURITY DEFINER
SET search_path TO 'public'
AS $function$
  select
    g.fid                                                       as dog_park_fid,
    g.name                                                      as name,
    coalesce(g.display_name_override, g.name)                   as display_name,
    g.lat                                                       as latitude,
    g.lon                                                       as longitude,
    coalesce(dp.surface_overlay, g.surface)                     as surface,
    coalesce(dp.has_fence, g.has_fence)                         as has_fence,
    coalesce(dp.has_drinking_water, g.has_drinking_water)       as has_drinking_water,
    dp.double_gate,
    dp.small_dog_area,
    dp.large_dog_area,
    dp.lighting,
    coalesce(dp.leash_policy, 'off_leash')                      as leash_policy,
    coalesce(dp.off_leash_flag, true)                           as off_leash_flag,
    dp.hours_text,
    dp.hours_open_time,
    dp.hours_close_time,
    dp.additional_rules,
    coalesce(dp.description_overlay, g.description)             as description,
    dp.source,
    dp.source_url,
    dp.operator_id,
    op.canonical_name                                           as operator_short,   -- ★ canonical_name
    case
      when p_lat is not null and p_lng is not null
      then ST_Distance(g.geom::geography, ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography)
      else null
    end                                                         as distance_m,
    dr.day_status_v2,
    dr.composite_score_v2,
    dr.best_window_label,
    dr.best_window_status,
    dr.summary_weather,
    dr.weather_code,
    dr.avg_temp,
    dr.avg_wind,
    dr.avg_uv,
    dr.go_hours_count,
    case
      when dr.go_hours_count is null then null
      else round(
        100.0 * dr.go_hours_count
        / nullif(dr.go_hours_count + dr.advisory_hours_count
               + dr.caution_hours_count + dr.no_go_hours_count, 0),
        0
      )::numeric
    end                                                         as composite_score
  from public.dog_parks_gold g
  left join public.dog_park_dog_policy dp on dp.dog_park_fid = g.fid
  left join public.operators op on op.id = dp.operator_id and op.is_canonical = true   -- ★ plural + canonical
  left join public.dog_park_day_recommendations dr
         on dr.dog_park_fid = g.fid and dr.local_date = p_date
  where g.is_active = true
    and g.is_scoreable = true
    and (
      p_leash = 'any'
      or (p_leash = 'off' and coalesce(dp.off_leash_flag, true) is true)
      or (p_leash = 'on'  and dp.leash_policy = 'on_leash')
    )
    and (p_surface is null or coalesce(dp.surface_overlay, g.surface) = p_surface)
    and (p_fence   is null or coalesce(dp.has_fence,          g.has_fence)          = p_fence)
    and (p_water   is null or coalesce(dp.has_drinking_water, g.has_drinking_water) = p_water)
    and (not p_scored_only or dr.day_status_v2 is not null)
  order by
    case
      when p_lat is not null and p_lng is not null
      then g.geom::geography <-> ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography
      else null
    end nulls last
  limit case when p_limit is not null and p_limit > 0 then p_limit else null end;
$function$;

-- ─────────────────────────────────────────────────────────────────────
-- 3. get_nearest_dog_park_for_beach (beach.html "nearby alternatives")
-- ─────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public.get_nearest_dog_park_for_beach(p_beach_fid bigint)
RETURNS TABLE(dp_fid bigint, dp_name text, dp_lat double precision, dp_lon double precision, dp_city text, dp_state text, distance_miles double precision, leash_policy text, hours_text text, hours_open_time text, hours_close_time text, additional_rules text, source text, source_url text, consensus_confidence numeric, operator_id bigint, operator_name text, operator_short text)
LANGUAGE sql
STABLE PARALLEL SAFE
AS $function$
  SELECT
    g.fid, g.name, g.lat, g.lon, g.address_city, g.state,
    b.nearest_dog_park_distance_m / 1609.344 AS distance_miles,
    p.leash_policy, p.hours_text, p.hours_open_time, p.hours_close_time,
    p.additional_rules, p.source, p.source_url, p.consensus_confidence,
    p.operator_id, o.canonical_name AS operator_name, o.short_name AS operator_short   -- ★ canonical_name
  FROM public.beaches_gold b
  JOIN public.dog_parks_gold g       ON g.fid = b.nearest_dog_park_fid
  JOIN public.dog_park_dog_policy p  ON p.dog_park_fid = g.fid
  LEFT JOIN public.operators o       ON o.id = p.operator_id AND o.is_canonical = true   -- ★ plural + canonical
  WHERE b.fid = p_beach_fid;
$function$;

COMMIT;
