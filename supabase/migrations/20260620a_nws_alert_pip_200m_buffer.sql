-- 20260620a_nws_alert_pip_200m_buffer.sql
--
-- Fix: NWS alert → beach matching silently drops beaches whose pin sits
-- water-side of the coastal-zone / alert polygon edge. Beach pins sit
-- water-side of land polygons by definition, so place-PIP must tolerate a
-- ~200m gap per HARD rule [[pip-for-places-uses-200m]]. Both RPCs used bare
-- ST_Intersects with no buffer.
--
-- Canonical incident: Huntington State Beach (fid 8453) sits 9.5m water-side
-- of CAZ552 (Orange County Coastal). A Beach Hazards Statement explicitly
-- covering "Orange County Coastal Areas" never attached, so HSB missed the
-- BHS + its 2 derived hazard rows (longshore + high surf) that adjacent
-- Huntington Dog Beach (fid 6212, dist 0m) received. 658 scoreable beaches
-- sit within 200m of but outside their own coastal zone polygon.
--
-- Zone path  → option 2: match the materialized beaches_gold.coastal_zone_id
--              directly (same membership source SRF/rip already use; the
--              column was itself PIP-backfilled at 200m in 20260610d). No
--              geometry test, self-consistent across pipelines, cheaper.
-- Inline path → option 1: add the 200m buffer (ST_DWithin 0.0018°). No
--              materialized analog exists for arbitrary alert geometry.

-- ── Zone-coded alerts (affectedZones UGC) — reuse materialized membership ──
CREATE OR REPLACE FUNCTION public._nws_alert_pip_via_zone(p_zone_id text, p_state text)
 RETURNS bigint[]
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  select coalesce(array_agg(b.fid), array[]::bigint[])
    from public.beaches_gold b
   where b.is_active = true
     and b.scoring_tier in ('daily','hourly')
     and b.state = upper(p_state)
     and b.coastal_zone_id = p_zone_id;
$function$;

-- ── Inline-geometry alerts (CWA-bounded products) — 200m buffer ──
CREATE OR REPLACE FUNCTION public._nws_alert_pip_beaches(p_geom_geojson text, p_state text)
 RETURNS bigint[]
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  select coalesce(array_agg(b.fid), array[]::bigint[])
    from public.beaches_gold b
   where b.is_active = true
     and b.scoring_tier in ('daily','hourly')
     and b.state = upper(p_state)
     and b.geom is not null
     and st_dwithin(
           b.geom,
           st_setsrid(st_geomfromgeojson(p_geom_geojson), 4326),
           0.0018
         );
$function$;
