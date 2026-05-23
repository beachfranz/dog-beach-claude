-- 20260523_timezone_per_state.sql
--
-- Fixes the structural bug where every non-PT state's beaches inherit
-- timezone='America/Los_Angeles' from promote_to_gold's hard-coded
-- default. Discovered 2026-05-23 EVENING during post-merge verification:
-- NH/MA/MI/OH/RI/MD/DE all had wrong tz, meaning every forecast time
-- was rendered in PT (off by 3 hours from local in Eastern states).
--
-- Three pieces:
--   1. NEW helper public._iana_tz_for_state(state_code text) — canonical
--      IANA tz per state (state's largest-population zone for splits).
--   2. CREATE OR REPLACE promote_to_gold to use the helper instead of
--      hard-coding 'America/Los_Angeles'.
--   3. Backfill beaches_gold + beach_day_recommendations +
--      beach_day_hourly_scores so the consumer surface picks up the
--      correct tz immediately, without waiting for nightly daily-refresh.

BEGIN;

-- ── 1. Helper function ─────────────────────────────────────────────
CREATE OR REPLACE FUNCTION public._iana_tz_for_state(p_state_code text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $function$
  -- State → canonical IANA timezone for the largest-population zone.
  -- Split-tz states (FL/IN/KY/MI/TN/TX/etc.) use their majority tz here;
  -- per-beach overrides can be set manually in beaches_gold.timezone
  -- where needed (e.g., NW Florida panhandle = America/Chicago).
  select case upper(p_state_code)
    when 'AL' then 'America/Chicago'
    when 'AK' then 'America/Anchorage'
    when 'AZ' then 'America/Phoenix'           -- no DST (except Navajo)
    when 'AR' then 'America/Chicago'
    when 'CA' then 'America/Los_Angeles'
    when 'CO' then 'America/Denver'
    when 'CT' then 'America/New_York'
    when 'DC' then 'America/New_York'
    when 'DE' then 'America/New_York'
    when 'FL' then 'America/New_York'
    when 'GA' then 'America/New_York'
    when 'HI' then 'Pacific/Honolulu'
    when 'IA' then 'America/Chicago'
    when 'ID' then 'America/Boise'
    when 'IL' then 'America/Chicago'
    when 'IN' then 'America/Indiana/Indianapolis'
    when 'KS' then 'America/Chicago'
    when 'KY' then 'America/Kentucky/Louisville'
    when 'LA' then 'America/Chicago'
    when 'MA' then 'America/New_York'
    when 'MD' then 'America/New_York'
    when 'ME' then 'America/New_York'
    when 'MI' then 'America/Detroit'
    when 'MN' then 'America/Chicago'
    when 'MO' then 'America/Chicago'
    when 'MS' then 'America/Chicago'
    when 'MT' then 'America/Denver'
    when 'NC' then 'America/New_York'
    when 'ND' then 'America/Chicago'
    when 'NE' then 'America/Chicago'
    when 'NH' then 'America/New_York'
    when 'NJ' then 'America/New_York'
    when 'NM' then 'America/Denver'
    when 'NV' then 'America/Los_Angeles'
    when 'NY' then 'America/New_York'
    when 'OH' then 'America/New_York'
    when 'OK' then 'America/Chicago'
    when 'OR' then 'America/Los_Angeles'
    when 'PA' then 'America/New_York'
    when 'RI' then 'America/New_York'
    when 'SC' then 'America/New_York'
    when 'SD' then 'America/Chicago'
    when 'TN' then 'America/Chicago'
    when 'TX' then 'America/Chicago'
    when 'UT' then 'America/Denver'
    when 'VA' then 'America/New_York'
    when 'VT' then 'America/New_York'
    when 'WA' then 'America/Los_Angeles'
    when 'WI' then 'America/Chicago'
    when 'WV' then 'America/New_York'
    when 'WY' then 'America/Denver'
    else null
  end;
$function$;

COMMENT ON FUNCTION public._iana_tz_for_state(text) IS
  'Returns the canonical IANA timezone string for a US state code. '
  'Split-tz states default to their largest-population zone; per-beach '
  'overrides via beaches_gold.timezone for known exceptions (e.g. NW '
  'Florida panhandle = America/Chicago).';

GRANT EXECUTE ON FUNCTION public._iana_tz_for_state(text)
  TO anon, authenticated, service_role;

-- ── 2. CREATE OR REPLACE promote_to_gold to use helper ─────────────
CREATE OR REPLACE FUNCTION public.promote_to_gold(p_fids bigint[], p_score boolean DEFAULT false, p_publish boolean DEFAULT true)
RETURNS TABLE(rows_promoted bigint, rows_already_in_gold bigint, containment_evidence bigint, source_evidence bigint, beach_dog_policy_changed bigint, beach_amenities_changed bigint, noaa_assigned bigint, slugs_assigned bigint)
LANGUAGE plpgsql
AS $function$
declare promoted int := 0; existing int := 0; fid_iter bigint;
        contain_ev int := 0; source_ev int := 0;
        bdp_changed int := 0; ba_changed int := 0;
        noaa_count int := 0; slug_count int := 0;
        new_fids bigint[];
begin
  if p_fids is null or array_length(p_fids, 1) is null then
    return query select 0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint;
    return;
  end if;

  -- ── Step 1: INSERT new beaches_gold rows
  with to_promote as (
    select a.* from public.arena a
     where a.fid = any(p_fids) and a.is_active = true
       and not exists (select 1 from public.beaches_gold g where g.fid = a.fid)
       and not exists (
         select 1 from public.beaches_gold g
           join public.arena a2 on a2.fid = g.fid
          where a2.group_id = coalesce(a.group_id, a.fid)
            and g.is_active and g.fid <> a.fid
       )
  ),
  to_promote_with_canonical as (
    select t.*,
           coalesce(osm.nav_lat, t.nav_lat, t.lat) as canonical_lat,
           coalesce(osm.nav_lon, t.nav_lon, t.lon) as canonical_lon,
           -- Resolve state first so we can derive the IANA tz from it.
           coalesce(public._infer_state_from_county_fips(t.county_fips),
                    public._infer_state_from_county(t.county_name)) as resolved_state
      from to_promote t
      left join lateral (
        select nav_lat, nav_lon
          from public.arena
         where group_id = coalesce(t.group_id, t.fid)
           and source_code = 'osm' and is_active = true
           and nav_lat is not null and nav_lon is not null
         limit 1
      ) osm on true
  ),
  ins as (
    insert into public.beaches_gold
      (fid, location_id, name, lat, lon, county_name, county_fips,
       source_code, source_id, group_id,
       nav_lat, nav_lon, nav_source, name_source, park_name, state, promoted_from, promoted_at,
       is_active, noaa_station_id, timezone, open_time, close_time, geom)
    select t.fid, public._make_location_slug(t.name, t.county_name, t.fid),
      t.name, t.canonical_lat, t.canonical_lon, t.county_name, t.county_fips,
      t.source_code, t.source_id, coalesce(t.group_id, t.fid),
      t.nav_lat, t.nav_lon, t.nav_source, t.name_source, t.park_name,
      t.resolved_state,
      'promote_to_gold_v3', now(), true,
      public._nearest_noaa_station(st_setsrid(st_makepoint(t.canonical_lon, t.canonical_lat), 4326)::geography),
      -- NEW (2026-05-23): state-aware IANA tz instead of LA hard-code.
      coalesce(public._iana_tz_for_state(t.resolved_state), 'America/Los_Angeles'),
      '05:00', '22:00',
      st_setsrid(st_makepoint(t.canonical_lon, t.canonical_lat), 4326)
    from to_promote_with_canonical t
    on conflict (fid) do nothing
    returning fid, location_id, noaa_station_id
  )
  select array_agg(fid), count(*), count(*) filter (where location_id is not null),
         count(*) filter (where noaa_station_id is not null)
    into new_fids, promoted, slug_count, noaa_count from ins;
  existing := array_length(p_fids, 1) - promoted;

  -- Step 2: build the beach evidence chain for every fid (delegates)
  foreach fid_iter in array p_fids loop
    perform public.build_beach_evidence(fid_iter);
  end loop;

  select count(*) into contain_ev from public.beach_enrichment_provenance
   where field_group = 'polygon_containment' and gold_fid = any(p_fids);
  select count(*) into source_ev from public.beach_enrichment_provenance
   where field_group in ('dogs','practical','access','governance')
     and gold_fid = any(p_fids);

  select count(*) into bdp_changed from public.beach_dog_policy
   where arena_group_id = any(p_fids) and source = 'auto_promoted_from_consensus';
  select count(*) into ba_changed from public.beach_amenities
   where arena_group_id = any(p_fids) and source = 'auto_promoted_from_consensus';

  return query select promoted::bigint, existing::bigint, contain_ev::bigint, source_ev::bigint,
                      bdp_changed::bigint, ba_changed::bigint, noaa_count::bigint, slug_count::bigint;
end;
$function$;

-- ── 3a. Backfill beaches_gold.timezone for all states ──────────────
UPDATE public.beaches_gold g
   SET timezone = public._iana_tz_for_state(g.state)
 WHERE g.state IS NOT NULL
   AND public._iana_tz_for_state(g.state) IS NOT NULL
   AND (g.timezone IS NULL OR g.timezone <> public._iana_tz_for_state(g.state));

-- ── 3b. Backfill beach_day_recommendations.timezone ────────────────
-- Denormalized from beaches_gold.timezone (set at daily-refresh write
-- time). Update in-place so the consumer surface picks up the right tz
-- immediately, without waiting for tomorrow night's refresh.
UPDATE public.beach_day_recommendations r
   SET timezone = g.timezone
  FROM public.beaches_gold g
 WHERE g.fid = r.arena_group_id
   AND r.timezone IS DISTINCT FROM g.timezone;

-- ── 3c. Backfill beach_day_hourly_scores.timezone if present ───────
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_name = 'beach_day_hourly_scores' AND column_name = 'timezone'
  ) THEN
    EXECUTE $sql$
      UPDATE public.beach_day_hourly_scores h
         SET timezone = g.timezone
        FROM public.beaches_gold g
       WHERE g.fid = h.arena_group_id
         AND h.timezone IS DISTINCT FROM g.timezone
    $sql$;
  END IF;
END $$;

COMMIT;

-- Sanity (post-apply):
--   SELECT state, timezone, COUNT(*) FROM public.beaches_gold
--    WHERE is_active GROUP BY 1,2 ORDER BY 1;
--   Expected: each state shows its correct tz; no Pacific tz outside
--   CA/OR/WA/NV.
