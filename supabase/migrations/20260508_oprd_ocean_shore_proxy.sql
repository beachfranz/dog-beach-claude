-- 20260508_oprd_ocean_shore_proxy.sql
--
-- Synthesize a proxy polygon for OPRD's Ocean Shore Recreation Area.
-- Reason: the canonical OPRD ocean-shore corridor is not represented as
-- a single PAD-US polygon (PAD-US only encodes discrete state-park
-- units). Without a polygon, the pad_us emitter classifies most OR
-- coastal beaches as adjacent-CITY or no-match, when the truth is
-- "OPRD-administered ocean shore (Beach Bill)".
--
-- Approach: take the union of OR coastal counties (Clatsop, Tillamook,
-- Lincoln, Lane, Coos, Curry), erode 2km inward, subtract — yields a
-- 2km strip along the county outer ring. Clip to longitude west of
-- -123.5° to keep only the Pacific-facing edge. Insert into pad_us_units
-- with mng_name='SPR', unit_name labeled as a synthesized proxy.
--
-- Same trick for WA: coastal counties (Clallam, Jefferson, Mason,
-- Kitsap, Pierce, Thurston, Pacific, Grays Harbor, Wahkiakum, San Juan,
-- Island, Skagit, Snohomish, King, Whatcom) → tideland strip.

begin;

-- ── OR Ocean Shore Recreation Area (proxy) ─────────────────────────────
with coast_counties as (
  select st_union(geom) as g
    from public.counties
   where state_fp = '41'
     and name in ('Clatsop','Tillamook','Lincoln','Lane','Coos','Curry')
),
shrunk as (
  select st_buffer(g::geography, -2000)::geometry as g
    from coast_counties
),
strip as (
  select st_difference(c.g, s.g) as g
    from coast_counties c, shrunk s
),
pacific_strip as (
  select st_multi(st_intersection(strip.g,
           st_setsrid(st_makeenvelope(-125.0, 41.99, -123.5, 46.30), 4326)))::geometry(MultiPolygon, 4326) as geom
    from strip
)
insert into public.pad_us_units
  (unit_id, unit_name, own_name, mng_name, mng_type, state, geom, raw_attrs)
select
  -8000041 as unit_id,                                      -- synthetic negative id
  'Oregon Ocean Shore Recreation Area (proxy)',
  'Oregon Parks and Recreation Department',
  'SPR', 'STAT',
  'OR',
  geom,
  jsonb_build_object(
    'Pub_Access', 'OA',
    'source', 'synthesized from coastal counties west edge',
    'notes', 'Proxy for OPRD ocean-shore corridor under ORS 390.605-390.770'
  )
  from pacific_strip
on conflict (unit_id) do update
  set unit_name = excluded.unit_name,
      own_name = excluded.own_name,
      mng_name = excluded.mng_name,
      mng_type = excluded.mng_type,
      state = excluded.state,
      geom = excluded.geom,
      raw_attrs = excluded.raw_attrs;

-- ── WA Tideland Strip (proxy for SMA / Public Trust) ───────────────────
with coast_counties as (
  select st_union(geom) as g
    from public.counties
   where state_fp = '53'
     and name in ('Clallam','Jefferson','Mason','Kitsap','Pierce','Thurston',
                  'Pacific','Grays Harbor','Wahkiakum','San Juan','Island',
                  'Skagit','Snohomish','King','Whatcom')
),
shrunk as (
  select st_buffer(g::geography, -2000)::geometry as g from coast_counties
),
strip as (
  select st_difference(c.g, s.g) as g from coast_counties c, shrunk s
),
puget_strip as (
  select st_multi(st_intersection(strip.g,
           st_setsrid(st_makeenvelope(-125.0, 45.5, -122.0, 49.0), 4326)))::geometry(MultiPolygon, 4326) as geom
    from strip
)
insert into public.pad_us_units
  (unit_id, unit_name, own_name, mng_name, mng_type, state, geom, raw_attrs)
select
  -8000053 as unit_id,
  'Washington Tideland (proxy)',
  'Washington Department of Natural Resources',
  'SPR', 'STAT',
  'WA',
  geom,
  jsonb_build_object(
    'Pub_Access', 'OA',
    'source', 'synthesized from coastal counties west/northwest edge',
    'notes', 'Proxy for WA tidelands and shoreline under RCW 90.58 (Shoreline Mgmt Act)'
  )
  from puget_strip
on conflict (unit_id) do update
  set unit_name = excluded.unit_name,
      own_name = excluded.own_name,
      mng_name = excluded.mng_name,
      mng_type = excluded.mng_type,
      state = excluded.state,
      geom = excluded.geom,
      raw_attrs = excluded.raw_attrs;

commit;
