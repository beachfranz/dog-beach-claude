-- 20260610d_beaches_gold_coastal_zone.sql
--
-- Cache the NWS coastal forecast zone ID on beaches_gold so find_beaches RPC
-- can JOIN to wfo_srf_forecast without a per-row PIP at read time.
--
-- Mirrors 20260525_weather_grid_materialization.sql + 20260606k_marine_grid_materialization.sql:
--   - cached column on beaches_gold
--   - BEFORE INSERT/UPDATE trigger refreshes the column when lat/lon change
--   - backfill statement at the bottom for existing rows
--
-- Spatial match: ST_DWithin(zone.geom, beach.geom, 0.0018) (~200m) per HARD
-- rule [[pip-for-places-uses-200m]] — coastal zone polygons stop at
-- high-water-line; beach pins sit ~10-200m on the water side. Strict
-- ST_Contains under-counts. Picking the NEAREST eligible zone breaks ties
-- when a beach sits at a zone boundary (e.g., harbor mouth between two
-- coastal county zones).
--
-- nws_zones is lazily populated by scripts/poll_nws_alerts.py — the trigger
-- only matches zones already cached. Trigger fn is therefore idempotent;
-- the edge fn refresh-nws-srf will re-fire backfill via UPDATE after warming
-- new zones in the bootstrap pass.

begin;

-- ───────────────────────────────────────────────────────────────────────────
-- 1. Cached column on beaches_gold
-- ───────────────────────────────────────────────────────────────────────────

alter table public.beaches_gold
  add column if not exists coastal_zone_id text;

create index if not exists beaches_gold_coastal_zone_idx
  on public.beaches_gold (coastal_zone_id)
  where is_active and coastal_zone_id is not null;

comment on column public.beaches_gold.coastal_zone_id is
  'NWS coastal forecast zone ID (e.g. CAZ041) covering this beach. Auto-maintained by tg_compute_coastal_zone. Consumers JOIN wfo_srf_forecast on this column; rip_current_risk + similar SRF-derived fields flow from it. NULL = inland or PIP miss against the lazy-loaded nws_zones cache.';

-- ───────────────────────────────────────────────────────────────────────────
-- 2. Trigger function — populate cached column from nws_zones PIP
-- ───────────────────────────────────────────────────────────────────────────

create or replace function public.tg_compute_coastal_zone()
returns trigger
language plpgsql
as $$
declare
  v_changed boolean;
  v_zone_id text;
begin
  v_changed := tg_op = 'INSERT'
            or new.lat is distinct from old.lat
            or new.lon is distinct from old.lon;

  if not v_changed then
    return new;
  end if;

  if new.lat is null or new.lon is null then
    new.coastal_zone_id := null;
    return new;
  end if;

  -- Nearest forecast zone within 200m of the beach pin. nws_zones.geom is
  -- geometry (SRID 4326), so cast to geography for meters-based distance.
  -- The column may not yet cover all coastal zones — trigger leaves NULL
  -- on miss; refresh-nws-srf re-fires the trigger after warming new zones.
  select z.zone_id
    into v_zone_id
    from public.nws_zones z
   where z.zone_type = 'forecast'
     and z.geom is not null
     and st_dwithin(
           z.geom::geography,
           st_setsrid(st_makepoint(new.lon::float, new.lat::float), 4326)::geography,
           200  -- meters
         )
   order by st_distance(
              z.geom::geography,
              st_setsrid(st_makepoint(new.lon::float, new.lat::float), 4326)::geography
            )
   limit 1;

  new.coastal_zone_id := v_zone_id;
  return new;
end $$;

comment on function public.tg_compute_coastal_zone is
  'Trigger fn on beaches_gold. PIPs the beach against nws_zones (zone_type=forecast) within 200m, taking the NEAREST. ST_DWithin tolerance per [[pip-for-places-uses-200m]] — coastal zone polygons stop at high-water-line, beach pins sit on the water side. nws_zones is lazily warmed by poll_nws_alerts.py + refresh-nws-srf; trigger leaves NULL on PIP miss.';

-- ───────────────────────────────────────────────────────────────────────────
-- 3. Trigger on beaches_gold
-- ───────────────────────────────────────────────────────────────────────────

drop trigger if exists tg_beaches_gold_coastal_zone on public.beaches_gold;
create trigger tg_beaches_gold_coastal_zone
  before insert or update on public.beaches_gold
  for each row execute function public.tg_compute_coastal_zone();

-- ───────────────────────────────────────────────────────────────────────────
-- 4. Backfill existing rows
-- ───────────────────────────────────────────────────────────────────────────
--
-- Only coastal MVP+ states — inland states will never match a forecast
-- zone and would just churn the trigger for no result. Other states get
-- auto-populated on their next UPDATE or when newly inserted.
--
-- This first pass uses whatever nws_zones is already cached; the edge fn
-- refresh-nws-srf will re-trigger the same beaches after warming any
-- coastal zones the parser surfaces.

update public.beaches_gold
   set lat = lat  -- no-op write to fire the trigger
 where state in ('CA','OR','WA','HI','MA','RI','NH','ME','MD','VA','DE','NC','SC','GA','FL','AL','MS','LA','TX','NJ','NY','CT')
   and is_active = true
   and lat is not null and lon is not null;

commit;
