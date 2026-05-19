-- 20260519_nws_zones_cache.sql
--
-- Cache table for NWS forecast/marine/public zone polygons. Backs the
-- zone-code resolution path in scripts/poll_nws_alerts.py — many NWS
-- alerts (especially Beach Hazards Statements + High Surf for marine
-- waters) ship with affectedZones=['<zone_id>'] instead of an inline
-- geometry. v1 of the poller skipped those entirely.
--
-- Lazy population: on first encounter, the poller fetches
-- api.weather.gov/zones/<type>/<id>.geojson and inserts the geom here.
-- Zones change rarely (annual updates), so cache freshness is generous.

begin;

create table if not exists public.nws_zones (
  zone_id      text  primary key,           -- e.g. 'PZZ565', 'CAZ506'
  zone_type    text  not null,              -- 'forecast' | 'public' | 'marine' | 'county' | 'fire' | 'offshore'
  name         text,                        -- human-readable zone name
  state        text,                        -- 2-letter state code (when applicable)
  geom         geometry(Geometry, 4326),    -- zone polygon (Polygon or MultiPolygon)
  fetched_at   timestamptz not null default now(),
  raw          jsonb                        -- full properties payload
);

create index if not exists nws_zones_geom_gix on public.nws_zones using gist (geom);
create index if not exists nws_zones_state_idx on public.nws_zones (state) where state is not null;

comment on table public.nws_zones is
  'Cached NWS forecast/marine/public/etc zone polygons. Populated lazily by '
  'scripts/poll_nws_alerts.py whenever an alert lists affectedZones=[...] '
  'instead of inline geometry. Zones rarely change; refresh on demand only.';

commit;
