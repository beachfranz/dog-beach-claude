-- CCC landing table — raw output from CCC's ArcGIS FeatureServer.
--
-- Mirrors the pattern from osm_landing and poi_landing: store the raw
-- API output in jsonb (properties + geometry) plus the most-used
-- columns extracted as actual columns for indexing/joins. Enrichment
-- (county / cpad) fills via BEFORE INSERT trigger.
--
-- Source endpoint:
--   https://services9.arcgis.com/wwVnNW92ZHUIr0V0/arcgis/rest/services/
--     AccessPoints/FeatureServer/0/query?where=1=1&outSR=4326&f=geojson
--
-- The existing admin-load-ccc edge function pulls this URL and pushes
-- to load_ccc_batch RPC into public.ccc_access_points. ccc_landing is
-- a parallel ingest path: same source, raw shape, no upsert collision
-- with the live ccc_access_points table.

create table public.ccc_landing (
  fetched_at  timestamptz not null default now(),
  fetched_by  text,
  objectid    integer not null,
  -- Most-used extracted columns (everything else stays in properties)
  name        text,
  county      text,             -- CCC's COUNTY field (string, not FIPS)
  district    text,
  archived    text,             -- 'Yes' / blank — CCC sentinel
  geom        geometry(Point, 4326),
  -- Raw payload
  properties  jsonb not null,   -- the entire ArcGIS feature.properties dict
  geometry    jsonb,             -- the raw GeoJSON geometry
  -- Enrichment (auto via trigger)
  county_geoid   text,
  county_name    text,
  cpad_unit_id   integer,
  cpad_unit_name text,
  primary key (objectid, fetched_at)
);
create index ccc_landing_objectid_idx   on public.ccc_landing (objectid);
create index ccc_landing_fetched_at_idx on public.ccc_landing (fetched_at desc);
create index ccc_landing_geom_idx       on public.ccc_landing using gist (geom);
create index ccc_landing_county_idx     on public.ccc_landing (county_geoid);
create index ccc_landing_cpad_idx       on public.ccc_landing (cpad_unit_id);

comment on table public.ccc_landing is
  'Raw CCC ArcGIS FeatureServer output. Each fetch lands one row per (objectid, fetched_at). properties + geometry preserve the entire feature; name/county/etc. are extracted for easy querying. Future ccc_access_points consumes via promote (TBD).';


-- Auto-enrich trigger (county_geoid + cpad_unit) on INSERT
create or replace function public._ccc_landing_enrich_trigger()
returns trigger
language plpgsql
security definer
as $function$
begin
  if NEW.geom is null then return NEW; end if;

  if NEW.county_geoid is null then
    select c.geoid, c.name
      into NEW.county_geoid, NEW.county_name
      from public.counties c
     where c.state_fp = '06'
       and st_contains(c.geom, NEW.geom)
     order by st_area(c.geom) asc
     limit 1;
  end if;

  if NEW.cpad_unit_id is null then
    select cu.unit_id, cu.unit_name
      into NEW.cpad_unit_id, NEW.cpad_unit_name
      from public.cpad_units cu
     where cu.unit_name is not null and trim(cu.unit_name) <> ''
       and st_contains(cu.geom, NEW.geom)
     order by st_area(cu.geom) asc
     limit 1;
  end if;

  return NEW;
end;
$function$;

drop trigger if exists trg_ccc_landing_enrich on public.ccc_landing;
create trigger trg_ccc_landing_enrich
  before insert on public.ccc_landing
  for each row execute function public._ccc_landing_enrich_trigger();
