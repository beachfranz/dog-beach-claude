-- Add TIGER place (city / CDP) to poi_landing + osm_landing.
--
-- Same PIP pattern we use for county_geoid + cpad_unit_id: smallest
-- containing polygon from public.jurisdictions wins. CA-only data,
-- so non-CA rows just stay null.
--
-- Each row gets a single place plus its TIGER place_type code, which
-- tells us what *kind* of place it is — same pattern as
-- populate_layer1_geographic in 20260424_jurisdictions_buffer_two_pass.sql.
--
-- TIGER place_type codes seen in jurisdictions:
--   C1..C9  — incorporated places (cities, towns, villages)
--   U1..U2  — census designated places (CDPs, unincorporated communities)
--
-- Columns added:
--   place_fips  — 5-char fips_place (e.g. '31393' for Grover Beach)
--   place_name  — short name (e.g. 'Grover Beach')
--   place_type  — TIGER class code (C1, U1, ...)
--
-- Triggers updated to fill on INSERT. Existing rows backfilled.

-- ── 1. Schema ───────────────────────────────────────────────────────

alter table public.poi_landing
  add column if not exists place_fips text,
  add column if not exists place_name text,
  add column if not exists place_type text;

alter table public.osm_landing
  add column if not exists place_fips text,
  add column if not exists place_name text,
  add column if not exists place_type text;

create index if not exists poi_landing_place_idx on public.poi_landing (place_fips);
create index if not exists osm_landing_place_idx on public.osm_landing (place_fips);


-- ── 2. POI trigger — add place block ────────────────────────────────

create or replace function public._poi_landing_enrich_trigger()
returns trigger
language plpgsql
security definer
as $function$
begin
  if NEW.geom is null and NEW.raw_wkt is not null then
    begin
      NEW.geom := ST_SetSRID(ST_GeomFromText(NEW.raw_wkt), 4326);
    exception when others then
      NEW.geom := null;
    end;
  end if;
  if NEW.geom is null then return NEW; end if;

  if NEW.county_geoid is null then
    select c.geoid, c.name
      into NEW.county_geoid, NEW.county_name
      from public.counties c
     where st_contains(c.geom, NEW.geom)
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

  if NEW.place_fips is null then
    select j.fips_place, j.name, j.place_type
      into NEW.place_fips, NEW.place_name, NEW.place_type
      from public.jurisdictions j
     where st_contains(j.geom, NEW.geom)
     order by st_area(j.geom) asc
     limit 1;
  end if;

  return NEW;
end;
$function$;


-- ── 3. OSM trigger — add place block ────────────────────────────────

create or replace function public._osm_landing_enrich_trigger()
returns trigger
language plpgsql
security definer
as $function$
declare
  v_geom geometry;
begin
  if NEW.type = 'node' and NEW.lat is not null and NEW.lon is not null then
    v_geom := ST_SetSRID(ST_MakePoint(NEW.lon, NEW.lat), 4326);
  elsif NEW.geom_full is not null then
    v_geom := ST_Centroid(NEW.geom_full);
  end if;

  if v_geom is null then return NEW; end if;

  if NEW.cpad_unit_id is null then
    select cu.unit_id, cu.unit_name
      into NEW.cpad_unit_id, NEW.cpad_unit_name
      from public.cpad_units cu
     where cu.unit_name is not null and trim(cu.unit_name) <> ''
       and st_contains(cu.geom, v_geom)
     order by st_area(cu.geom) asc
     limit 1;
  end if;

  if NEW.county_geoid is null then
    select c.geoid, c.name
      into NEW.county_geoid, NEW.county_name
      from public.counties c
     where c.state_fp = '06'
       and st_contains(c.geom, v_geom)
     order by st_area(c.geom) asc
     limit 1;
  end if;

  if NEW.place_fips is null then
    select j.fips_place, j.name, j.place_type
      into NEW.place_fips, NEW.place_name, NEW.place_type
      from public.jurisdictions j
     where st_contains(j.geom, v_geom)
     order by st_area(j.geom) asc
     limit 1;
  end if;

  return NEW;
end;
$function$;


-- ── 4. Backfill — POI ───────────────────────────────────────────────

with pick as (
  select distinct on (l.fid, l.fetched_at)
         l.fid, l.fetched_at, j.fips_place, j.name, j.place_type
    from public.poi_landing l
    join public.jurisdictions j on st_contains(j.geom, l.geom)
   where l.place_fips is null and l.geom is not null
   order by l.fid, l.fetched_at, st_area(j.geom) asc
)
update public.poi_landing l
   set place_fips = pk.fips_place,
       place_name = pk.name,
       place_type = pk.place_type
  from pick pk
 where l.fid = pk.fid and l.fetched_at = pk.fetched_at;


-- ── 5. Backfill — OSM ───────────────────────────────────────────────

with pick as (
  select distinct on (l.type, l.id, l.fetched_at)
         l.type, l.id, l.fetched_at, j.fips_place, j.name, j.place_type
    from public.osm_landing l
    join public.jurisdictions j
      on st_contains(
           j.geom,
           case
             when l.type = 'node' and l.lat is not null and l.lon is not null
               then ST_SetSRID(ST_MakePoint(l.lon, l.lat), 4326)
             when l.geom_full is not null
               then ST_Centroid(l.geom_full)
           end
         )
   where l.place_fips is null
     and (
       (l.type = 'node' and l.lat is not null and l.lon is not null)
       or l.geom_full is not null
     )
   order by l.type, l.id, l.fetched_at, st_area(j.geom) asc
)
update public.osm_landing l
   set place_fips = pk.fips_place,
       place_name = pk.name,
       place_type = pk.place_type
  from pick pk
 where l.type = pk.type
   and l.id = pk.id
   and l.fetched_at = pk.fetched_at;
