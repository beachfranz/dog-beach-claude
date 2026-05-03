-- BEFORE INSERT trigger that enriches osm_landing rows with CPAD unit
-- + TIGER county at land time. Auto-applies to any future INSERT,
-- regardless of source (fetcher script, manual psql, dbt seed, etc.).
--
-- Logic:
--   1. Build a representative point from whatever's on the row:
--      - nodes  -> ST_MakePoint(lon, lat)
--      - ways/relations with geom_full -> ST_Centroid(geom_full)
--      - else NULL (no enrichment, columns stay null)
--   2. Spatial-lookup smallest containing CPAD polygon (no name filter)
--      -> cpad_unit_id + cpad_unit_name
--   3. Spatial-lookup containing county polygon (CA only)
--      -> county_geoid + county_name
--
-- Caller-set values are preserved (i.e. if a fetcher writes the
-- enrichment columns explicitly, the trigger doesn't overwrite). This
-- also means the backfill is preserved.

create or replace function public._osm_landing_enrich_trigger()
returns trigger
language plpgsql
security definer
as $function$
declare
  v_geom geometry;
begin
  -- Build representative point from row data
  if NEW.type = 'node' and NEW.lat is not null and NEW.lon is not null then
    v_geom := ST_SetSRID(ST_MakePoint(NEW.lon, NEW.lat), 4326);
  elsif NEW.geom_full is not null then
    v_geom := ST_Centroid(NEW.geom_full);
  end if;

  if v_geom is null then return NEW; end if;  -- nothing to enrich on

  -- Smallest containing CPAD polygon
  if NEW.cpad_unit_id is null then
    select cu.unit_id, cu.unit_name
      into NEW.cpad_unit_id, NEW.cpad_unit_name
      from public.cpad_units cu
     where cu.unit_name is not null and trim(cu.unit_name) <> ''
       and st_contains(cu.geom, v_geom)
     order by st_area(cu.geom) asc
     limit 1;
  end if;

  -- Smallest containing CA county
  if NEW.county_geoid is null then
    select c.geoid, c.name
      into NEW.county_geoid, NEW.county_name
      from public.counties c
     where c.state_fp = '06'
       and st_contains(c.geom, v_geom)
     order by st_area(c.geom) asc
     limit 1;
  end if;

  return NEW;
end;
$function$;

drop trigger if exists trg_osm_landing_enrich on public.osm_landing;
create trigger trg_osm_landing_enrich
  before insert on public.osm_landing
  for each row execute function public._osm_landing_enrich_trigger();

comment on trigger trg_osm_landing_enrich on public.osm_landing is
  'Auto-enriches landing rows with CPAD unit + TIGER county at INSERT time. Caller-set values preserved.';
