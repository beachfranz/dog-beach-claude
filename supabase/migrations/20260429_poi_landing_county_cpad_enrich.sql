-- Backfill county + CPAD enrichment on poi_landing.
--
-- The original trigger fired the state lookup (public.states.stusps)
-- which doesn't exist on this DB; the load was done with the trigger
-- disabled, leaving 8,041 rows with NULL enrichment. State info already
-- comes from the CSV-address parser (address_state column), so the
-- spatial state lookup is redundant — drop it. Keep county + cpad.

-- ── 1. Replace trigger function (drop state block) ──────────────────

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

  -- County (nationwide; counties has 3,235 across 56 states/territories)
  if NEW.county_geoid is null then
    select c.geoid, c.name
      into NEW.county_geoid, NEW.county_name
      from public.counties c
     where st_contains(c.geom, NEW.geom)
     order by st_area(c.geom) asc
     limit 1;
  end if;

  -- Smallest containing CPAD polygon (CA-only dataset, naturally filters)
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


-- ── 2. Backfill existing 8,041 rows ─────────────────────────────────

with county_pick as (
  select distinct on (l.fid, l.fetched_at)
         l.fid, l.fetched_at, c.geoid, c.name
    from public.poi_landing l
    join public.counties c on st_contains(c.geom, l.geom)
   where l.county_geoid is null and l.geom is not null
   order by l.fid, l.fetched_at, st_area(c.geom) asc
)
update public.poi_landing l
   set county_geoid = cp.geoid,
       county_name  = cp.name
  from county_pick cp
 where l.fid = cp.fid and l.fetched_at = cp.fetched_at;

with cpad_pick as (
  select distinct on (l.fid, l.fetched_at)
         l.fid, l.fetched_at, cu.unit_id, cu.unit_name
    from public.poi_landing l
    join public.cpad_units cu on st_contains(cu.geom, l.geom)
   where l.cpad_unit_id is null and l.geom is not null
     and cu.unit_name is not null and trim(cu.unit_name) <> ''
   order by l.fid, l.fetched_at, st_area(cu.geom) asc
)
update public.poi_landing l
   set cpad_unit_id   = cp.unit_id,
       cpad_unit_name = cp.unit_name
  from cpad_pick cp
 where l.fid = cp.fid and l.fetched_at = cp.fetched_at;


-- ── 3. Re-enable the trigger for future inserts ─────────────────────

alter table public.poi_landing enable trigger trg_poi_landing_enrich;
