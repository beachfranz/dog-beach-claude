-- POI landing table — raw US_beaches.csv records.
--
-- Mirrors osm_landing: CSV-shaped + landing metadata + spatial enrichment.
-- Schema follows the actual US_beaches.csv columns exactly (WKT, fid,
-- COUNTRY, NAME, ADDR1..5, CAT_MOD). Enrichment fields (state, county,
-- cpad) are filled at INSERT time by trigger.
--
-- After this:
--   - new CSV imports write here (loader script refactor pending)
--   - downstream us_beach_points consumes via promote (pending)

create table public.poi_landing (
  fetched_at   timestamptz not null default now(),
  fetched_by   text,
  fid          integer not null,
  raw_wkt      text,
  name         text,
  country      text,
  addr1        text,
  addr2        text,
  addr3        text,
  addr4        text,
  addr5        text,
  cat_mod      text,
  geom         geometry(Point, 4326),
  -- Enrichment (auto-filled by trigger)
  state        text,
  county_geoid text,
  county_name  text,
  cpad_unit_id integer,
  cpad_unit_name text,
  primary key (fid, fetched_at)
);
create index poi_landing_fid_idx        on public.poi_landing (fid);
create index poi_landing_fetched_at_idx on public.poi_landing (fetched_at desc);
create index poi_landing_geom_idx       on public.poi_landing using gist (geom);
create index poi_landing_cat_mod_idx    on public.poi_landing (cat_mod);
create index poi_landing_state_idx      on public.poi_landing (state);
create index poi_landing_county_idx     on public.poi_landing (county_geoid);
create index poi_landing_cpad_idx       on public.poi_landing (cpad_unit_id);

comment on table public.poi_landing is
  'Raw POI records from share/Dog_Beaches/US_beaches.csv. Schema mirrors the CSV: WKT/fid/COUNTRY/NAME/ADDR1..5/CAT_MOD. Geom parsed from WKT. Enrichment columns (state, county, cpad) filled at INSERT via trigger. Downstream us_beach_points consumes via promote.';


-- ── Auto-enrichment trigger ─────────────────────────────────────────

create or replace function public._poi_landing_enrich_trigger()
returns trigger
language plpgsql
security definer
as $function$
begin
  -- Parse geom from raw_wkt if not provided
  if NEW.geom is null and NEW.raw_wkt is not null then
    begin
      NEW.geom := ST_SetSRID(ST_GeomFromText(NEW.raw_wkt), 4326);
    exception when others then
      NEW.geom := null;
    end;
  end if;
  if NEW.geom is null then return NEW; end if;

  -- State (smallest containing US state polygon, fast lookup)
  if NEW.state is null then
    select s.stusps into NEW.state
      from public.states s
     where st_contains(s.geom, NEW.geom)
     limit 1;
  end if;

  -- County (CA only — matches us_beach_points convention; widen later)
  if NEW.county_geoid is null then
    select c.geoid, c.name
      into NEW.county_geoid, NEW.county_name
      from public.counties c
     where st_contains(c.geom, NEW.geom)
     order by st_area(c.geom) asc
     limit 1;
  end if;

  -- Smallest containing CPAD polygon
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

drop trigger if exists trg_poi_landing_enrich on public.poi_landing;
create trigger trg_poi_landing_enrich
  before insert on public.poi_landing
  for each row execute function public._poi_landing_enrich_trigger();


-- ── Backfill from current us_beach_points ───────────────────────────
-- Synthesizes one landing row per existing us_beach_points row using
-- whatever the table has (raw_wkt, name, addresses, cat_mod, state).

insert into public.poi_landing
  (fetched_at, fetched_by, fid, raw_wkt, name, country,
   addr1, addr2, addr3, addr4, addr5, cat_mod, geom,
   state, county_geoid, county_name)
select
  coalesce(loaded_at, '2026-04-23'::timestamptz) as fetched_at,
  'backfill_from_us_beach_points' as fetched_by,
  fid,
  raw_wkt,
  name,
  country,
  addr1, addr2, addr3, addr4, addr5,
  cat_mod,
  geom,
  state,
  county_fips_tiger as county_geoid,
  county_name_tiger as county_name
  from public.us_beach_points
on conflict (fid, fetched_at) do nothing;
