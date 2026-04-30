-- public.arena — master location list, consolidated from landing tables.
--
-- Built by matching osm_landing / poi_landing / ccc_landing on
-- spatial proximity, name similarity, county FIPS, and CPAD unit_id.
-- One row per "real-world location"; multiple landing rows can resolve
-- to the same arena row (the matcher records this in arena_source_link
-- — added later when the matching step is built).
--
-- For now: schema only. No populate. Matching logic is the next step.

create table public.arena (
  fid           bigserial primary key,    -- master/canonical ID
  name          text,
  address       text,
  lat           double precision,
  lon           double precision,
  county_fips   text,                     -- 5-char GEOID
  county_name   text,
  source_code   text,                     -- 'osm' | 'poi' | 'ccc' (and later 'cpad', 'manual', etc.)
  source_id     text,                     -- source-specific composite key (e.g. 'osm/way/12345', 'ccc/1587', 'poi/8041')
  cpad_unit_id  integer,                  -- CPAD unit_id of the smallest containing polygon
  geom          geometry(Point, 4326),    -- derived from lat/lon for spatial joins
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now()
);

-- Spatial + matching-friendly indexes
create index arena_geom_gist_idx        on public.arena using gist (geom);
create index arena_county_fips_idx      on public.arena (county_fips);
create index arena_cpad_unit_id_idx     on public.arena (cpad_unit_id);
create index arena_source_idx           on public.arena (source_code, source_id);
create index arena_name_trgm_idx        on public.arena using gin (name gin_trgm_ops);


-- Auto-fill geom from lat/lon if caller doesn't provide it.
-- Auto-update updated_at on UPDATE.
create or replace function public._arena_set_geom_and_updated_at()
returns trigger
language plpgsql
as $function$
begin
  if NEW.geom is null and NEW.lat is not null and NEW.lon is not null then
    NEW.geom := ST_SetSRID(ST_MakePoint(NEW.lon, NEW.lat), 4326);
  end if;
  if TG_OP = 'UPDATE' then
    NEW.updated_at := now();
  end if;
  return NEW;
end;
$function$;

drop trigger if exists trg_arena_set_geom on public.arena;
create trigger trg_arena_set_geom
  before insert or update on public.arena
  for each row execute function public._arena_set_geom_and_updated_at();


comment on table public.arena is
  'Master location list consolidated from osm_landing / poi_landing / ccc_landing. Matching strategy: spatial proximity + name similarity + county_fips + cpad_unit_id. fid is the canonical/master id; source_code + source_id identify the originating landing record.';
comment on column public.arena.fid is
  'Master canonical ID (bigserial). Stable identifier for this real-world location across runs.';
comment on column public.arena.source_id is
  'Source-specific composite key (e.g., osm/way/12345, ccc/1587, poi/8041). Multiple source records may resolve to the same fid; this column records the FIRST contributor or the canonical pick depending on matcher logic.';
