-- public.beaches_gold — cross-state canonical beach inventory.
--
-- Receives rows after each per-state arena run. arena is per-state and
-- transient (truncate-and-rebuild per state); beaches_gold is the long
-- table that accumulates all states.
--
-- Round 1 scope: ALL active arena heads (fid=group_id, source_code IN
-- ('osm','poi'), is_active=true). Arena's existing dedup (18 inactive
-- reasons) has already collapsed POI<->OSM matches; surviving POI heads
-- are deliberately distinct beaches not represented by an OSM polygon.
-- Pure-OSM filter excluded ~17 high-value curated dog beaches (Coronado,
-- HB Dog, Rosie's, Refugio, Leo Carrillo, etc.) that are POI-only.
--
-- fid passthrough: beaches_gold.fid IS arena.fid (no translation). This
-- lets beach_policy_extractions / beach_policy_gold_set re-target their
-- FKs to beaches_gold.fid without rewriting any keys.
--
-- This migration is SCHEMA ONLY. Population is done by
-- scripts/promote_arena_to_beaches_gold.py per-state.

create table if not exists public.beaches_gold (
  fid             bigint primary key,        -- == arena.fid for the head row
  name            text,
  address         text,
  lat             double precision,
  lon             double precision,
  county_fips     text,
  county_name     text,
  source_code     text not null,             -- 'osm' for now; may include 'poi' later
  source_id       text,                      -- e.g. 'osm/relation/637634'
  cpad_unit_id    integer,                   -- only meaningful for CA
  geom            geometry(Point, 4326),
  group_id        bigint,                    -- arena.group_id (= fid for heads, but kept for traceability)
  nav_lat         double precision,          -- point-on-surface for OSM polys
  nav_lon         double precision,
  nav_source      text,
  name_source     text,
  park_name       text,                      -- CPAD parent name where applicable
  state           text not null,             -- hardcoded per populator run; 'CA','OR',etc.
  promoted_from   text not null default 'osm_only_v1',
  promoted_at     timestamptz not null default now(),
  is_active       boolean not null default true,
  inactive_reason text
);

create index if not exists beaches_gold_state_idx       on public.beaches_gold (state);
create index if not exists beaches_gold_county_idx      on public.beaches_gold (county_fips);
create index if not exists beaches_gold_source_idx      on public.beaches_gold (source_code, source_id);
create index if not exists beaches_gold_group_idx       on public.beaches_gold (group_id);
create index if not exists beaches_gold_geom_gist_idx   on public.beaches_gold using gist (geom);
create index if not exists beaches_gold_name_trgm_idx   on public.beaches_gold using gin (name gin_trgm_ops);

comment on table public.beaches_gold is
  'Cross-state canonical beach inventory. Receives rows after each per-state arena run via scripts/promote_arena_to_beaches_gold.py. fid passes through from arena.fid. Round 1 scope: OSM-anchored heads only.';

-- Auto-fill geom from lat/lon if caller doesn't provide it (mirrors arena).
create or replace function public._beaches_gold_set_geom()
returns trigger
language plpgsql
as $function$
begin
  if NEW.geom is null and NEW.lat is not null and NEW.lon is not null then
    NEW.geom := ST_SetSRID(ST_MakePoint(NEW.lon, NEW.lat), 4326);
  end if;
  return NEW;
end;
$function$;

drop trigger if exists trg_beaches_gold_set_geom on public.beaches_gold;
create trigger trg_beaches_gold_set_geom
  before insert or update on public.beaches_gold
  for each row execute function public._beaches_gold_set_geom();
