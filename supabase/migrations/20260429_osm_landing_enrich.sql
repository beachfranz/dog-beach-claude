-- Enrich osm_landing with CPAD unit + county attribution + dedupe to
-- one row per (type, id). After this:
--   - landing has cpad_unit_id / cpad_unit_name / county_geoid / county_name
--     populated via spatial joins
--   - landing keeps the LATEST row per (type, id); older fetches are
--     dropped (history preserved only via promote() runs into osm_features)
--
-- The CPAD attribution uses smallest-containing-polygon (no name filter
-- — the cleanup of "State of California" / "Wildlife Refuge" etc.
-- happens during promote when picking what to USE as the OSM name).
-- Landing captures the raw spatial fact; promote applies policy.

-- ── 1. Add enrichment columns ────────────────────────────────────────

alter table public.osm_landing
  add column if not exists cpad_unit_id   integer,
  add column if not exists cpad_unit_name text,
  add column if not exists county_geoid   text,
  add column if not exists county_name    text;

create index if not exists osm_landing_cpad_unit_idx
  on public.osm_landing (cpad_unit_id);
create index if not exists osm_landing_county_idx
  on public.osm_landing (county_geoid);

comment on column public.osm_landing.cpad_unit_id is
  'CPAD unit_id of the smallest containing CPAD polygon. Set at landing time via spatial join.';
comment on column public.osm_landing.cpad_unit_name is
  'CPAD unit_name corresponding to cpad_unit_id. Available as a name-borrow source during promote.';
comment on column public.osm_landing.county_geoid is
  'TIGER county GEOID (5-char FIPS) of the containing county. Set at landing time via spatial join.';
comment on column public.osm_landing.county_name is
  'TIGER county name corresponding to county_geoid.';


-- ── 2. Backfill enrichment for existing rows ─────────────────────────
-- Spatial lookup uses osm_features.geom (centroid for all types) since
-- osm_landing's geometry is jsonb — easier to join through features.
-- For new fetches, the fetcher should populate these columns directly.

with row_geom as (
  select l.type, l.id, l.fetched_at, f.geom
    from public.osm_landing l
    join public.osm_features f
      on f.osm_type = l.type and f.osm_id = l.id
),
cpad_pick as (
  select distinct on (rg.type, rg.id, rg.fetched_at)
         rg.type, rg.id, rg.fetched_at,
         cu.unit_id, cu.unit_name
    from row_geom rg
    join public.cpad_units cu on st_contains(cu.geom, rg.geom)
   where cu.unit_name is not null and trim(cu.unit_name) <> ''
   order by rg.type, rg.id, rg.fetched_at, st_area(cu.geom) asc
),
county_pick as (
  select distinct on (rg.type, rg.id, rg.fetched_at)
         rg.type, rg.id, rg.fetched_at,
         c.geoid, c.name
    from row_geom rg
    join public.counties c on st_contains(c.geom, rg.geom)
   where c.state_fp = '06'  -- CA only for now
   order by rg.type, rg.id, rg.fetched_at, st_area(c.geom) asc
)
update public.osm_landing l
   set cpad_unit_id   = cp.unit_id,
       cpad_unit_name = cp.unit_name,
       county_geoid   = co.geoid,
       county_name    = co.name
  from row_geom rg
  left join cpad_pick   cp using (type, id, fetched_at)
  left join county_pick co using (type, id, fetched_at)
 where l.type = rg.type and l.id = rg.id and l.fetched_at = rg.fetched_at;


-- ── 3. Dedupe — keep the LATEST row per (type, id) ───────────────────
-- The PK is (type, id, fetched_at), so duplicates only exist when the
-- same id was landed at multiple times. After dedupe, landing
-- effectively has one canonical row per id.

delete from public.osm_landing l
 using public.osm_landing l2
 where l.type = l2.type
   and l.id   = l2.id
   and l2.fetched_at > l.fetched_at;
