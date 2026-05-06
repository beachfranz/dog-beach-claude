-- 20260506_wildlife_critical_habitat.sql
--
-- Canonical USFWS critical habitat. National coverage, all federally
-- listed species. Source-of-truth analog to cpad_units + pad_us_units.
--
-- Source: services.arcgis.com/QVENGdaPbd4LUkLV/.../USFWS_Critical_Habitat/FeatureServer/0
-- Loaded by scripts/external_sources.py via the Tier 1 loader registry.
--
-- Note: dog_policy_zones already has a WSP row from last night's
-- ad-hoc load. Keep that for now; a downstream projector can keep
-- dog_policy_zones in sync from this canonical table once we wire it.

begin;

create table if not exists public.wildlife_critical_habitat (
  unit_id      bigint primary key,        -- maps from USFWS OBJECTID
  comname      text,                      -- common name
  sciname      text,                      -- scientific name
  spcode       text,                      -- USFWS species code
  vipcode      text,
  unit         text,                      -- unit ID
  subunit      text,
  unitname     text,
  subunitname  text,
  status       text,                      -- 'Final' / 'Proposed'
  pubdate      timestamptz,
  effectdate   timestamptz,
  source_url   text,
  raw_attrs    jsonb,
  geom         geometry(MultiPolygon, 4326),
  loaded_at    timestamptz default now()
);

create index if not exists wch_geom_idx     on public.wildlife_critical_habitat using gist (geom);
create index if not exists wch_sciname_idx  on public.wildlife_critical_habitat (sciname);
create index if not exists wch_comname_idx  on public.wildlife_critical_habitat (comname);
create index if not exists wch_status_idx   on public.wildlife_critical_habitat (status);

commit;
