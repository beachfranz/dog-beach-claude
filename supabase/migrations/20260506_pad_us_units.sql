-- 20260506_pad_us_units.sql
--
-- Federal-level analog to cpad_units. Loaded per-state from the
-- Esri Living Atlas REST FeatureServer
-- (services.arcgis.com/v01gqwM5QqNysAAi/.../Manager_Name/FeatureServer/0).
--
-- Closes the inland-orphan coverage gap for the catalog ingest +
-- dog-policy cascade: USFS / BLM / USFWS / private-dam reservoirs that
-- aren't in CPAD's CA-curated subset show up in PAD-US.

begin;

create table if not exists public.pad_us_units (
  unit_id      bigint primary key,        -- maps from PAD-US OBJECTID
  feat_class   text,                       -- Fee / Easement / Designation
  category     text,
  own_type     text,
  own_name     text,                       -- owner (CPAD analog: agncy_name)
  loc_own      text,
  mng_type     text,
  mng_name     text,                       -- manager (CPAD analog: mng_agncy)
  loc_mang     text,
  des_tp       text,                       -- designation type
  loc_ds       text,
  unit_name    text,
  loc_name     text,
  state        text,                       -- 2-char state code
  agg_src      text,                       -- source aggregator
  raw_attrs    jsonb,                      -- catch-all
  geom         geometry(MultiPolygon, 4326),
  loaded_at    timestamptz default now()
);

create index if not exists pad_us_units_geom_idx
  on public.pad_us_units using gist (geom);
create index if not exists pad_us_units_state_idx
  on public.pad_us_units (state);
create index if not exists pad_us_units_mng_name_idx
  on public.pad_us_units (mng_name);
create index if not exists pad_us_units_own_name_idx
  on public.pad_us_units (own_name);

commit;
