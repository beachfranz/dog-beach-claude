-- California Protected Areas Database (CPAD) agency-level units,
-- mirrored locally into PostGIS so the jurisdiction classifier can do
-- a single ST_DWithin join instead of per-beach ArcGIS round-trips.
--
-- Populated by admin-load-cpad (paginated loader driven from scripts/
-- load_cpad.py). Source-of-truth is still the CNRA ArcGIS FeatureServer;
-- we just cache it here for speed and to get access to the richer field
-- set (AGNCY_* = owner, MNG_* = operator, PARK_URL, ACCESS_TYP, etc.)
-- that we weren't extracting through the per-row query path.

create table if not exists public.cpad_units (
  objectid      int primary key,
  holding_id    int,
  access_typ    text,
  unit_id       int,
  unit_name     text,
  suid_nma      int,
  agncy_id      int,
  agncy_name    text,
  agncy_lev     text,
  agncy_typ     text,
  agncy_web     text,
  layer         text,
  mng_ag_id     numeric,
  mng_agncy     text,
  mng_ag_lev    text,
  mng_ag_typ    text,
  site_name     text,
  alt_site_n    text,
  park_url      text,
  land_water    text,
  spec_use      text,
  city          text,
  county        text,
  acres         numeric,
  label_name    text,
  date_revis    timestamptz,
  src_attr      text,
  src_align     text,
  yr_protect    int,
  yr_est        int,
  gap_source    text,
  geom          geometry(MultiPolygon, 4326) not null,
  loaded_at     timestamptz not null default now()
);

-- Common access patterns
create index if not exists cpad_units_geom_gix       on public.cpad_units using gist(geom);
create index if not exists cpad_units_agncy_lev_idx  on public.cpad_units (agncy_lev);
create index if not exists cpad_units_mng_ag_lev_idx on public.cpad_units (mng_ag_lev);
create index if not exists cpad_units_county_idx     on public.cpad_units (county);
create index if not exists cpad_units_unit_name_idx  on public.cpad_units (unit_name);

alter table public.cpad_units enable row level security;

-- Batch-insert RPC used by admin-load-cpad-batch edge function. Takes a
-- GeoJSON FeatureCollection-shaped jsonb and upserts each feature.
-- ArcGIS returns geometry as Esri JSON by default; we request f=geojson
-- so the geometry comes back as standard GeoJSON and ST_GeomFromGeoJSON
-- accepts it directly.
create or replace function public.load_cpad_batch(p_features jsonb)
returns jsonb
language plpgsql
security definer
as $$
declare
  v_inserted int := 0;
  v_updated  int := 0;
  v_skipped  int := 0;
  v_total    int := jsonb_array_length(p_features);
  v_feature  jsonb;
  v_attrs    jsonb;
  v_geom     jsonb;
  v_existed  boolean;
begin
  for v_feature in select value from jsonb_array_elements(p_features)
  loop
    v_attrs := v_feature->'properties';
    v_geom  := v_feature->'geometry';
    if v_attrs is null or v_geom is null or v_attrs->>'OBJECTID' is null then
      v_skipped := v_skipped + 1;
      continue;
    end if;

    select true into v_existed from public.cpad_units where objectid = (v_attrs->>'OBJECTID')::int;

    insert into public.cpad_units as c (
      objectid, holding_id, access_typ, unit_id, unit_name, suid_nma,
      agncy_id, agncy_name, agncy_lev, agncy_typ, agncy_web, layer,
      mng_ag_id, mng_agncy, mng_ag_lev, mng_ag_typ,
      site_name, alt_site_n, park_url, land_water, spec_use,
      city, county, acres, label_name,
      date_revis, src_attr, src_align, yr_protect, yr_est, gap_source,
      geom
    ) values (
      (v_attrs->>'OBJECTID')::int,
      nullif(v_attrs->>'HOLDING_ID','')::int,
      v_attrs->>'ACCESS_TYP',
      nullif(v_attrs->>'UNIT_ID','')::int,
      v_attrs->>'UNIT_NAME',
      nullif(v_attrs->>'SUID_NMA','')::int,
      nullif(v_attrs->>'AGNCY_ID','')::int,
      v_attrs->>'AGNCY_NAME',
      v_attrs->>'AGNCY_LEV',
      v_attrs->>'AGNCY_TYP',
      v_attrs->>'AGNCY_WEB',
      v_attrs->>'LAYER',
      nullif(v_attrs->>'MNG_AG_ID','')::numeric,
      v_attrs->>'MNG_AGNCY',
      v_attrs->>'MNG_AG_LEV',
      v_attrs->>'MNG_AG_TYP',
      v_attrs->>'SITE_NAME',
      v_attrs->>'ALT_SITE_N',
      v_attrs->>'PARK_URL',
      v_attrs->>'LAND_WATER',
      v_attrs->>'SPEC_USE',
      v_attrs->>'CITY',
      v_attrs->>'COUNTY',
      nullif(v_attrs->>'ACRES','')::numeric,
      v_attrs->>'LABEL_NAME',
      to_timestamp(nullif(v_attrs->>'DATE_REVIS','')::bigint / 1000.0),
      v_attrs->>'SRC_ATTR',
      v_attrs->>'SRC_ALIGN',
      nullif(v_attrs->>'YR_PROTECT','')::int,
      nullif(v_attrs->>'YR_EST','')::int,
      v_attrs->>'GAP_Source',
      ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(v_geom::text), 4326))
    )
    on conflict (objectid) do update set
      holding_id = excluded.holding_id,
      access_typ = excluded.access_typ,
      unit_id    = excluded.unit_id,
      unit_name  = excluded.unit_name,
      suid_nma   = excluded.suid_nma,
      agncy_id   = excluded.agncy_id,
      agncy_name = excluded.agncy_name,
      agncy_lev  = excluded.agncy_lev,
      agncy_typ  = excluded.agncy_typ,
      agncy_web  = excluded.agncy_web,
      layer      = excluded.layer,
      mng_ag_id  = excluded.mng_ag_id,
      mng_agncy  = excluded.mng_agncy,
      mng_ag_lev = excluded.mng_ag_lev,
      mng_ag_typ = excluded.mng_ag_typ,
      site_name  = excluded.site_name,
      alt_site_n = excluded.alt_site_n,
      park_url   = excluded.park_url,
      land_water = excluded.land_water,
      spec_use   = excluded.spec_use,
      city       = excluded.city,
      county     = excluded.county,
      acres      = excluded.acres,
      label_name = excluded.label_name,
      date_revis = excluded.date_revis,
      src_attr   = excluded.src_attr,
      src_align  = excluded.src_align,
      yr_protect = excluded.yr_protect,
      yr_est     = excluded.yr_est,
      gap_source = excluded.gap_source,
      geom       = excluded.geom,
      loaded_at  = now();

    if coalesce(v_existed, false) then
      v_updated := v_updated + 1;
    else
      v_inserted := v_inserted + 1;
    end if;
  end loop;

  return jsonb_build_object(
    'total',    v_total,
    'inserted', v_inserted,
    'updated',  v_updated,
    'skipped',  v_skipped
  );
end;
$$;

revoke all on function public.load_cpad_batch(jsonb) from public, anon, authenticated;
grant  execute on function public.load_cpad_batch(jsonb) to service_role;
