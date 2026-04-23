-- Replace the per-row plpgsql loop in load_cpad_batch with a single
-- set-based INSERT. The original version looped 500 times calling
-- ST_GeomFromGeoJSON on each feature, which was fast for small polygons
-- but timed out on state-park-sized multipolygons. A single INSERT ...
-- SELECT ... FROM jsonb_array_elements parses the whole batch once.

create or replace function public.load_cpad_batch(p_features jsonb)
returns jsonb
language sql
security definer
as $$
  with candidates as (
    select
      (f->'properties'->>'OBJECTID')::int               as objectid,
      nullif(f->'properties'->>'HOLDING_ID','')::int    as holding_id,
       f->'properties'->>'ACCESS_TYP'                   as access_typ,
      nullif(f->'properties'->>'UNIT_ID','')::int       as unit_id,
       f->'properties'->>'UNIT_NAME'                    as unit_name,
      nullif(f->'properties'->>'SUID_NMA','')::int      as suid_nma,
      nullif(f->'properties'->>'AGNCY_ID','')::int      as agncy_id,
       f->'properties'->>'AGNCY_NAME'                   as agncy_name,
       f->'properties'->>'AGNCY_LEV'                    as agncy_lev,
       f->'properties'->>'AGNCY_TYP'                    as agncy_typ,
       f->'properties'->>'AGNCY_WEB'                    as agncy_web,
       f->'properties'->>'LAYER'                        as layer,
      nullif(f->'properties'->>'MNG_AG_ID','')::numeric as mng_ag_id,
       f->'properties'->>'MNG_AGNCY'                    as mng_agncy,
       f->'properties'->>'MNG_AG_LEV'                   as mng_ag_lev,
       f->'properties'->>'MNG_AG_TYP'                   as mng_ag_typ,
       f->'properties'->>'SITE_NAME'                    as site_name,
       f->'properties'->>'ALT_SITE_N'                   as alt_site_n,
       f->'properties'->>'PARK_URL'                     as park_url,
       f->'properties'->>'LAND_WATER'                   as land_water,
       f->'properties'->>'SPEC_USE'                     as spec_use,
       f->'properties'->>'CITY'                         as city,
       f->'properties'->>'COUNTY'                       as county,
      nullif(f->'properties'->>'ACRES','')::numeric     as acres,
       f->'properties'->>'LABEL_NAME'                   as label_name,
       to_timestamp(nullif(f->'properties'->>'DATE_REVIS','')::bigint / 1000.0) as date_revis,
       f->'properties'->>'SRC_ATTR'                     as src_attr,
       f->'properties'->>'SRC_ALIGN'                    as src_align,
      nullif(f->'properties'->>'YR_PROTECT','')::int    as yr_protect,
      nullif(f->'properties'->>'YR_EST','')::int        as yr_est,
       f->'properties'->>'GAP_Source'                   as gap_source,
       ST_Multi(
         ST_CollectionExtract(
           ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON((f->'geometry')::text), 4326)),
           3  -- 3 = polygons only; drops stray lines/points that ST_MakeValid may emit
         )
       ) as geom
    from jsonb_array_elements(p_features) as f
    where (f->'properties'->>'OBJECTID') is not null
      and (f->'geometry') is not null
  ),
  upserted as (
    insert into public.cpad_units as c (
      objectid, holding_id, access_typ, unit_id, unit_name, suid_nma,
      agncy_id, agncy_name, agncy_lev, agncy_typ, agncy_web, layer,
      mng_ag_id, mng_agncy, mng_ag_lev, mng_ag_typ,
      site_name, alt_site_n, park_url, land_water, spec_use,
      city, county, acres, label_name,
      date_revis, src_attr, src_align, yr_protect, yr_est, gap_source,
      geom
    )
    select * from candidates
    on conflict (objectid) do update set
      holding_id = excluded.holding_id, access_typ = excluded.access_typ,
      unit_id    = excluded.unit_id,    unit_name  = excluded.unit_name,
      suid_nma   = excluded.suid_nma,   agncy_id   = excluded.agncy_id,
      agncy_name = excluded.agncy_name, agncy_lev  = excluded.agncy_lev,
      agncy_typ  = excluded.agncy_typ,  agncy_web  = excluded.agncy_web,
      layer      = excluded.layer,      mng_ag_id  = excluded.mng_ag_id,
      mng_agncy  = excluded.mng_agncy,  mng_ag_lev = excluded.mng_ag_lev,
      mng_ag_typ = excluded.mng_ag_typ, site_name  = excluded.site_name,
      alt_site_n = excluded.alt_site_n, park_url   = excluded.park_url,
      land_water = excluded.land_water, spec_use   = excluded.spec_use,
      city       = excluded.city,       county     = excluded.county,
      acres      = excluded.acres,      label_name = excluded.label_name,
      date_revis = excluded.date_revis, src_attr   = excluded.src_attr,
      src_align  = excluded.src_align,  yr_protect = excluded.yr_protect,
      yr_est     = excluded.yr_est,     gap_source = excluded.gap_source,
      geom       = excluded.geom,       loaded_at  = now()
    returning 1
  )
  select jsonb_build_object(
    'total',    jsonb_array_length(p_features),
    'affected', (select count(*)::int from upserted),
    'skipped',  jsonb_array_length(p_features) - (select count(*)::int from candidates)
  );
$$;

revoke all on function public.load_cpad_batch(jsonb) from public, anon, authenticated;
grant  execute on function public.load_cpad_batch(jsonb) to service_role;