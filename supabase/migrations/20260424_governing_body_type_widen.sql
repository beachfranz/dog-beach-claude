-- Widen governing_body_type enum on us_beach_points_staging (2026-04-24)
-- After studying CPAD mng_ag_lev value distribution: 3,541 polygons (20%
-- of CPAD) fall into Special District / Non Profit / Joint — all
-- currently unmappable to our 7-value enum. Add three values.
--
-- Source distribution observed in cpad_units.mng_ag_lev:
--   Special District (2,354) — water districts, port districts,
--                              recreation/parks districts, etc.
--   Non Profit       (1,098) — land trusts, conservation orgs
--   Joint            (   89) — joint powers authorities
--
-- Without these, the pipeline collapses these into 'unknown', losing
-- meaningful classification.

alter table public.us_beach_points_staging
  drop constraint if exists us_beach_points_staging_governing_body_type_check;

alter table public.us_beach_points_staging
  add constraint us_beach_points_staging_governing_body_type_check
  check (governing_body_type is null or governing_body_type in (
    'city',
    'county',
    'state',
    'federal',
    'tribal',
    'private',
    'special_district',
    'nonprofit',
    'joint',
    'unknown'
  ));

comment on column public.us_beach_points_staging.governing_body_type is
  'Operator type. 10 values: city/county/state/federal/tribal/private + special_district (water/port/parks districts), nonprofit (land trusts), joint (JPAs), unknown. Maps from CPAD mng_ag_lev directly.';
