-- Wraps the existing one-shot 20260427_osm_features_reborrow_names
-- migration as a callable function so it can be invoked from Dagster
-- whenever new OSM features land.
--
-- Same logic as the original:
--   Pass 1: nearest UBP within 200m -> name (name_source='us_beach_points')
--   Pass 2: nearest CCC within 200m, only if CCC.name has beach-y word
--           -> name (name_source='ccc')
--
-- Idempotent: every run resets prior borrows ('us_beach_points', 'ccc')
-- before re-picking. Original-OSM names (name_source='osm') and manual
-- overrides are never touched.

create or replace function public.reborrow_osm_feature_names()
returns jsonb
language plpgsql
security definer
as $function$
declare
  v_reset    int := 0;
  v_from_ubp int := 0;
  v_from_ccc int := 0;
  v_still_unnamed int := 0;
begin
  -- Reset prior borrows so we re-pick from scratch.
  update public.osm_features
     set name = null, name_source = null
   where feature_type in ('beach','dog_friendly_beach')
     and name_source in ('ccc','us_beach_points');
  get diagnostics v_reset = row_count;

  -- Pass 1: UBP first.
  with sub as (
    select t.osm_type, t.osm_id, u.name as borrowed
    from public.osm_features t
    cross join lateral (
      select u.name, u.geom
      from public.us_beach_points u
      where u.state = 'CA'
        and u.name is not null and trim(u.name) <> ''
        and st_dwithin(t.geom::geography, u.geom::geography, 200)
      order by t.geom <-> u.geom
      limit 1
    ) u
    where t.feature_type in ('beach','dog_friendly_beach')
      and (t.name is null or trim(t.name) = '')
  )
  update public.osm_features t
     set name = sub.borrowed,
         name_source = 'us_beach_points'
    from sub
   where t.osm_type = sub.osm_type and t.osm_id = sub.osm_id;
  get diagnostics v_from_ubp = row_count;

  -- Pass 2: CCC fallback when its name is beach-y.
  with sub as (
    select t.osm_type, t.osm_id, c.name as borrowed
    from public.osm_features t
    cross join lateral (
      select c.name, c.geom
      from public.ccc_access_points c
      where c.name is not null and trim(c.name) <> ''
        and c.name ~* '\m(beach|cove|shore|sand)'
        and st_dwithin(t.geom::geography, c.geom::geography, 200)
      order by t.geom <-> c.geom
      limit 1
    ) c
    where t.feature_type in ('beach','dog_friendly_beach')
      and (t.name is null or trim(t.name) = '')
  )
  update public.osm_features t
     set name = sub.borrowed,
         name_source = 'ccc'
    from sub
   where t.osm_type = sub.osm_type and t.osm_id = sub.osm_id;
  get diagnostics v_from_ccc = row_count;

  select count(*) into v_still_unnamed
    from public.osm_features
   where feature_type in ('beach','dog_friendly_beach')
     and (name is null or trim(name) = '');

  return jsonb_build_object(
    'reset_prior_borrows', v_reset,
    'borrowed_from_ubp',   v_from_ubp,
    'borrowed_from_ccc',   v_from_ccc,
    'still_unnamed',       v_still_unnamed
  );
end;
$function$;

comment on function public.reborrow_osm_feature_names() is
  'Re-runs the OSM beach name-borrow cascade (UBP within 200m, then beach-y CCC within 200m). Idempotent. Returns counts. Wrapped by Dagster asset osm_reborrow_names_run.';
