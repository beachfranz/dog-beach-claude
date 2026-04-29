-- Rewire all_coastal_features_lite to read dogs_verdict from
-- beach_verdicts via origin_key. Eliminates the CCC-association
-- pathway that the OSM branch was using to inherit verdict, and
-- replaces ccc_access_points.dogs_verdict reads with beach_verdicts
-- joins for the CCC and UBP branches.
--
-- Pre-requisite: 20260428_verdict_pass_8_rekey_off_ccc.sql must run
-- first to populate beach_verdicts.
--
-- After this migration, ccc_access_points.dogs_verdict is no longer
-- read by any active display path (the CCC-keyed
-- compute_dogs_verdict shim still updates it for any direct
-- legacy callers; column will be dropped once those are gone).

drop function if exists public.all_coastal_features_lite();
create or replace function public.all_coastal_features_lite(
  p_counties text[] default null
)
returns table (
  layer              text,
  origin_key         text,
  name               text,
  feature_type       text,
  origin_source      text,
  operator_canonical text,
  dogs_verdict       text,
  description        text,
  lat                float8,
  lng                float8
)
language sql stable security definer
as $$
  -- OSM beach polygons. dogs_verdict via beach_verdicts.
  select 'beach'::text,
         'osm/' || o.osm_type || '/' || o.osm_id::text as origin_key,
         o.name, o.feature_type, 'osm'::text,
         op.canonical_name,
         bv.dogs_verdict,
         null::text,
         st_y(o.geom), st_x(o.geom)
  from public.osm_features o
  left join public.operators op on op.id = o.operator_id
  left join public.beach_verdicts bv
    on bv.origin_key = 'osm/' || o.osm_type || '/' || o.osm_id::text
  where o.feature_type in ('beach','dog_friendly_beach')
    and (o.admin_inactive is null or o.admin_inactive = false)
    and (p_counties is null or o.county_name_tiger = any(p_counties))

  union all

  -- UBP-CA. dogs_verdict via beach_verdicts.
  select 'beach',
         'ubp/' || u.fid::text as origin_key,
         u.name, 'beach', 'ubp',
         op.canonical_name,
         bv.dogs_verdict,
         null::text,
         st_y(u.geom), st_x(u.geom)
  from public.us_beach_points u
  left join public.operators op on op.id = u.operator_id
  left join public.beach_verdicts bv on bv.origin_key = 'ubp/' || u.fid::text
  where u.state = 'CA'
    and (u.admin_inactive is null or u.admin_inactive = false)
    and (p_counties is null or u.county_name_tiger = any(p_counties))

  union all

  -- CCC: split into beach vs access via inferred_type. Verdict via
  -- beach_verdicts (NOT ccc_access_points.dogs_verdict, which is now
  -- legacy storage).
  select case when coalesce(c.inferred_type, '') in ('beach','named_beach') then 'beach'
              else 'access' end,
         'ccc/' || c.objectid::text as origin_key,
         c.name,
         coalesce(c.inferred_type, 'unknown'),
         'ccc',
         op.canonical_name,
         bv.dogs_verdict,
         c.description,
         st_y(c.geom), st_x(c.geom)
  from public.ccc_access_points c
  left join public.operators op on op.id = c.operator_id
  left join public.beach_verdicts bv on bv.origin_key = 'ccc/' || c.objectid::text
  where (c.archived is null or c.archived <> 'Yes')
    and (c.admin_inactive is null or c.admin_inactive = false)
    and c.latitude is not null
    and (p_counties is null or c.county_name_tiger = any(p_counties));
$$;

grant execute on function public.all_coastal_features_lite(text[]) to anon, authenticated;
