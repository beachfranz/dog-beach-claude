-- Statewide-bird's-eye RPC: every beach + every access feature, but
-- centroid-only (no geom_full polygons) to keep payload small enough
-- for anon's 8s statement timeout. Used by the all-mode of the
-- per-beach coastal map page.

-- Query source tables directly. The views' correlated LATERAL JOINs
-- (address backfill) and NOT EXISTS dedupe make a statewide pull
-- exceed anon's 8s statement timeout. Show-everything semantics
-- doesn't need the dedupe — duplicates between OSM/UBP/CCC are
-- expected and useful for visualization.
drop function if exists public.all_coastal_features_lite();
create or replace function public.all_coastal_features_lite(
  p_counties text[] default null  -- null = no filter (statewide)
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
  -- OSM beach polygons (centroids only). dogs_verdict derived from
  -- associated CCC via feature_associations(same_beach): any 'yes'
  -- among voting CCCs wins, all 'no' = 'no', otherwise null.
  select 'beach'::text,
         'osm/' || o.osm_type || '/' || o.osm_id::text,
         o.name, o.feature_type, 'osm'::text,
         op.canonical_name,
         (select case
            when bool_or(c.dogs_verdict = 'yes') then 'yes'
            when bool_and(c.dogs_verdict = 'no')  then 'no'
            else null
          end
          from public.feature_associations fa
          join public.ccc_access_points c on c.objectid::text = fa.a_id
          where fa.a_source = 'ccc' and fa.b_source = 'osm'
            and fa.relationship = 'same_beach'
            and fa.b_id = o.osm_type || '/' || o.osm_id::text
            and c.dogs_verdict in ('yes','no')) as dogs_verdict,
         null::text,
         st_y(o.geom), st_x(o.geom)
  from public.osm_features o
  left join public.operators op on op.id = o.operator_id
  where o.feature_type in ('beach','dog_friendly_beach')
    and (o.admin_inactive is null or o.admin_inactive = false)
    and (p_counties is null or o.county_name_tiger = any(p_counties))

  union all

  -- UBP-CA (all). UBP has no native dog signal — leave dogs_verdict
  -- null until UBP↔CCC associations exist.
  select 'beach',
         'ubp/' || u.fid::text,
         u.name, 'beach', 'ubp',
         op.canonical_name, null::text, null::text,
         st_y(u.geom), st_x(u.geom)
  from public.us_beach_points u
  left join public.operators op on op.id = u.operator_id
  where u.state = 'CA'
    and (u.admin_inactive is null or u.admin_inactive = false)
    and (p_counties is null or u.county_name_tiger = any(p_counties))

  union all

  -- CCC: split into beach vs access via inferred_type
  select case when coalesce(c.inferred_type, '') in ('beach','named_beach') then 'beach'
              else 'access' end,
         'ccc/' || c.objectid::text,
         c.name,
         coalesce(c.inferred_type, 'unknown'),
         'ccc',
         op.canonical_name,
         c.dogs_verdict,
         c.description,
         st_y(c.geom), st_x(c.geom)
  from public.ccc_access_points c
  left join public.operators op on op.id = c.operator_id
  where (c.archived is null or c.archived <> 'Yes')
    and (c.admin_inactive is null or c.admin_inactive = false)
    and c.latitude is not null
    and (p_counties is null or c.county_name_tiger = any(p_counties));
$$;

grant execute on function public.all_coastal_features_lite(text[]) to anon, authenticated;
