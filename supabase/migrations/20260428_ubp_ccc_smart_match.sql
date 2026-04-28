-- UBP↔CCC smart match: pair UBP CA records with CCC access points
-- via cleaned-name + 5km KNN, recording the relationship in
-- feature_associations(a_source='ubp', b_source='ccc',
-- relationship='same_beach').
--
-- Mirrors the CCC↔OSM smart match shipped earlier — same threshold
-- (clean_sim ≥ 0.7) and same cleaning function. ~343 pairs expected.
-- Each pair lets a UBP beach inherit dogs_verdict from its CCC
-- partner via the lite RPC's bool_or aggregation.

insert into public.feature_associations
  (a_source, a_id, b_source, b_id, relationship, note)
select 'ubp',
       m.fid::text,
       'ccc',
       m.ccc_id::text,
       'same_beach',
       format('ubp_ccc_smart_match clean_sim=%s dist_m=%s',
              round(m.clean_sim::numeric, 2),
              round(m.dist_m::numeric))
from (
  select distinct on (u.fid)
    u.fid,
    c.objectid       as ccc_id,
    similarity(public.clean_beach_name(u.name), public.clean_beach_name(c.name)) as clean_sim,
    st_distance(u.geom::geography, c.geom::geography) as dist_m
  from public.us_beach_points u
  cross join lateral (
    select objectid, name, geom
    from public.ccc_access_points
    where (archived is null or archived <> 'Yes')
      and (admin_inactive is null or admin_inactive = false)
      and name is not null and name <> ''
      and length(public.clean_beach_name(name)) > 2
      and st_dwithin(geom::geography, u.geom::geography, 5000)
    order by similarity(public.clean_beach_name(u.name), public.clean_beach_name(name)) desc nulls last,
             st_distance(u.geom::geography, geom::geography) asc
    limit 1
  ) c
  where u.state = 'CA'
    and (u.admin_inactive is null or u.admin_inactive = false)
    and length(public.clean_beach_name(u.name)) > 2
) m
where m.clean_sim >= 0.7
on conflict (a_source, a_id, b_source, b_id, relationship) do nothing;


-- Update the lite RPC: UBP rows now derive dogs_verdict from their
-- associated CCC (parallel to the OSM aggregation).
drop function if exists public.all_coastal_features_lite(text[]);
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
  -- OSM beach polygons — verdict via associated CCC same_beach
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

  -- UBP-CA — verdict via associated CCC same_beach (NEW)
  select 'beach',
         'ubp/' || u.fid::text,
         u.name, 'beach', 'ubp',
         op.canonical_name,
         (select case
            when bool_or(c.dogs_verdict = 'yes') then 'yes'
            when bool_and(c.dogs_verdict = 'no')  then 'no'
            else null
          end
          from public.feature_associations fa
          join public.ccc_access_points c on c.objectid::text = fa.b_id
          where fa.a_source = 'ubp' and fa.b_source = 'ccc'
            and fa.relationship = 'same_beach'
            and fa.a_id = u.fid::text
            and c.dogs_verdict in ('yes','no')) as dogs_verdict,
         null::text,
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
