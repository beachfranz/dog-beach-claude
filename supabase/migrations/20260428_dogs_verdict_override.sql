-- dogs_verdict_override: per-origin_key admin override at the TOP of
-- the cascade in all_coastal_features_lite. Highest precedence — beats
-- inheritance, operator policy, exceptions, everything.
--
-- Two use cases:
--   1. Manual admin overrides (verdict=manual, set by hand)
--   2. External triangulation auto-fills (verdict=auto, set by
--      apply_external_triangulation_overrides() based on
--      truth_comparison_v consensus across BringFido / DogTrekker /
--      CaliforniaBeaches / websearch).
--
-- Scope: applies to OSM / UBP / CCC rows alike via origin_key.

create table if not exists public.dogs_verdict_override (
  origin_key text primary key,
  verdict    text not null check (verdict in ('yes','no')),
  reason     text,
  source     text not null check (source in ('manual','auto')),
  set_at     timestamptz not null default now()
);

create index if not exists dogs_verdict_override_source_idx
  on public.dogs_verdict_override (source);


-- ── Updated lite RPC: override consulted first ──────────────────────
drop view if exists public.truth_comparison_v cascade;
drop function if exists public.all_coastal_features_lite(text[]) cascade;
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
  select 'beach'::text,
         'osm/' || o.osm_type || '/' || o.osm_id::text as origin_key,
         o.name, o.feature_type, 'osm'::text,
         op.canonical_name,
         coalesce(
           (select ov.verdict
              from public.dogs_verdict_override ov
             where ov.origin_key = 'osm/' || o.osm_type || '/' || o.osm_id::text),
           (select case
              when count(*) filter (where c.dogs_verdict='yes')
                 > count(*) filter (where c.dogs_verdict='no')  then 'yes'
              when count(*) filter (where c.dogs_verdict='no')
                 > count(*) filter (where c.dogs_verdict='yes') then 'no'
              else null end
            from public.feature_associations fa
            join public.ccc_access_points c on c.objectid::text = fa.a_id
            where fa.a_source='ccc' and fa.b_source='osm'
              and fa.relationship='same_beach'
              and fa.b_id = o.osm_type || '/' || o.osm_id::text
              and c.dogs_verdict in ('yes','no')),
           public.operator_verdict_for_beach(o.operator_id, o.name)
         ) as dogs_verdict,
         null::text,
         st_y(o.geom), st_x(o.geom)
  from public.osm_features o
  left join public.operators op on op.id = o.operator_id
  where o.feature_type in ('beach','dog_friendly_beach')
    and (o.admin_inactive is null or o.admin_inactive = false)
    and (p_counties is null or o.county_name_tiger = any(p_counties))

  union all

  select 'beach',
         'ubp/' || u.fid::text as origin_key,
         u.name, 'beach', 'ubp',
         op.canonical_name,
         coalesce(
           (select ov.verdict
              from public.dogs_verdict_override ov
             where ov.origin_key = 'ubp/' || u.fid::text),
           (select case
              when count(*) filter (where c.dogs_verdict='yes')
                 > count(*) filter (where c.dogs_verdict='no')  then 'yes'
              when count(*) filter (where c.dogs_verdict='no')
                 > count(*) filter (where c.dogs_verdict='yes') then 'no'
              else null end
            from public.feature_associations fa
            join public.ccc_access_points c on c.objectid::text = fa.b_id
            where fa.a_source='ubp' and fa.b_source='ccc'
              and fa.relationship='same_beach'
              and fa.a_id = u.fid::text
              and c.dogs_verdict in ('yes','no')),
           public.operator_verdict_for_beach(u.operator_id, u.name)
         ) as dogs_verdict,
         null::text,
         st_y(u.geom), st_x(u.geom)
  from public.us_beach_points u
  left join public.operators op on op.id = u.operator_id
  where u.state = 'CA'
    and (u.admin_inactive is null or u.admin_inactive = false)
    and (p_counties is null or u.county_name_tiger = any(p_counties))

  union all

  select case when coalesce(c.inferred_type, '') in ('beach','named_beach') then 'beach'
              else 'access' end,
         'ccc/' || c.objectid::text as origin_key,
         c.name,
         coalesce(c.inferred_type, 'unknown'),
         'ccc',
         op.canonical_name,
         coalesce(
           (select ov.verdict
              from public.dogs_verdict_override ov
             where ov.origin_key = 'ccc/' || c.objectid::text),
           c.dogs_verdict
         ),
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


-- ── Auto-fill function: read truth_comparison_v, set overrides ──────
-- Rule: when >=2 external sources agree (yes or no) AND our_verdict is
-- null, set an auto-override matching the external consensus.
--
-- Idempotent — only inserts where origin_key not already overridden.

create or replace function public.apply_external_triangulation_overrides()
returns int language plpgsql security definer as $$
declare
  v_count int := 0;
  r       record;
begin
  for r in
    select origin_key, n_external_yes, n_external_no
      from public.truth_comparison_v
     where our_verdict is null
       and (n_external_yes >= 2 or n_external_no >= 2)
  loop
    insert into public.dogs_verdict_override(origin_key, verdict, reason, source)
    values (
      r.origin_key,
      case when r.n_external_yes > r.n_external_no then 'yes' else 'no' end,
      format('external_triangulation: %s yes / %s no',
             r.n_external_yes, r.n_external_no),
      'auto'
    )
    on conflict (origin_key) do nothing;
    v_count := v_count + 1;
  end loop;
  return v_count;
end;
$$;
