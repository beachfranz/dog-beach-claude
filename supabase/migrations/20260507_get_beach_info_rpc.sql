-- 20260507_get_beach_info_rpc.sql
--
-- Single RPC that returns everything beach.html needs:
--   beach metadata (name/operator/county/address/website/lat-lng/geom)
--   dog policy (zone_rules, leash binaries, dogs_allowed)
--   amenities (parking, restrooms, showers, picnic, etc.)
--
-- Designed to be called directly from the browser via PostgREST
-- using the anon key. No edge function needed.

begin;

create or replace function public.get_beach_info(p_fid bigint)
returns jsonb
language sql
stable
security definer
set search_path to 'public'
as $$
  select jsonb_build_object(
    'beach', jsonb_build_object(
      'fid',           g.fid,
      'location_id',   g.location_id,
      'name',          g.name,
      'display_name',  coalesce(g.display_name_override, g.name),
      'county',        g.county_name,
      'state',         g.state,
      'address',       g.address,
      'website',       g.website,
      'timezone',      g.timezone,
      'lat',           st_y(g.geom),
      'lng',           st_x(g.geom),
      'geom',          st_asgeojson(g.geom)::jsonb,
      'open_time',     g.open_time,
      'close_time',    g.close_time,
      'is_scoreable',  g.is_scoreable
    ),
    'dog_policy', case when bdp.arena_group_id is not null then
      jsonb_build_object(
        'dogs_allowed',           bdp.dogs_allowed,
        'leash_policy',           bdp.leash_policy,
        'has_on_leash',           bdp.has_on_leash,
        'has_off_leash',          bdp.has_off_leash,
        'off_leash_flag',         bdp.off_leash_flag,
        'zone_rules',             bdp.zone_rules,
        'consensus_confidence',   bdp.consensus_confidence,
        'disagreement_flag',      bdp.disagreement_flag
      ) else null end,
    'amenities', case when ba.arena_group_id is not null then
      jsonb_build_object(
        'has_parking',         ba.has_parking,
        'parking_type',        ba.parking_type,
        'parking_notes',       ba.parking_notes,
        'has_restrooms',       ba.has_restrooms,
        'has_showers',         ba.has_showers,
        'has_picnic_area',     ba.has_picnic_area,
        'has_drinking_water',  ba.has_drinking_water,
        'has_food',            ba.has_food,
        'has_fire_pits',       ba.has_fire_pits,
        'has_lifeguards',      ba.has_lifeguards,
        'has_disabled_access', ba.has_disabled_access,
        'hours_text',          ba.hours_text
      ) else null end
  )
  from public.beaches_gold g
  left join public.beach_dog_policy bdp on bdp.arena_group_id = g.group_id
  left join public.beach_amenities  ba  on ba.arena_group_id  = g.group_id
  where g.fid = p_fid and g.is_active
  limit 1;
$$;

grant execute on function public.get_beach_info(bigint)
  to anon, authenticated, service_role;

commit;
