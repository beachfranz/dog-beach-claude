-- 20260514_get_beach_info_with_today.sql
--
-- Add today's `day` recommendation to get_beach_info response so
-- beach.html can render a Best Time card + a "for now" Scout blurb
-- using the same context detail.html has. Mirrors the subset of
-- fields detail.html's scout-blurb logic consumes.
--
-- Returns null when the beach has no day recommendation for today
-- (non-scoreable beach, or refresh hasn't run yet). Frontend hides
-- the card in that case.

begin;

create or replace function public.get_beach_info(p_fid bigint)
returns jsonb
language sql
stable security definer
set search_path to public
as $function$
  with selected_photos as (
    select p.source, p.image_url, p.thumb_url, p.attribution, p.license,
           p.page_url, p.captured_at, p.curated_at,
           p.sort_order, p.id,
           coalesce((p.source_meta->>'predicted_keep_prob')::float, 0) keep_prob
      from public.beach_photos p
     where p.arena_group_id = p_fid
       and (
         p.curated_at is not null
         or coalesce((p.source_meta->>'predicted_keep_prob')::float, 0) >= 0.65
       )
     order by
       (p.curated_at is null)::int asc,
       coalesce((p.source_meta->>'predicted_keep_prob')::float, 0) desc,
       p.sort_order asc,
       p.id asc
     limit 6
  ),
  alts as (
    select jsonb_agg(jsonb_build_object(
      'fid',                a.fid,
      'location_id',        a.location_id,
      'name',               a.name,
      'distance_km',        a.distance_km,
      'has_on_leash',       a.has_on_leash,
      'has_off_leash',      a.has_off_leash,
      'day_status',         a.day_status,
      'best_window_label',  a.best_window_label
    ) order by a.distance_km) as data
      from public.find_alternatives_for_detail(p_fid, current_date, 40, 5) a
  )
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
      'noaa_station_id', g.noaa_station_id
      ),
    'dog_policy', case when bdp.arena_group_id is not null then
      jsonb_build_object(
        'dogs_allowed',           bdp.dogs_allowed,
        'leash_policy',           bdp.leash_policy,
        'has_off_leash',          bdp.has_off_leash,
        'has_on_leash',           bdp.has_on_leash,
        'access_rule',            bdp.access_rule,
        'off_leash_flag',         bdp.off_leash_flag,
        'dogs_prohibited_start',  bdp.dogs_prohibited_start,
        'dogs_prohibited_end',    bdp.dogs_prohibited_end,
        'zone_rules',             bdp.zone_rules,
        'notes',                  bdp.notes
      )
    end,
    'amenities', case when ba.arena_group_id is not null then
      to_jsonb(ba) - 'arena_group_id'
    end,
    'description', case when bd.arena_group_id is not null then
      jsonb_build_object(
        'text', bd.description,
        'generated_at', bd.generated_at
      )
    end,
    'today', case when dr.arena_group_id is not null then
      jsonb_build_object(
        'day_status',          dr.day_status,
        'best_window_label',   dr.best_window_label,
        'best_window_status',  dr.best_window_status,
        'avg_wind',            dr.avg_wind,
        'avg_temp',            dr.avg_temp,
        'avg_uv',              dr.avg_uv,
        'lowest_tide_height',  dr.lowest_tide_height,
        'busyness_category',   dr.busyness_category,
        'bacteria_risk',       dr.bacteria_risk
      )
    end,
    'photos', coalesce((
      select jsonb_agg(jsonb_build_object(
        'source',      sp.source,
        'image_url',   sp.image_url,
        'thumb_url',   sp.thumb_url,
        'attribution', sp.attribution,
        'license',     sp.license,
        'page_url',    sp.page_url,
        'captured_at', sp.captured_at,
        'curated',     sp.curated_at is not null,
        'keep_prob',   sp.keep_prob
      ) order by (sp.curated_at is null)::int, sp.keep_prob desc, sp.sort_order, sp.id)
      from selected_photos sp
    ), '[]'::jsonb),
    'alternatives', coalesce((select data from alts), '[]'::jsonb)
  )
  from public.beaches_gold g
  left join public.beach_dog_policy bdp on bdp.arena_group_id = g.group_id
  left join public.beach_amenities  ba  on ba.arena_group_id  = g.group_id
  left join public.beach_descriptions bd on bd.arena_group_id = g.fid
  left join public.beach_day_recommendations dr
    on dr.arena_group_id = g.group_id and dr.local_date = current_date
  where g.fid = p_fid and g.is_active
  limit 1;
$function$;

commit;
