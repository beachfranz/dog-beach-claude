-- 20260530_get_beach_info_v2_fields.sql
--
-- Adds day_status_v2 + composite_score_v2 to the 'today' jsonb and
-- hour_score_v2 + hour_status_v2 to each row of 'hours_today' returned
-- by get_beach_info. Task #7 of v1-retirement track. beach.html /
-- mobile-beach.html consumers prefer v2 fields with v1 fallback.

begin;

create or replace function public.get_beach_info(p_fid bigint)
returns jsonb
language sql
stable
security definer
set search_path to 'public'
as $$
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
        'public_access',          bdp.public_access,
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
        'day_status_v2',       dr.day_status_v2,
        'composite_score_v2',  dr.composite_score_v2,
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
    'hours_today', coalesce((
      select jsonb_agg(jsonb_build_object(
        'local_hour',        h.local_hour,
        'hour_score',        h.hour_score,
        'hour_score_v2',     h.hour_score_v2,
        'hour_status',       h.hour_status,
        'hour_status_v2',    public.v2_compute_hour_status(
                               h.uv_index::numeric, h.asphalt_temp::numeric,
                               h.sand_temp::numeric, h.tide_height::numeric,
                               h.wind_speed::numeric, h.busyness_score::numeric,
                               h.precip_chance::numeric, h.feels_like::numeric,
                               false),
        'is_in_best_window', h.is_in_best_window,
        'is_now',            h.is_now,
        -- Per-metric for the Advisories card
        'tide_status',       h.tide_status,
        'tide_height',       h.tide_height,
        'wind_status',       h.wind_status,
        'wind_speed',        h.wind_speed,
        'rain_status',       h.rain_status,
        'precip_chance',     h.precip_chance,
        'crowd_status',      h.crowd_status,
        'busyness_score',    h.busyness_score,
        'temp_status',       h.temp_status,
        'temp_air',          h.temp_air,
        'feels_like',        h.feels_like,
        'uv_status',         h.uv_status,
        'uv_index',          h.uv_index,
        'sand_status',       h.sand_status,
        'sand_temp',         h.sand_temp,
        'asphalt_status',    h.asphalt_status,
        'asphalt_temp',      h.asphalt_temp,
        -- Per-metric v2 status — derived inline from raw values via
        -- v2_signal_status. Mobile-beach chips + Scout prompts read these
        -- with v1 fallback. Per Franz 2026-05-30 v1-retirement tasks #4/#8.
        'tide_status_v2',    public.v2_signal_status('beach', 'tide_neg',        h.tide_height::numeric),
        'wind_status_v2',    public.v2_signal_status('beach', 'wind_harsh_neg',  h.wind_speed::numeric),
        'rain_status_v2',    public.v2_signal_status('beach', 'precip_chance',   h.precip_chance::numeric),
        'crowd_status_v2',   public.v2_signal_status('beach', 'crowd_neg',       h.busyness_score::numeric),
        'uv_status_v2',      public.v2_signal_status('beach', 'uv_neg',          h.uv_index::numeric),
        'sand_status_v2',    public.v2_signal_status('beach', 'sand_temp_neg',   h.sand_temp::numeric),
        'asphalt_status_v2', public.v2_signal_status('beach', 'asphalt_neg',     h.asphalt_temp::numeric),
        'temp_hot_status_v2',  public.v2_signal_status('beach', 'feels_like_hot',  h.feels_like::numeric),
        'temp_cold_status_v2', public.v2_signal_status('beach', 'feels_like_cold', h.feels_like::numeric)
      ) order by h.local_hour)
      from public.beach_day_hourly_scores h
      where h.arena_group_id = g.group_id and h.local_date = current_date
    ), '[]'::jsonb),
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
$$;

commit;
