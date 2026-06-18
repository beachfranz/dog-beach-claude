-- get_beach_info: expose v3 score alongside v2 so beach.html can read v3.
-- Adds composite_score_v3 to `today` and hour_score_v3 to each hours_today
-- row. Both columns already exist on beach_day_recommendations /
-- beach_day_hourly_scores (dual-written since 20260615e). Pure additive —
-- v2 fields stay; beach.html does v3 ?? v2 fallback client-side.
-- Body otherwise identical to 20260612_get_beach_info_area_m2.sql.

CREATE OR REPLACE FUNCTION public.get_beach_info(p_fid bigint, p_date date DEFAULT NULL::date)
 RETURNS jsonb
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  WITH d AS (
    SELECT COALESCE(
      p_date,
      (now() AT TIME ZONE COALESCE(
        (SELECT timezone FROM public.beaches_gold WHERE fid = p_fid LIMIT 1),
        'America/Los_Angeles'
      ))::date
    ) AS target_date
  ),
  selected_photos AS (
    SELECT p.source, p.image_url, p.thumb_url, p.attribution, p.license,
           p.page_url, p.captured_at, p.curated_at,
           p.sort_order, p.id,
           coalesce((p.source_meta->>'predicted_keep_prob')::float, 0) keep_prob
      FROM public.beach_photos p
     WHERE p.arena_group_id = p_fid
       AND (
         p.curated_at IS NOT NULL
         OR coalesce((p.source_meta->>'predicted_keep_prob')::float, 0) >= 0.65
       )
     ORDER BY
       (p.curated_at IS NULL)::int ASC,
       coalesce((p.source_meta->>'predicted_keep_prob')::float, 0) DESC,
       p.sort_order ASC,
       p.id ASC
     LIMIT 6
  ),
  alts AS (
    SELECT jsonb_agg(jsonb_build_object(
      'fid',                a.fid,
      'location_id',        a.location_id,
      'name',               a.name,
      'distance_km',        a.distance_km,
      'has_on_leash',       a.has_on_leash,
      'has_off_leash',      a.has_off_leash,
      'best_window_label',  a.best_window_label
    ) ORDER BY a.distance_km) AS data
      FROM public.find_alternatives_for_detail(p_fid, current_date, 40, 5) a
  )
  SELECT jsonb_build_object(
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
      'noaa_station_id', g.noaa_station_id,
      'area_m2',       g.area_m2
      ),
    'requested_date', (SELECT target_date FROM d),
    'dog_policy', CASE WHEN bdp.arena_group_id IS NOT NULL THEN
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
    END,
    'amenities', CASE WHEN ba.arena_group_id IS NOT NULL THEN
      to_jsonb(ba) - 'arena_group_id'
    END,
    'description', CASE WHEN bd.arena_group_id IS NOT NULL THEN
      jsonb_build_object(
        'text', bd.description,
        'generated_at', bd.generated_at
      )
    END,
    'today', CASE WHEN dr.arena_group_id IS NOT NULL THEN
      jsonb_build_object(
        'composite_score_v2',  dr.composite_score_v2,
        'composite_score_v3',  dr.composite_score_v3,
        'best_window_label',   dr.best_window_label,
        'best_window_status',  dr.best_window_status,
        'avg_wind',            dr.avg_wind,
        'avg_temp',            dr.avg_temp,
        'avg_uv',              dr.avg_uv,
        'lowest_tide_height',  dr.lowest_tide_height,
        'busyness_category',   dr.busyness_category,
        'bacteria_risk',       dr.bacteria_risk
      )
    END,
    'hours_today', coalesce((
      SELECT jsonb_agg(jsonb_build_object(
        'local_hour',        h.local_hour,
        'hour_score_v2',     h.hour_score_v2,
        'hour_score_v3',     h.hour_score_v3,
        'is_in_best_window', h.is_in_best_window,
        'is_now',            h.is_now,
        'tide_height',       h.tide_height,
        'wind_speed',        h.wind_speed,
        'precip_chance',     h.precip_chance,
        'busyness_score',    h.busyness_score,
        'temp_air',          h.temp_air,
        'feels_like',        h.feels_like,
        'uv_index',          h.uv_index,
        'sand_temp',         h.sand_temp,
        'asphalt_temp',      h.asphalt_temp
      ) ORDER BY h.local_hour)
      FROM public.beach_day_hourly_scores h, d
      WHERE h.arena_group_id = g.group_id AND h.local_date = d.target_date
    ), '[]'::jsonb),
    'photos', coalesce((
      SELECT jsonb_agg(jsonb_build_object(
        'source',      sp.source,
        'image_url',   sp.image_url,
        'thumb_url',   sp.thumb_url,
        'attribution', sp.attribution,
        'license',     sp.license,
        'page_url',    sp.page_url,
        'captured_at', sp.captured_at,
        'curated',     sp.curated_at IS NOT NULL,
        'keep_prob',   sp.keep_prob
      ) ORDER BY (sp.curated_at IS NULL)::int, sp.keep_prob DESC, sp.sort_order, sp.id)
      FROM selected_photos sp
    ), '[]'::jsonb),
    'alternatives', coalesce((SELECT data FROM alts), '[]'::jsonb)
  )
  FROM public.beaches_gold g
  CROSS JOIN d
  LEFT JOIN public.beach_dog_policy bdp ON bdp.arena_group_id = g.group_id
  LEFT JOIN public.beach_amenities  ba  ON ba.arena_group_id  = g.group_id
  LEFT JOIN public.beach_descriptions bd ON bd.arena_group_id = g.fid
  LEFT JOIN public.beach_day_recommendations dr
    ON dr.arena_group_id = g.group_id AND dr.local_date = d.target_date
  WHERE g.fid = p_fid AND g.is_active
  LIMIT 1;
$function$;

NOTIFY pgrst, 'reload schema';
