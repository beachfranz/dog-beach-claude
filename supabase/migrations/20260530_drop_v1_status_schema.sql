-- 20260530_drop_v1_status_schema.sql
--
-- Task #12 of v1-retirement track. Final destructive cleanup:
--   - DROPs v1 *_status + hour_status columns from beach_day_hourly_scores
--     and dog_park_day_hourly_scores.
--   - DROPs day_status columns from beach_day_recommendations and
--     dog_park_day_recommendations.
--   - Updates find_beaches, find_dog_parks, nearest_dog_parks, and
--     get_beach_info RPCs to drop v1 column references.
--   - DROPs the legacy scoring_config table.
--
-- Pre-requisites (all landed earlier today):
--   * Task #11 stopped daily-refresh + get-now from writing v1 columns.
--   * v2 day_status_v2 + composite_score_v2 + hour_score_v2 backfilled
--     across the catalog via the chunked-refresh cron.
--   * All consumer-facing edge functions + HTML migrated to v2.
--   * Final grep sweep (task #10) confirmed no remaining v1-only reads.
--
-- This migration is IRREVERSIBLE. Restoring v1 would require regenerating
-- the columns + recomputing values from raw row data using the retired
-- _shared/scoring.ts logic (still committed in git history).

begin;

-- ── Update find_beaches RPC: drop day_status from RETURN ─────────────
drop function if exists public.find_beaches(date, double precision, double precision, text, integer, boolean);

create function public.find_beaches(
  p_date date,
  p_lat double precision default null,
  p_lng double precision default null,
  p_leash text default 'any',
  p_limit integer default null,
  p_scored_only boolean default false
)
returns table(
  arena_group_id        bigint,
  location_id           text,
  display_name          text,
  latitude              double precision,
  longitude             double precision,
  access_rule           text,
  has_on_leash          boolean,
  has_off_leash         boolean,
  dogs_allowed          text,
  dogs_prohibited_start text,
  location_tier         text,
  distance_m            double precision,
  day_status_v2         text,
  composite_score_v2    int,
  best_window_label     text,
  best_window_status    text,
  bacteria_risk         text,
  summary_weather       text,
  weather_code          integer,
  lowest_tide_height    numeric,
  avg_temp              numeric,
  avg_wind              numeric,
  busyness_category     text,
  go_hours_count        integer,
  avg_tide_height       numeric
)
language sql
stable
security definer
set search_path to 'public'
as $$
  select
    g.fid as arena_group_id,
    g.location_id,
    coalesce(g.display_name_override, g.name) as display_name,
    g.lat::double precision as latitude,
    g.lon::double precision as longitude,
    dp.access_rule,
    dp.has_on_leash,
    dp.has_off_leash,
    dp.dogs_allowed,
    dp.dogs_prohibited_start::text as dogs_prohibited_start,
    public.beach_location_tier(
      dp.dogs_allowed, dp.has_off_leash, dp.has_on_leash,
      dp.dogs_prohibited_start::text
    ) as location_tier,
    case
      when p_lat is not null and p_lng is not null
      then ST_Distance(g.geom::geography, ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography)
      else null
    end as distance_m,
    dr.day_status_v2,
    dr.composite_score_v2,
    dr.best_window_label,
    dr.best_window_status,
    dr.bacteria_risk,
    dr.summary_weather,
    dr.weather_code,
    dr.lowest_tide_height,
    dr.avg_temp,
    dr.avg_wind,
    dr.busyness_category,
    dr.go_hours_count,
    dr.avg_tide_height
  from public.beaches_gold g
  left join public.beach_day_recommendations dr
         on dr.arena_group_id = g.fid
        and dr.local_date     = p_date
  left join public.beach_dog_policy dp
         on dp.arena_group_id = g.fid
  where g.is_active = true
    and (p_leash = 'any' or dp.access_rule = p_leash or dp.access_rule is null)
    and (not p_scored_only or dr.day_status_v2 is not null)
  order by
    case
      when p_lat is not null and p_lng is not null
      then g.geom::geography <-> ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography
      else null
    end nulls last
  limit case when p_limit is not null and p_limit > 0 then p_limit else null end;
$$;

-- ── Update find_dog_parks RPC: drop day_status from RETURN ───────────
drop function if exists public.find_dog_parks(date, double precision, double precision, text, integer, boolean, text, boolean, boolean);

create function public.find_dog_parks(
  p_date date,
  p_lat double precision default null,
  p_lng double precision default null,
  p_leash text default 'any',
  p_limit integer default null,
  p_scored_only boolean default false,
  p_surface text default null,
  p_fence boolean default null,
  p_water boolean default null
)
returns table(
  dog_park_fid          bigint,
  name                  text,
  display_name          text,
  latitude              double precision,
  longitude             double precision,
  surface               text,
  has_fence             boolean,
  has_drinking_water    boolean,
  double_gate           boolean,
  small_dog_area        boolean,
  large_dog_area        boolean,
  lighting              boolean,
  leash_policy          text,
  off_leash_flag        boolean,
  hours_text            text,
  hours_open_time       text,
  hours_close_time      text,
  additional_rules      text,
  description           text,
  source                text,
  source_url            text,
  operator_id           bigint,
  operator_short        text,
  distance_m            double precision,
  day_status_v2         text,
  composite_score_v2    int,
  best_window_label     text,
  best_window_status    text,
  summary_weather       text,
  weather_code          integer,
  avg_temp              numeric,
  avg_wind              numeric,
  avg_uv                numeric,
  go_hours_count        integer,
  composite_score       numeric
)
language sql
stable
security definer
set search_path to 'public'
as $$
  select
    g.fid                                                       as dog_park_fid,
    g.name                                                      as name,
    coalesce(g.display_name_override, g.name)                   as display_name,
    g.lat                                                       as latitude,
    g.lon                                                       as longitude,
    coalesce(dp.surface_overlay, g.surface)                     as surface,
    coalesce(dp.has_fence, g.has_fence)                         as has_fence,
    coalesce(dp.has_drinking_water, g.has_drinking_water)       as has_drinking_water,
    dp.double_gate,
    dp.small_dog_area,
    dp.large_dog_area,
    dp.lighting,
    coalesce(dp.leash_policy, 'off_leash')                      as leash_policy,
    coalesce(dp.off_leash_flag, true)                           as off_leash_flag,
    dp.hours_text,
    dp.hours_open_time,
    dp.hours_close_time,
    dp.additional_rules,
    coalesce(dp.description_overlay, g.description)             as description,
    dp.source,
    dp.source_url,
    dp.operator_id,
    op.name                                                     as operator_short,
    case
      when p_lat is not null and p_lng is not null
      then ST_Distance(g.geom::geography, ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography)
      else null
    end                                                         as distance_m,
    dr.day_status_v2,
    dr.composite_score_v2,
    dr.best_window_label,
    dr.best_window_status,
    dr.summary_weather,
    dr.weather_code,
    dr.avg_temp,
    dr.avg_wind,
    dr.avg_uv,
    dr.go_hours_count,
    case
      when dr.go_hours_count is null then null
      else round(
        100.0 * dr.go_hours_count
        / nullif(dr.go_hours_count + dr.advisory_hours_count
               + dr.caution_hours_count + dr.no_go_hours_count, 0),
        0
      )::numeric
    end                                                         as composite_score
  from public.dog_parks_gold g
  left join public.dog_park_dog_policy dp on dp.dog_park_fid = g.fid
  left join public.operator op on op.id = dp.operator_id
  left join public.dog_park_day_recommendations dr
         on dr.dog_park_fid = g.fid and dr.local_date = p_date
  where g.is_active = true
    and g.is_scoreable = true
    and (
      p_leash = 'any'
      or (p_leash = 'off' and coalesce(dp.off_leash_flag, true) is true)
      or (p_leash = 'on'  and dp.leash_policy = 'on_leash')
    )
    and (p_surface is null or coalesce(dp.surface_overlay, g.surface) = p_surface)
    and (p_fence   is null or coalesce(dp.has_fence,          g.has_fence)          = p_fence)
    and (p_water   is null or coalesce(dp.has_drinking_water, g.has_drinking_water) = p_water)
    and (not p_scored_only or dr.day_status_v2 is not null)
  order by
    case
      when p_lat is not null and p_lng is not null
      then g.geom::geography <-> ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)::geography
      else null
    end nulls last
  limit case when p_limit is not null and p_limit > 0 then p_limit else null end;
$$;

-- ── Update nearest_dog_parks RPC: drop day_status from RETURN ────────
drop function if exists public.nearest_dog_parks(double precision, double precision, integer, text, date);

create function public.nearest_dog_parks(
  p_lat double precision default null,
  p_lng double precision default null,
  p_limit integer default 10,
  p_state text default null,
  p_date date default null
)
returns table(
  fid                    bigint,
  name                   text,
  state                  text,
  address_city           text,
  lat                    double precision,
  lon                    double precision,
  distance_miles         double precision,
  has_fence              boolean,
  has_drinking_water     boolean,
  surface                text,
  leash_policy           text,
  hours_text             text,
  hours_open_time        text,
  hours_close_time       text,
  additional_rules       text,
  source                 text,
  source_url             text,
  consensus_confidence   numeric,
  operator_id            bigint,
  operator_name          text,
  operator_short         text,
  composite_score        numeric,
  day_status_v2          text,
  composite_score_v2     int,
  best_window_label      text,
  best_window_status     text,
  go_hours_count         integer,
  summary_weather        text,
  weather_code           integer,
  avg_temp               numeric,
  avg_wind               numeric,
  score_v2               integer,
  score_v2_components    jsonb,
  drive_minutes          integer
)
language sql
stable parallel safe
as $$
  with user_pt as (
    select case when p_lat is not null and p_lng is not null
                then st_setsrid(st_makepoint(p_lng, p_lat), 4326)::geography
                else null::geography end as pt,
           coalesce(p_date, current_date) as d
  )
  select
    g.fid, g.name, g.state, g.address_city, g.lat, g.lon,
    case when up.pt is not null
         then st_distance(up.pt, g.geom::geography) / 1609.344
         else null::double precision
    end as distance_miles,
    g.has_fence, g.has_drinking_water, g.surface,
    p.leash_policy, p.hours_text, p.hours_open_time, p.hours_close_time, p.additional_rules,
    p.source, p.source_url, p.consensus_confidence,
    p.operator_id, o.name as operator_name, o.short_name as operator_short,
    hs.composite_score,
    dr.day_status_v2, dr.composite_score_v2,
    dr.best_window_label, dr.best_window_status, dr.go_hours_count,
    dr.summary_weather, dr.weather_code, dr.avg_temp, dr.avg_wind,
    (sv.payload->>'score')::int           as score_v2,
    sv.payload                            as score_v2_components,
    (sv.payload->>'drive_minutes')::int   as drive_minutes
  from public.dog_parks_gold g
  join public.dog_park_dog_policy p on p.dog_park_fid = g.fid
  cross join user_pt up
  left join public.operator o on o.id = p.operator_id
  left join public.dog_park_day_recommendations dr
         on dr.dog_park_fid = g.fid and dr.local_date = up.d
  left join lateral (
    select round(avg(h.hour_score_v2)::numeric, 0) as composite_score
      from public.dog_park_day_hourly_scores h
     where h.dog_park_fid = g.fid
       and h.local_date = up.d
       and h.hour_score_v2 is not null
  ) hs on true
  left join lateral (
    select public.compute_dog_park_score_v2(g.fid, p_lat, p_lng, up.d) as payload
  ) sv on true
  where g.is_active = true
    and (p_state is null or g.state = p_state)
  order by
    case when up.pt is not null then st_distance(up.pt, g.geom::geography) end nulls last,
    coalesce((sv.payload->>'score')::int, 0) desc,
    g.name
  limit greatest(coalesce(p_limit, 10), 1);
$$;

-- ── Update get_beach_info RPC: drop v1 status fields from JSON ───────
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
        'hour_score_v2',     h.hour_score_v2,
        'hour_status_v2',    public.v2_compute_hour_status(
                               h.uv_index::numeric, h.asphalt_temp::numeric,
                               h.sand_temp::numeric, h.tide_height::numeric,
                               h.wind_speed::numeric, h.busyness_score::numeric,
                               h.precip_chance::numeric, h.feels_like::numeric,
                               false),
        'is_in_best_window', h.is_in_best_window,
        'is_now',            h.is_now,
        -- Raw values (consumers compute per-metric v2 statuses inline)
        'tide_height',       h.tide_height,
        'wind_speed',        h.wind_speed,
        'precip_chance',     h.precip_chance,
        'busyness_score',    h.busyness_score,
        'temp_air',          h.temp_air,
        'feels_like',        h.feels_like,
        'uv_index',          h.uv_index,
        'sand_temp',         h.sand_temp,
        'asphalt_temp',      h.asphalt_temp,
        -- Per-metric v2 status (server-side derivation via v2_signal_status)
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

-- ── Update find_alternatives_for_detail to read day_status_v2 ───────
create or replace function public.find_alternatives_for_detail(
  p_fid bigint,
  p_date date,
  p_max_km double precision default 40,
  p_limit integer default 5
)
returns table(
  fid bigint, location_id text, name text,
  distance_km double precision,
  has_on_leash boolean, has_off_leash boolean,
  day_status text, best_window_label text
)
language sql
stable security definer
set search_path to 'public'
as $$
  with src as (
    select fid, geom from public.beaches_gold where fid = p_fid
  )
  select
    g.fid,
    g.location_id,
    coalesce(g.display_name_override, g.name) as name,
    st_distance(g.geom::geography, src.geom::geography) / 1000.0 as distance_km,
    bdp.has_on_leash,
    bdp.has_off_leash,
    dr.day_status_v2 as day_status,
    dr.best_window_label
  from public.beaches_gold g
  cross join src
  left join public.beach_dog_policy bdp on bdp.arena_group_id = g.group_id
  left join public.beach_day_recommendations dr
         on dr.arena_group_id = g.group_id and dr.local_date = p_date
  where g.is_active
    and (g.scoring_tier in ('daily','hourly'))
    and g.fid <> p_fid
    and (bdp.has_on_leash is true or bdp.has_off_leash is true)
    and st_dwithin(g.geom::geography, src.geom::geography, p_max_km * 1000)
  order by g.geom::geography <-> src.geom::geography
  limit greatest(p_limit, 1);
$$;

-- ── Recreate dependent view ──────────────────────────────────────────
-- beach_pipeline_activity_recent embeds day_status; swap to day_status_v2.
drop view if exists public.beach_pipeline_activity_recent;
create view public.beach_pipeline_activity_recent as
(SELECT 'evidence_added'::text AS event,
    e.updated_at AS event_at,
    e.gold_fid AS fid,
    bg.name AS beach_name,
    e.source AS detail,
    NULL::text AS field_name
   FROM beach_enrichment_provenance e
     JOIN beaches_gold bg ON bg.fid = e.gold_fid
  WHERE e.field_group = 'dogs'::text
  ORDER BY e.updated_at DESC
 LIMIT 25)
UNION ALL
(SELECT 'zone_rules_promoted'::text AS event,
    p.zone_rules_updated_at AS event_at,
    p.arena_group_id AS fid,
    bg.name AS beach_name,
    NULL::text AS detail,
    'zone_rules'::text AS field_name
   FROM beach_dog_policy p
     JOIN beaches_gold bg ON bg.fid = p.arena_group_id
  WHERE p.zone_rules_updated_at IS NOT NULL
  ORDER BY p.zone_rules_updated_at DESC
 LIMIT 25)
UNION ALL
(SELECT 'recommendation_landed'::text AS event,
    r.updated_at AS event_at,
    r.arena_group_id AS fid,
    bg.name AS beach_name,
    r.day_status_v2 AS detail,
    'recommendation'::text AS field_name
   FROM beach_day_recommendations r
     JOIN beaches_gold bg ON bg.fid = r.arena_group_id
  WHERE r.local_date = CURRENT_DATE AND r.updated_at > now() - '06:00:00'::interval
  ORDER BY r.updated_at DESC
 LIMIT 15)
ORDER BY 2 DESC NULLS LAST
LIMIT 50;

-- ── DROP v1 status columns ───────────────────────────────────────────
alter table public.beach_day_hourly_scores
  drop column if exists hour_status,
  drop column if exists tide_status,
  drop column if exists wind_status,
  drop column if exists rain_status,
  drop column if exists crowd_status,
  drop column if exists temp_status,
  drop column if exists temp_hot_status,
  drop column if exists temp_cold_status,
  drop column if exists uv_status,
  drop column if exists sand_status,
  drop column if exists asphalt_status;

alter table public.beach_day_recommendations
  drop column if exists day_status;

alter table public.dog_park_day_hourly_scores
  drop column if exists hour_status,
  drop column if exists wind_status,
  drop column if exists rain_status,
  drop column if exists temp_status,
  drop column if exists temp_hot_status,
  drop column if exists temp_cold_status,
  drop column if exists uv_status,
  drop column if exists asphalt_status,
  drop column if exists surface_status;

alter table public.dog_park_day_recommendations
  drop column if exists day_status;

-- ── DROP legacy scoring_config table ─────────────────────────────────
drop table if exists public.scoring_config;

commit;
