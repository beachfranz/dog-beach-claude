-- 20260528_dog_park_closures.sql
--
-- Add structured closures to dog_park_dog_policy + plumb into v2 scoring.
-- Per Franz 2026-05-28 — Costa Mesa fid 518 is closed Wednesdays for
-- maintenance; we were still sending people there because hours were
-- captured only as free text. Hour-level only — no day-level rollup;
-- partial-day closures still let people visit during open hours.
--
-- Schema:
--   dog_park_dog_policy.closures jsonb default '[]'
--
-- Shape (each element):
--   { kind: 'weekly',       weekday: 'wed', all_day: true, reason: '...' }
--   { kind: 'weekly_time',  weekday: 'tue', start: '06:00', end: '10:00', reason: '...' }
--   (defer: monthly_nth_weekday, seasonal_only — handle in a v2 of this work)
--
-- Behavior:
--   compute_dog_park_hourly_v2 → set hour status='closed', score=0 for matching hours
--   _find_best_window_v2 → filters out status='closed' from candidate hours
--   day_status / composite NOT changed — partial closures don't gate the whole day

begin;

alter table public.dog_park_dog_policy
  add column if not exists closures jsonb not null default '[]'::jsonb;

comment on column public.dog_park_dog_policy.closures is
  'Array of recurring closure rules. Each element: '
  '{kind: ''weekly''|''weekly_time'', weekday: ''mon''..''sun'', '
  'all_day: bool, start: ''HH:MM'', end: ''HH:MM'', reason: text, '
  'evidence_quote: text}. Reads honored by compute_dog_park_hourly_v2 '
  'at hour granularity — no day-level rollup.';


-- ── Helper: is (date, hour) closed? ─────────────────────────────────

create or replace function public._v2_is_hour_closed(
  p_closures jsonb,
  p_date     date,
  p_hour     int
) returns boolean language sql stable as $$
  with dow as (
    select case extract(isodow from p_date)::int
      when 1 then 'mon' when 2 then 'tue' when 3 then 'wed'
      when 4 then 'thu' when 5 then 'fri' when 6 then 'sat'
      when 7 then 'sun' end as day_str
  )
  select coalesce((
    select true
    from jsonb_array_elements(coalesce(p_closures, '[]'::jsonb)) c, dow
    where lower(c->>'weekday') = dow.day_str
      and (
        ((c->>'kind') = 'weekly' and coalesce((c->>'all_day')::boolean, true))
        or
        ((c->>'kind') = 'weekly_time'
         and p_hour >= extract(hour from (c->>'start')::time)::int
         and p_hour <  extract(hour from (c->>'end')::time)::int)
      )
    limit 1
  ), false)
$$;

comment on function public._v2_is_hour_closed is
  'True if (date, hour) falls inside any closure window in the closures jsonb. '
  'Used by entity-level v2 scoring to mark hour_status=''closed''.';


-- ── Update _find_best_window_v2 to skip closed hours ────────────────

create or replace function public._find_best_window_v2(
  p_hours      jsonb,
  p_min_score  integer default 70,
  p_min_len    integer default 2,
  p_max_len    integer default 5
) returns jsonb language plpgsql stable as $$
declare
  v_start int; v_end int; v_score int; v_label text;
begin
  with hours_data as (
    select (h->>'local_hour')::int as hr,
           (h->>'score')::int as score,
           coalesce((h->>'gate_fired')::boolean, false) as gate_fired,
           coalesce(h->>'status', '') as status
    from jsonb_array_elements(p_hours) h
  ),
  qualifying as (
    select hr, score, hr - row_number() over (order by hr) as grp
    from hours_data
    where score >= p_min_score
      and not gate_fired
      and status != 'closed'
  ),
  runs as (
    select min(hr) as start_h, max(hr) as end_h,
           count(*)::int as len, max(score)::int as max_score
    from qualifying
    group by grp
    having count(*) >= p_min_len
  ),
  best_run as (
    select start_h, end_h, len, max_score from runs
    order by max_score desc, len desc, start_h
    limit 1
  )
  select start_h, end_h, max_score into v_start, v_end, v_score from best_run;

  if v_start is null then
    return jsonb_build_object('label', null, 'start', null, 'end', null, 'score', null);
  end if;

  if (v_end - v_start + 1) > p_max_len then
    select sub_start into v_start
      from (
        select s.sub_start,
               (select sum((h2->>'score')::int)
                  from jsonb_array_elements(p_hours) h2
                 where (h2->>'local_hour')::int between s.sub_start
                                                   and s.sub_start + p_max_len - 1
                   and coalesce(h2->>'status', '') != 'closed') as window_sum
        from generate_series(v_start, v_end - p_max_len + 1) s(sub_start)
      ) candidates
      order by window_sum desc nulls last
      limit 1;
    v_end := v_start + p_max_len - 1;
  end if;

  v_label := public._format_hour_range(v_start, v_end + 1);
  return jsonb_build_object(
    'label', v_label, 'start', v_start, 'end', v_end, 'score', v_score
  );
end;
$$;


-- ── Update compute_dog_park_hourly_v2 to honor closures ─────────────

create or replace function public.compute_dog_park_hourly_v2(
  p_fid bigint, p_user_lat double precision default null,
  p_user_lng double precision default null, p_date date default null
) returns jsonb language plpgsql stable as $$
declare
  v_gold record; v_policy record; v_today record;
  v_view_date date := coalesce(p_date, current_date);
  v_fence_pos integer := 0; v_water_pos integer := 0; v_comfort_pos integer := 0;
  v_agility_pos integer := 0;
  v_fence boolean; v_water boolean; v_shade boolean;
  v_agility boolean; v_water_play boolean;
  v_surface text;
  v_drive integer := 0; v_drive_miles numeric; v_drive_min integer;
  v_open_h integer; v_close_h integer; v_hours jsonb;
  v_bw jsonb;
  v_closures jsonb;
  v_b_wind_pos jsonb; v_b_wind_harsh_neg jsonb; v_b_asphalt_neg jsonb;
  v_b_uv_neg   jsonb; v_b_uv_need        jsonb; v_b_temp_need   jsonb;
  v_b_drive    jsonb;
begin
  select * into v_gold from public.dog_parks_gold where fid = p_fid;
  if v_gold.fid is null then return jsonb_build_object('error','park not found','fid',p_fid); end if;
  select * into v_policy from public.dog_park_dog_policy where dog_park_fid = p_fid limit 1;
  select * into v_today from public.dog_park_day_recommendations where dog_park_fid = p_fid and local_date = v_view_date limit 1;

  v_closures := coalesce(v_policy.closures, '[]'::jsonb);

  select bands into v_b_wind_pos       from public.scoring_config_v2 where entity_type = 'dog_park' and signal_key = 'wind_pos';
  select bands into v_b_wind_harsh_neg from public.scoring_config_v2 where entity_type = 'dog_park' and signal_key = 'wind_harsh_neg';
  select bands into v_b_asphalt_neg    from public.scoring_config_v2 where entity_type = 'dog_park' and signal_key = 'asphalt_neg';
  select bands into v_b_uv_neg         from public.scoring_config_v2 where entity_type = 'dog_park' and signal_key = 'uv_neg';
  select bands into v_b_uv_need        from public.scoring_config_v2 where entity_type = 'dog_park' and signal_key = 'uv_need';
  select bands into v_b_temp_need      from public.scoring_config_v2 where entity_type = 'dog_park' and signal_key = 'temp_need';
  select bands into v_b_drive          from public.scoring_config_v2 where entity_type = 'dog_park' and signal_key = 'drive_factor';

  v_fence := coalesce(v_policy.has_fence, v_gold.has_fence);
  v_water := coalesce(v_policy.has_drinking_water, v_gold.has_drinking_water);
  v_shade := v_policy.has_shade;
  v_agility := v_policy.has_agility;
  v_water_play := v_policy.has_water_play;
  v_surface := lower(coalesce(v_policy.surface_overlay, v_gold.surface, ''));

  if v_fence = true              then v_fence_pos := v_fence_pos + 6; end if;
  if v_policy.double_gate = true  then v_fence_pos := v_fence_pos + 2; end if;
  if v_water = true              then v_water_pos := v_water_pos + 4; end if;
  if v_surface = 'grass'         then v_comfort_pos := v_comfort_pos + 1; end if;
  if v_policy.small_dog_area = true
     or v_policy.large_dog_area = true then v_comfort_pos := v_comfort_pos + 1; end if;
  if v_policy.has_picnic_tables = true then v_comfort_pos := v_comfort_pos + 2; end if;
  if v_agility = true            then v_agility_pos := 3; end if;

  if p_user_lat is not null and p_user_lng is not null
     and v_gold.lat is not null and v_gold.lon is not null then
    v_drive_miles := st_distance(
      st_setsrid(st_makepoint(p_user_lng, p_user_lat), 4326)::geography,
      st_setsrid(st_makepoint(v_gold.lon, v_gold.lat), 4326)::geography) / 1609.344;
    v_drive_min := round(v_drive_miles * 1.8)::integer;
    v_drive := public._v2_lookup_band(v_drive_min::numeric, v_b_drive);
  end if;

  v_open_h := coalesce(extract(hour from v_policy.hours_open_time::time)::int, 6);
  v_close_h := coalesce(extract(hour from v_policy.hours_close_time::time)::int, 22);
  if v_close_h <= v_open_h then v_close_h := 22; end if;

  with per_hour as (
    select h.local_hour, h.hour_label, h.hour_status as v1_status,
      h.feels_like, h.wind_speed, h.uv_index, h.weather_code,
      h.asphalt_temp, h.precip_chance, h.is_in_best_window,
      public._v2_is_hour_closed(v_closures, v_view_date, h.local_hour) as is_closed,
      greatest(0, round(10 * (1 - least(abs(coalesce(h.feels_like, 72) - 72), 20) / 20.0))::int) as temp_pos,
      public._v2_lookup_band(coalesce(h.wind_speed, -1), v_b_wind_pos) as wind_pos,
      case h.weather_code
        when 0 then 6 when 1 then 5 when 2 then 4 when 3 then 2
        when 45 then 1 when 48 then 0 when 51 then 2 when 53 then 1 when 55 then 0
        when 56 then 0 when 57 then 0 when 61 then 1 when 63 then 0 when 65 then 0
        when 66 then 0 when 67 then 0 when 71 then 0 when 73 then 0 when 75 then 0 when 77 then 0
        when 80 then 1 when 81 then 0 when 82 then 0 when 85 then 0 when 86 then 0
        when 95 then 0 when 96 then 0 when 99 then 0 else 2 end as sky_pos,
      least(6, greatest(
        public._v2_lookup_band(coalesce(h.uv_index, -1),  v_b_uv_need),
        public._v2_lookup_band(coalesce(h.feels_like, -1), v_b_temp_need)
      )) as shade_need,
      least(25,
        public._v2_lookup_band(coalesce(h.asphalt_temp, -1), v_b_asphalt_neg)
        + public._v2_lookup_band(coalesce(h.uv_index, -1), v_b_uv_neg)
        + case when h.feels_like > 90 then least(5, greatest(0, round((h.feels_like - 90) * 0.5)::int)) else 0 end
      ) as heat_uv_neg,
      least(15,
        public._v2_lookup_band(coalesce(h.wind_speed, -1), v_b_wind_harsh_neg)
        + case when h.precip_chance > 30 then least(10, greatest(0, round((h.precip_chance - 30) / 7.0)::int)) else 0 end
        + case when h.feels_like < 50 then least(10, greatest(0, round((50 - h.feels_like) * 0.4)::int)) else 0 end
      ) as harsh_neg,
      case when h.is_in_best_window then 5 else 0 end as window_pos,
      (coalesce(h.asphalt_temp, 0) >= 125 or coalesce(h.uv_index, 0) >= 11
       or coalesce(h.feels_like, 99) <= 20) as gate_fired,
      case when coalesce(h.asphalt_temp, 0) >= 125 or coalesce(h.uv_index, 0) >= 11
                or coalesce(h.feels_like, 99) <= 20 then 30
           when coalesce(h.asphalt_temp, 0) >= 115 or coalesce(h.feels_like, 70) >= 95
                or coalesce(h.feels_like, 70) <= 32 or coalesce(h.precip_chance, 0) >= 80 then 60
           else 100 end as status_cap
    from public.dog_park_day_hourly_scores h
    where h.dog_park_fid = p_fid and h.local_date = v_view_date
      and h.local_hour between v_open_h and v_close_h
  ),
  composed as (
    select *,
      least(22, temp_pos + wind_pos + sky_pos) as weather_pos,
      case when v_shade = true then shade_need else 0 end as shade_pos,
      case when v_water_play = true then shade_need else 0 end as water_play_pos,
      case when v_shade = false then shade_need else 0 end as gotchas_neg,
      v_fence_pos + v_water_pos + v_comfort_pos as features_static
    from per_hour
  ),
  scored as (
    select *,
      case when is_closed then 0
           else greatest(0, least(status_cap,
             50 + features_static + shade_pos + weather_pos + window_pos
                + v_agility_pos + water_play_pos
                - heat_uv_neg - harsh_neg - gotchas_neg + v_drive))
      end as score,
      case when is_closed then 'closed' else v1_status end as status_v2
    from composed
  )
  select jsonb_agg(jsonb_build_object(
    'local_hour', local_hour, 'hour_label', hour_label,
    'status', status_v2,
    'score', score, 'gate_fired', gate_fired, 'status_cap', status_cap,
    'is_closed', is_closed,
    'pos_weather', case when is_closed then 0 else weather_pos end,
    'pos_features', case when is_closed then 0 else features_static + shade_pos end,
    'pos_enrichment', case when is_closed then 0 else v_agility_pos + water_play_pos end,
    'pos_window', case when is_closed then 0 else window_pos end,
    'neg_heat_uv', case when is_closed then 0 else heat_uv_neg end,
    'neg_harsh',   case when is_closed then 0 else harsh_neg end,
    'neg_gotchas', case when is_closed then 0 else gotchas_neg end,
    'drive', case when is_closed then 0 else v_drive end,
    'pos_temp', case when is_closed then 0 else temp_pos end,
    'pos_wind', case when is_closed then 0 else wind_pos end,
    'pos_sky',  case when is_closed then 0 else sky_pos end,
    'pos_fence', case when is_closed then 0 else v_fence_pos end,
    'pos_water', case when is_closed then 0 else v_water_pos end,
    'pos_shade', case when is_closed then 0 else shade_pos end,
    'pos_comfort', case when is_closed then 0 else v_comfort_pos end,
    'pos_agility', case when is_closed then 0 else v_agility_pos end,
    'pos_water_play', case when is_closed then 0 else water_play_pos end,
    'feels_like', feels_like, 'wind_speed', wind_speed,
    'uv_index', uv_index, 'asphalt_temp', asphalt_temp,
    'weather_code', weather_code, 'precip_chance', precip_chance
  ) order by local_hour) into v_hours from scored;

  v_bw := public._find_best_window_v2(coalesce(v_hours, '[]'::jsonb), 70, 2, 5);

  return jsonb_build_object(
    'fid', p_fid, 'view_date', v_view_date,
    'open_hour', v_open_h, 'close_hour', v_close_h,
    'features_positive', v_fence_pos + v_water_pos + v_comfort_pos,
    'pos_fence', v_fence_pos, 'pos_water', v_water_pos, 'pos_comfort', v_comfort_pos,
    'pos_agility', v_agility_pos,
    'drive_factor', v_drive, 'drive_minutes', v_drive_min,
    'drive_miles', case when v_drive_miles is not null then round(v_drive_miles::numeric, 1) else null end,
    'best_window_label', v_today.best_window_label,
    'best_window_v2_label', v_bw->>'label',
    'best_window_v2_start', nullif(v_bw->>'start', '')::int,
    'best_window_v2_end',   nullif(v_bw->>'end',   '')::int,
    'best_window_v2_score', nullif(v_bw->>'score', '')::int,
    'closures', v_closures,
    'hours', coalesce(v_hours, '[]'::jsonb));
end;
$$;


-- ── Seed closures for known parks (manual; bulk extraction is a follow-up)

update public.dog_park_dog_policy
   set closures = '[{"kind":"weekly","weekday":"wed","all_day":true,"reason":"maintenance","evidence_quote":"closed every Wednesday for regular maintenance"}]'::jsonb
 where dog_park_fid = 518;  -- Costa Mesa Bark Park

update public.dog_park_dog_policy
   set closures = '[{"kind":"weekly_time","weekday":"tue","start":"06:00","end":"10:00","reason":"maintenance","evidence_quote":"closed Tuesdays 6am-10am for weekly maintenance"}]'::jsonb
 where dog_park_fid = 555;  -- Whitnall Highway (Los Angeles)

update public.dog_park_dog_policy
   set closures = '[{"kind":"weekly_time","weekday":"tue","start":"06:00","end":"09:00","reason":"maintenance","evidence_quote":"closed Tuesday 6am-9am for weekly maintenance"}]'::jsonb
 where dog_park_fid = 1830;  -- Wheaton Dog Park (MD)

update public.dog_park_dog_policy
   set closures = '[{"kind":"weekly_time","weekday":"fri","start":"06:00","end":"10:00","reason":"maintenance","evidence_quote":"closed Fridays 6am-10am for maintenance"}]'::jsonb
 where dog_park_fid = 1996;  -- Bark Park Riverside (MD)

commit;
notify pgrst, 'reload schema';
