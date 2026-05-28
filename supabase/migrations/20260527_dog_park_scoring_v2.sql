-- 20260527_dog_park_scoring_v2.sql
--
-- Dog-park scoring v2. Replaces the v1 weighted-sum composite (which was a
-- beach algorithm retrofitted to dog parks) with a baseline+positives−negatives
-- model + status gate. Per project_dog_park_scoring_v2_spec.md 2026-05-27.
--
-- Two functions:
--   1. public._score_v2_compose(...) — GENERIC composer used by both
--      dog-park and (future) beach v2 scorers. Pure math on jsonb inputs.
--   2. public.compute_dog_park_score_v2(p_fid, p_user_lat, p_user_lng, p_date)
--      — domain function. Pulls park identity + policy + now + today,
--      computes per-component values, calls the composer.
--
-- Beach v2 (Franz: "very soon") will add compute_beach_score_v2() with its
-- own positives/negatives (tide, surf, bacteria, etc.) and reuse the
-- composer verbatim.

begin;

-- ════════════════════════════════════════════════════════════════════
-- Generic v2 score composer
-- ════════════════════════════════════════════════════════════════════

create or replace function public._score_v2_compose(
  p_baseline           integer default 50,
  p_positives          jsonb   default '{}'::jsonb,   -- {name: {value, max}}
  p_negatives          jsonb   default '{}'::jsonb,   -- {name: {value, max}}
  p_drive_factor       integer default 0,
  p_status_gate_fired  boolean default false,
  p_status_gate_cap    integer default 30
) returns jsonb
language sql immutable
as $$
  with
    pos as (
      select coalesce(sum(least((v->>'value')::int, (v->>'max')::int)), 0) as total
        from jsonb_each(p_positives) as x(k, v)
    ),
    neg as (
      select coalesce(sum(least((v->>'value')::int, (v->>'max')::int)), 0) as total
        from jsonb_each(p_negatives) as x(k, v)
    ),
    raw_score as (
      select p_baseline + pos.total - neg.total + p_drive_factor as s,
             pos.total as pos_total, neg.total as neg_total
        from pos, neg
    ),
    capped as (
      select s as s_uncapped, pos_total, neg_total,
             greatest(0, least(100,
               case when p_status_gate_fired
                    then least(s, p_status_gate_cap)
                    else s end
             )) as s
        from raw_score
    )
  select jsonb_build_object(
    'score',             c.s,
    'score_uncapped',    c.s_uncapped,
    'baseline',          p_baseline,
    'positives',         p_positives,
    'negatives',         p_negatives,
    'positives_total',   c.pos_total,
    'negatives_total',   c.neg_total,
    'drive_factor',      p_drive_factor,
    'status_gate_fired', p_status_gate_fired,
    'status_gate_cap',   p_status_gate_cap
  ) from capped c;
$$;

comment on function public._score_v2_compose is
  'Generic v2 score composer. Baseline + positives - negatives + drive, '
  'optionally capped by status gate. Reusable across entity types '
  '(dog-park v2 + beach v2). Positives/negatives shape: {name: {value, max}} '
  'where each value is clamped to max before summing.';


-- ════════════════════════════════════════════════════════════════════
-- Dog-park v2 scorer
-- ════════════════════════════════════════════════════════════════════

create or replace function public.compute_dog_park_score_v2(
  p_fid       bigint,
  p_user_lat  double precision default null,
  p_user_lng  double precision default null,
  p_date      date default null
) returns jsonb
language plpgsql stable
as $$
declare
  v_gold      record;
  v_policy    record;
  v_now       record;
  v_today     record;
  v_view_date date := coalesce(p_date, current_date);
  v_baseline  integer := 50;
  v_pos       jsonb;
  v_neg       jsonb;
  v_drive     integer := 0;
  v_drive_miles  numeric;
  v_drive_min    integer;
  v_gate_fired   boolean := false;
  v_weather_pos  integer := 0;
  v_features_pos integer := 0;
  v_window_pos   integer := 0;
  v_heat_neg     integer := 0;
  v_harsh_neg    integer := 0;
  v_gotchas_neg  integer := 0;
  v_fence        boolean;
  v_water        boolean;
  v_surface      text;
begin
  -- ── Fetch park + policy + live row + today's rollup ───────────────
  select * into v_gold from public.dog_parks_gold where fid = p_fid;
  if v_gold.fid is null then
    return jsonb_build_object('error', 'park not found', 'fid', p_fid);
  end if;

  select * into v_policy from public.dog_park_dog_policy
   where dog_park_fid = p_fid limit 1;
  select * into v_now from public.dog_park_day_hourly_scores
   where dog_park_fid = p_fid and is_now = true limit 1;
  select * into v_today from public.dog_park_day_recommendations
   where dog_park_fid = p_fid and local_date = v_view_date limit 1;

  v_fence   := coalesce(v_policy.has_fence, v_gold.has_fence);
  v_water   := coalesce(v_policy.has_drinking_water, v_gold.has_drinking_water);
  v_surface := lower(coalesce(v_policy.surface_overlay, v_gold.surface, ''));

  -- ── ☀️  Weather positive (0..22) ──────────────────────────────────
  if v_now.feels_like is not null then
    if v_now.feels_like between 60 and 75 then v_weather_pos := v_weather_pos + 10; end if;
  end if;
  if v_now.wind_speed is not null and v_now.wind_speed < 8 then
    v_weather_pos := v_weather_pos + 6;
  end if;
  if v_now.weather_code is not null and v_now.weather_code between 0 and 2 then
    v_weather_pos := v_weather_pos + 6;
  end if;

  -- ── 🏞️  Park features positive (0..20) ───────────────────────────
  if v_fence = true                       then v_features_pos := v_features_pos + 6; end if;
  if v_water = true                       then v_features_pos := v_features_pos + 5; end if;
  if v_policy.double_gate = true          then v_features_pos := v_features_pos + 3; end if;
  if v_surface = 'grass'                  then v_features_pos := v_features_pos + 3; end if;
  if v_policy.small_dog_area = true
     or v_policy.large_dog_area = true    then v_features_pos := v_features_pos + 2; end if;
  if v_gold.area_m2 is not null
     and v_gold.area_m2 >= 4047            then v_features_pos := v_features_pos + 1; end if;  -- 1 acre

  -- ── ⏰  Window-now positive (0..5) ────────────────────────────────
  if v_today.best_window_label is not null then
    -- TODO refine: parse best_window_start_ts/end_ts vs current local hour
    v_window_pos := 5;
  elsif coalesce(v_today.go_hours_count, 0) > 0 then
    v_window_pos := 2;
  end if;

  -- ── 🔥  Heat/UV negative (0..25) ──────────────────────────────────
  -- Asphalt-based paw burn risk
  if v_now.asphalt_temp is not null then
    if v_now.asphalt_temp >= 125 then
      v_heat_neg := v_heat_neg + 15;
      v_gate_fired := true;
    elsif v_now.asphalt_temp >= 115 then
      v_heat_neg := v_heat_neg + 15;
    elsif v_now.asphalt_temp >= 105 then
      v_heat_neg := v_heat_neg + 8;
    end if;
  end if;
  -- UV danger
  if v_now.uv_index is not null then
    if v_now.uv_index >= 11 then
      v_heat_neg := v_heat_neg + 10;
      v_gate_fired := true;
    elsif v_now.uv_index >= 8 then
      v_heat_neg := v_heat_neg + 5;
    end if;
  end if;
  -- Compound heat
  if v_now.feels_like is not null and v_now.feels_like >= 95 then
    v_heat_neg := v_heat_neg + 5;
  end if;
  v_heat_neg := least(v_heat_neg, 25);

  -- ── 💨  Weather harsh negative (0..15) ───────────────────────────
  if v_now.wind_speed is not null then
    if v_now.wind_speed >= 25 then v_harsh_neg := v_harsh_neg + 10;
    elsif v_now.wind_speed >= 15 then v_harsh_neg := v_harsh_neg + 5;
    end if;
  end if;
  if v_now.precip_chance is not null and v_now.precip_chance >= 70 then
    v_harsh_neg := v_harsh_neg + 10;
  end if;
  if v_now.feels_like is not null then
    if v_now.feels_like <= 20 then
      v_harsh_neg := v_harsh_neg + 10;
      v_gate_fired := true;
    elsif v_now.feels_like <= 35 then
      v_harsh_neg := v_harsh_neg + 5;
    end if;
  end if;
  v_harsh_neg := least(v_harsh_neg, 15);

  -- ── ⚠️   Park gotchas negative (0..10) ───────────────────────────
  if v_fence = false then v_gotchas_neg := v_gotchas_neg + 5; end if;
  if v_water = false then v_gotchas_neg := v_gotchas_neg + 3; end if;
  if v_surface in ('asphalt', 'concrete')
     and coalesce(v_now.uv_index, 0) >= 6 then
    v_gotchas_neg := v_gotchas_neg + 3;
  end if;
  v_gotchas_neg := least(v_gotchas_neg, 10);

  -- ── 🚗  Drive time factor (-10..+3) ──────────────────────────────
  if p_user_lat is not null and p_user_lng is not null
     and v_gold.lat is not null and v_gold.lon is not null then
    v_drive_miles := st_distance(
      st_setsrid(st_makepoint(p_user_lng, p_user_lat), 4326)::geography,
      st_setsrid(st_makepoint(v_gold.lon, v_gold.lat), 4326)::geography
    ) / 1609.344;
    v_drive_min := round(v_drive_miles * 1.8)::integer;
    if v_drive_min <= 10 then v_drive := 3;
    elsif v_drive_min <= 25 then v_drive := 0;
    elsif v_drive_min <= 45 then v_drive := -3;
    elsif v_drive_min <= 75 then v_drive := -6;
    else                       v_drive := -10;
    end if;
  end if;

  -- ── Status gate: existing hour_status drives the safety cap ──────
  if v_now.hour_status = 'no_go' then v_gate_fired := true; end if;

  -- ── Build positives + negatives jsonb ────────────────────────────
  v_pos := jsonb_build_object(
    'weather',  jsonb_build_object('value', v_weather_pos,  'max', 22),
    'features', jsonb_build_object('value', v_features_pos, 'max', 20),
    'window',   jsonb_build_object('value', v_window_pos,   'max', 5)
  );
  v_neg := jsonb_build_object(
    'heat_uv',       jsonb_build_object('value', v_heat_neg,    'max', 25),
    'weather_harsh', jsonb_build_object('value', v_harsh_neg,   'max', 15),
    'gotchas',       jsonb_build_object('value', v_gotchas_neg, 'max', 10)
  );

  -- ── Compose final + return enriched payload ──────────────────────
  return public._score_v2_compose(
    p_baseline          := v_baseline,
    p_positives         := v_pos,
    p_negatives         := v_neg,
    p_drive_factor      := v_drive,
    p_status_gate_fired := v_gate_fired,
    p_status_gate_cap   := 30
  ) || jsonb_build_object(
    'fid',             p_fid,
    'view_date',       v_view_date,
    'drive_minutes',   v_drive_min,
    'drive_miles',     case when v_drive_miles is not null
                            then round(v_drive_miles::numeric, 1) else null end,
    'now_available',   v_now.dog_park_fid is not null,
    'today_available', v_today.dog_park_fid is not null,
    'hour_status',     v_now.hour_status,
    'best_window_label', v_today.best_window_label
  );
end;
$$;

comment on function public.compute_dog_park_score_v2 is
  'Dog-park scoring v2 (Franz 2026-05-27). Returns jsonb with score (0-100) '
  'plus per-component contributions for stacked-bar rendering. Drive time '
  'folds in only when user_lat/user_lng provided; otherwise neutral. '
  'Status gate caps score at 30 when any safety-critical metric fires no_go '
  '(asphalt>=125, UV>=11, feels_like<=20, etc). Spec: '
  '[[dog-park-scoring-v2-spec]] in memory.';


-- ════════════════════════════════════════════════════════════════════
-- nearest_dog_parks — embed v2 score + components
-- ════════════════════════════════════════════════════════════════════

-- Drop existing variants before CREATE — return signature changed
-- (added score_v2, score_v2_components, drive_minutes cols).
drop function if exists public.nearest_dog_parks(double precision, double precision, integer, text, date);
drop function if exists public.nearest_dog_parks(double precision, double precision, integer, text);

create or replace function public.nearest_dog_parks(
  p_lat   double precision default null,
  p_lng   double precision default null,
  p_limit integer default 10,
  p_state text default null,
  p_date  date default null
) returns table(
  fid bigint,
  name text,
  state text,
  address_city text,
  lat double precision,
  lon double precision,
  distance_miles double precision,
  has_fence boolean,
  has_drinking_water boolean,
  surface text,
  leash_policy text,
  hours_text text,
  hours_open_time text,
  hours_close_time text,
  additional_rules text,
  source text,
  source_url text,
  consensus_confidence numeric,
  operator_id bigint,
  operator_name text,
  operator_short text,
  composite_score numeric,
  day_status text,
  best_window_label text,
  best_window_status text,
  go_hours_count integer,
  summary_weather text,
  weather_code integer,
  avg_temp numeric,
  avg_wind numeric,
  -- v2 additions
  score_v2 integer,
  score_v2_components jsonb,
  drive_minutes integer
)
language sql stable parallel safe
as $function$
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
    dr.day_status, dr.best_window_label, dr.best_window_status, dr.go_hours_count,
    dr.summary_weather, dr.weather_code, dr.avg_temp, dr.avg_wind,
    -- v2
    (sv.payload->>'score')::int as score_v2,
    sv.payload as score_v2_components,
    (sv.payload->>'drive_minutes')::int as drive_minutes
  from public.dog_parks_gold g
  join public.dog_park_dog_policy p on p.dog_park_fid = g.fid
  cross join user_pt up
  left join public.operator o on o.id = p.operator_id
  left join public.dog_park_day_recommendations dr
         on dr.dog_park_fid = g.fid and dr.local_date = up.d
  left join lateral (
    select round(avg(h.hour_score)::numeric, 0) as composite_score
      from public.dog_park_day_hourly_scores h
     where h.dog_park_fid = g.fid
       and h.local_date = up.d
       and h.hour_score is not null
  ) hs on true
  left join lateral (
    select public.compute_dog_park_score_v2(g.fid, p_lat, p_lng, up.d) as payload
  ) sv on true
  where g.is_active = true
    and (p_state is null or g.state = p_state)
  order by
    -- v2 default rank: by score desc (with drive folded in). Falls back
    -- to distance ordering when v2 unavailable.
    coalesce((sv.payload->>'score')::int, 0) desc,
    case when up.pt is not null then st_distance(up.pt, g.geom::geography) end nulls last,
    g.name
  limit greatest(coalesce(p_limit, 10), 1);
$function$;

commit;
notify pgrst, 'reload schema';
