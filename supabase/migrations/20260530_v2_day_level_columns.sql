-- 20260530_v2_day_level_columns.sql
--
-- v2 day-level + per-hour status columns + composer helper. Task #2 of
-- v1-retirement track (2026-05-30). Builds on [[v2-signal-status-mapping]]
-- (migration 20260530_v2_signal_status_helper.sql) by exposing v2 severity
-- at the per-hour AND per-day grain so downstream consumers (find_beaches
-- RPC, find.html, beach.html alternatives gate, Scout prompts) can read
-- v2 directly without recomputing.
--
-- Columns added:
--   public.beach_day_hourly_scores
--     hour_score_v2   int   -- per-hour v2 numeric score from
--                              compute_beach_hourly_v2 ('score' field)
--   public.beach_day_recommendations
--     day_status_v2          text  -- worst v2 hour status across daylight
--     composite_score_v2     int   -- avg hour_score_v2 across best window
--
-- Helpers:
--   v2_compute_hour_status(uv, asphalt, sand, tide, wind, crowd, precip,
--                          feels_like, is_closed)
--                          → text
--     Worst v2 severity across all signals for one hour. Closed hours
--     return 'closed'.
--
--   v2_compute_day_status(p_fid, p_date)
--                          → text
--     Walks beach_day_hourly_scores rows for the day, picks worst v2
--     hour_status across daylight + non-closed hours. Skips hours where
--     all raw values are NULL.
--
-- apply_v2_best_window_to_beach_recommendations is extended to also
-- populate hour_score_v2, day_status_v2, composite_score_v2 on every
-- invocation. Already called by:
--   - daily-beach-refresh (after v1 daily upsert)
--   - get-beach-now (after hourly NOW upsert, per 2026-05-30 staleness fix)
-- so v2 fields stay fresh on the same cadence.

begin;

-- ── Columns ──────────────────────────────────────────────────────────
alter table public.beach_day_hourly_scores
  add column if not exists hour_score_v2 int;

alter table public.beach_day_recommendations
  add column if not exists day_status_v2 text,
  add column if not exists composite_score_v2 int;

comment on column public.beach_day_hourly_scores.hour_score_v2 is
  'v2 per-hour numeric score from compute_beach_hourly_v2 (0-100). Set by apply_v2_best_window_to_beach_recommendations.';
comment on column public.beach_day_recommendations.day_status_v2 is
  'v2 day status: worst v2 hour_status across daylight + non-closed hours. {clear|advisory|caution|no_go|closed}.';
comment on column public.beach_day_recommendations.composite_score_v2 is
  'v2 day-level composite score: avg hour_score_v2 across best-window hours. 0-100.';

-- ── v2_compute_hour_status ───────────────────────────────────────────
-- Worst severity across signals for one hour. Drives day_status_v2.
create or replace function public.v2_compute_hour_status(
  p_uv          numeric,
  p_asphalt     numeric,
  p_sand        numeric,
  p_tide        numeric,
  p_wind        numeric,
  p_crowd       numeric,
  p_precip      numeric,
  p_feels_like  numeric,
  p_is_closed   boolean default false
) returns text
language plpgsql
stable
as $$
declare
  v_worst_rank int := 0;  -- 0=clear, 1=advisory, 2=caution, 3=no_go
  v_rank int;
  s text;
begin
  if p_is_closed then return 'closed'; end if;

  -- Per-signal status; keep the worst
  for s in select unnest(array[
    public.v2_signal_status('beach', 'uv_neg',          p_uv),
    public.v2_signal_status('beach', 'asphalt_neg',     p_asphalt),
    public.v2_signal_status('beach', 'sand_temp_neg',   p_sand),
    public.v2_signal_status('beach', 'tide_neg',        p_tide),
    public.v2_signal_status('beach', 'wind_harsh_neg',  p_wind),
    public.v2_signal_status('beach', 'crowd_neg',       p_crowd),
    public.v2_signal_status('beach', 'precip_chance',   p_precip),
    public.v2_signal_status('beach', 'feels_like_hot',  p_feels_like),
    public.v2_signal_status('beach', 'feels_like_cold', p_feels_like)
  ])
  loop
    v_rank := case s
      when 'no_go'    then 3
      when 'caution'  then 2
      when 'advisory' then 1
      when 'clear'    then 0
      else 0  -- NULL → clear (signal not measurable)
    end;
    if v_rank > v_worst_rank then v_worst_rank := v_rank; end if;
  end loop;

  return case v_worst_rank
    when 3 then 'no_go'
    when 2 then 'caution'
    when 1 then 'advisory'
    else        'clear'
  end;
end;
$$;

comment on function public.v2_compute_hour_status(numeric,numeric,numeric,numeric,numeric,numeric,numeric,numeric,boolean) is
  'Composes worst v2 severity tier across all per-hour signals. Returns clear|advisory|caution|no_go|closed. Used by v2_compute_day_status and the per-hour writer.';

-- ── v2_compute_day_status ────────────────────────────────────────────
-- Walks the day's hourly_scores rows and returns the worst v2 status
-- across daylight hours. Beach_day_hourly_scores does NOT have an
-- is_closed column — closures live in dog_policy.zone_rules and are
-- evaluated by _v2_is_hour_closed at query time. Caller can detect
-- "closed today" separately via the closures jsonb returned by
-- compute_beach_hourly_v2.
create or replace function public.v2_compute_day_status(
  p_fid  bigint,
  p_date date
) returns text
language sql
stable
as $$
  with hourly as (
    select public.v2_compute_hour_status(
        h.uv_index::numeric,
        h.asphalt_temp::numeric,
        h.sand_temp::numeric,
        h.tide_height::numeric,
        h.wind_speed::numeric,
        h.busyness_score::numeric,
        h.precip_chance::numeric,
        h.feels_like::numeric,
        false
      ) as st
    from public.beach_day_hourly_scores h
    where h.arena_group_id = p_fid
      and h.local_date     = p_date
      and h.is_daylight    = true
  ),
  worst as (
    select max(case st
      when 'no_go'    then 3
      when 'caution'  then 2
      when 'advisory' then 1
      else 0
    end) as r
    from hourly
  )
  select case (select r from worst)
    when 3 then 'no_go'
    when 2 then 'caution'
    when 1 then 'advisory'
    else        'clear'
  end;
$$;

comment on function public.v2_compute_day_status(bigint,date) is
  'Aggregates worst v2 hour_status across daylight hours. Returns clear|advisory|caution|no_go. Closure detection is separate (see compute_beach_hourly_v2.closures).';

-- ── Extend apply_v2_best_window_to_beach_recommendations ─────────────
-- Now writes hour_score_v2 per hour, day_status_v2 + composite_score_v2
-- per day, in addition to the existing best_window_* fields.
create or replace function public.apply_v2_best_window_to_beach_recommendations(
  p_fid  bigint,
  p_date date default current_date
) returns void
language plpgsql
as $$
declare
  v_result jsonb;
  v_label text; v_start int; v_end int; v_v2_score int;
  v_location_id text;
  v_day_status_v2 text;
  v_composite_v2 int;
  h jsonb;
begin
  select location_id into v_location_id from public.beaches_gold where fid = p_fid;
  if v_location_id is null then return; end if;

  v_result := public.compute_beach_hourly_v2(p_fid, null, null, p_date);
  v_label    := v_result->>'best_window_v2_label';
  v_start    := nullif(v_result->>'best_window_v2_start', '')::int;
  v_end      := nullif(v_result->>'best_window_v2_end',   '')::int;
  v_v2_score := nullif(v_result->>'best_window_v2_score', '')::int;

  -- Write per-hour v2 scores (already computed inside compute_beach_hourly_v2)
  for h in select * from jsonb_array_elements(coalesce(v_result->'hours', '[]'::jsonb))
  loop
    update public.beach_day_hourly_scores
       set hour_score_v2 = nullif(h->>'score', '')::int
     where arena_group_id = p_fid
       and local_date     = p_date
       and local_hour     = (h->>'local_hour')::int;
  end loop;

  -- v2 day status from hour-level aggregation
  v_day_status_v2 := public.v2_compute_day_status(p_fid, p_date);

  -- v2 composite is the best-window-v2 score. If no v2 window picked,
  -- fall back to avg hour_score_v2 across daylight hours so find-page
  -- ranking still has a number to sort on. (Closure handling lives in
  -- compute_beach_hourly_v2; closed hours already get score=0 there.)
  if v_v2_score is not null then
    v_composite_v2 := v_v2_score;
  else
    select avg(hour_score_v2)::int
      into v_composite_v2
      from public.beach_day_hourly_scores
     where arena_group_id = p_fid
       and local_date     = p_date
       and is_daylight    = true
       and hour_score_v2 is not null;
  end if;

  update public.beach_day_recommendations
     set best_window_label    = v_label,
         best_window_start_ts = case when v_start is not null
                                     then (p_date::timestamp + (v_start || ' hours')::interval) at time zone 'UTC'
                                     else null end,
         best_window_end_ts   = case when v_end is not null
                                     then (p_date::timestamp + ((v_end + 1) || ' hours')::interval) at time zone 'UTC'
                                     else null end,
         day_status_v2        = v_day_status_v2,
         composite_score_v2   = v_composite_v2
   where location_id = v_location_id and local_date = p_date;
end;
$$;

commit;
