-- 20260530_dog_park_v2_day_level.sql
--
-- Mirror of 20260530_v2_day_level_columns.sql for the dog-park side.
-- Task #9 of v1-retirement track. Adds v2 hour + day columns to
-- dog_park_day_hourly_scores + dog_park_day_recommendations, the
-- dog-park flavor of v2_compute_hour_status, and an apply_v2 wrapper
-- daily-dog-park-refresh / get-dog-park-now will invoke.
--
-- Dog parks differ from beaches in two ways:
--   * No tide / sand / no crowd (dog parks are off-leash by definition,
--     so crowd isn't a risk signal — busier means more dogs to socialize)
--   * Different scoring_config_v2 bands per signal (e.g. asphalt_neg
--     max penalty is 15 for dog_park vs 8 for beach — dog parks penalize
--     hot asphalt harder because paws spend more time on it)
--
-- The composer reads entity_type='dog_park' from scoring_config_v2 so
-- bands are correctly scaled.

begin;

-- ── Columns ──────────────────────────────────────────────────────────
alter table public.dog_park_day_hourly_scores
  add column if not exists hour_score_v2 int;

alter table public.dog_park_day_recommendations
  add column if not exists day_status_v2 text,
  add column if not exists composite_score_v2 int;

comment on column public.dog_park_day_hourly_scores.hour_score_v2 is
  'v2 per-hour score from compute_dog_park_hourly_v2 (0-100). Set by apply_v2_best_window_to_dog_park_recommendations.';
comment on column public.dog_park_day_recommendations.day_status_v2 is
  'v2 day status: worst v2 hour_status across daylight + non-closed hours. {clear|advisory|caution|no_go|closed}.';
comment on column public.dog_park_day_recommendations.composite_score_v2 is
  'v2 day-level composite score: avg hour_score_v2 across best-window hours. 0-100.';

-- ── v2_compute_hour_status_dog_park ──────────────────────────────────
-- Worst severity across dog-park-relevant signals for one hour. Drops
-- tide / sand / crowd (irrelevant for dog parks) and adds nothing new;
-- the heat / UV / wind / precip / feels-like axes are all that matter.
create or replace function public.v2_compute_hour_status_dog_park(
  p_uv          numeric,
  p_asphalt     numeric,
  p_wind        numeric,
  p_precip      numeric,
  p_feels_like  numeric,
  p_is_closed   boolean default false
) returns text
language plpgsql
stable
as $$
declare
  v_worst_rank int := 0;
  v_rank int;
  s text;
begin
  if p_is_closed then return 'closed'; end if;

  for s in select unnest(array[
    public.v2_signal_status('dog_park', 'uv_neg',          p_uv),
    public.v2_signal_status('dog_park', 'asphalt_neg',     p_asphalt),
    public.v2_signal_status('dog_park', 'wind_harsh_neg',  p_wind),
    public.v2_signal_status('dog_park', 'precip_chance',   p_precip),
    public.v2_signal_status('dog_park', 'feels_like_hot',  p_feels_like),
    public.v2_signal_status('dog_park', 'feels_like_cold', p_feels_like)
  ])
  loop
    v_rank := case s
      when 'no_go'    then 3
      when 'caution'  then 2
      when 'advisory' then 1
      when 'clear'    then 0
      else 0
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

comment on function public.v2_compute_hour_status_dog_park(numeric,numeric,numeric,numeric,numeric,boolean) is
  'Dog-park flavor of v2_compute_hour_status: composes worst severity across UV, asphalt, wind, precip, feels_like signals (no tide/sand/crowd). Uses entity_type=dog_park bands.';

-- ── apply_v2_best_window_to_dog_park_recommendations ─────────────────
create or replace function public.apply_v2_best_window_to_dog_park_recommendations(
  p_fid  bigint,
  p_date date default current_date
) returns void
language plpgsql
as $$
declare
  v_result jsonb;
  v_label text; v_start int; v_end int; v_v2_score int;
  v_day_status_v2 text;
  v_composite_v2 int;
  h jsonb;
begin
  v_result := public.compute_dog_park_hourly_v2(p_fid, null, null, p_date);
  v_label    := v_result->>'best_window_v2_label';
  v_start    := nullif(v_result->>'best_window_v2_start', '')::int;
  v_end      := nullif(v_result->>'best_window_v2_end',   '')::int;
  v_v2_score := nullif(v_result->>'best_window_v2_score', '')::int;

  -- Per-hour v2 score backfill
  for h in select * from jsonb_array_elements(coalesce(v_result->'hours', '[]'::jsonb))
  loop
    update public.dog_park_day_hourly_scores
       set hour_score_v2 = nullif(h->>'score', '')::int
     where dog_park_fid = p_fid
       and local_date   = p_date
       and local_hour   = (h->>'local_hour')::int;
  end loop;

  -- Aggregate day status from raw values
  select case max(case st
      when 'no_go'    then 3
      when 'caution'  then 2
      when 'advisory' then 1
      else 0
    end)
    when 3 then 'no_go'
    when 2 then 'caution'
    when 1 then 'advisory'
    else 'clear'
  end
    into v_day_status_v2
    from (
      select public.v2_compute_hour_status_dog_park(
        h.uv_index::numeric,
        h.asphalt_temp::numeric,
        h.wind_speed::numeric,
        h.precip_chance::numeric,
        h.feels_like::numeric,
        false
      ) as st
      from public.dog_park_day_hourly_scores h
      where h.dog_park_fid = p_fid
        and h.local_date   = p_date
        and h.is_daylight  = true
    ) q;

  if v_v2_score is not null then
    v_composite_v2 := v_v2_score;
  else
    select avg(hour_score_v2)::int
      into v_composite_v2
      from public.dog_park_day_hourly_scores
     where dog_park_fid = p_fid
       and local_date   = p_date
       and is_daylight  = true
       and hour_score_v2 is not null;
  end if;

  update public.dog_park_day_recommendations
     set best_window_label    = coalesce(v_label, best_window_label),
         day_status_v2        = v_day_status_v2,
         composite_score_v2   = v_composite_v2
   where dog_park_fid = p_fid and local_date = p_date;
end;
$$;

commit;
