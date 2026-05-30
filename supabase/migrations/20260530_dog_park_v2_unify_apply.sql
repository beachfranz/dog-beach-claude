-- 20260530_dog_park_v2_unify_apply.sql
--
-- Unify the dog-park apply_v2 path: extend the existing
-- apply_v2_best_window_to_recommendations function (already called by
-- daily-dog-park-refresh) to ALSO write the v2 day-level + per-hour
-- columns added by 20260530_dog_park_v2_day_level.sql. Drop the
-- redundant apply_v2_best_window_to_dog_park_recommendations function
-- from that same migration since the existing name is what callers use.

begin;

drop function if exists public.apply_v2_best_window_to_dog_park_recommendations(bigint, date);

create or replace function public.apply_v2_best_window_to_recommendations(
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
         best_window_start_ts = case when v_start is not null
                                     then (p_date::timestamp + (v_start || ' hours')::interval) at time zone 'UTC'
                                     else best_window_start_ts end,
         best_window_end_ts   = case when v_end is not null
                                     then (p_date::timestamp + ((v_end + 1) || ' hours')::interval) at time zone 'UTC'
                                     else best_window_end_ts end,
         day_status_v2        = v_day_status_v2,
         composite_score_v2   = v_composite_v2
   where dog_park_fid = p_fid and local_date = p_date;
end;
$$;

commit;
