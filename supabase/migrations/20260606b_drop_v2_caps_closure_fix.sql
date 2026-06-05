-- 2026-06-06 follow-up — closed hours must not poison day_status_v2.
--
-- 20260606_drop_v2_caps.sql wrote hour_score_v2 = 0 for closed hours (because
-- compute_*_hourly_v2 emits score=0 when is_closed). Day-status then read
-- 0 as <30 → no_go, regressing beaches with curated mid-day closures
-- (canonical case: La Jolla Shores, dog-prohibited 9am-6pm).
--
-- Fix: write NULL for closed hours in hour_score_v2. The day-status
-- aggregator already filters `hour_score_v2 IS NOT NULL`, so closed hours
-- drop out cleanly.

CREATE OR REPLACE FUNCTION public.apply_v2_best_window_to_recommendations(
  p_fid bigint, p_date date DEFAULT CURRENT_DATE
) RETURNS void LANGUAGE plpgsql AS $function$
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

  for h in select * from jsonb_array_elements(coalesce(v_result->'hours', '[]'::jsonb))
  loop
    update public.dog_park_day_hourly_scores
       set hour_score_v2 = case when (h->>'is_closed')::boolean = true
                                then null
                                else nullif(h->>'score', '')::int end
     where dog_park_fid = p_fid
       and local_date   = p_date
       and local_hour   = (h->>'local_hour')::int;
  end loop;

  select case max(case public._v2_status_from_score(h.hour_score_v2)
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
    from public.dog_park_day_hourly_scores h
   where h.dog_park_fid = p_fid
     and h.local_date   = p_date
     and h.is_daylight  = true
     and h.hour_score_v2 is not null;

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
$function$;

CREATE OR REPLACE FUNCTION public.apply_v2_best_window_to_beach_recommendations(
  p_fid bigint, p_date date DEFAULT CURRENT_DATE
) RETURNS void LANGUAGE plpgsql AS $function$
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

  for h in select * from jsonb_array_elements(coalesce(v_result->'hours', '[]'::jsonb))
  loop
    update public.beach_day_hourly_scores
       set hour_score_v2 = case when (h->>'is_closed')::boolean = true
                                then null
                                else nullif(h->>'score', '')::int end
     where arena_group_id = p_fid
       and local_date     = p_date
       and local_hour     = (h->>'local_hour')::int;
  end loop;

  select case max(case public._v2_status_from_score(h.hour_score_v2)
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
    from public.beach_day_hourly_scores h
   where h.arena_group_id = p_fid
     and h.local_date     = p_date
     and h.is_daylight    = true
     and h.hour_score_v2 is not null;

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
$function$;
