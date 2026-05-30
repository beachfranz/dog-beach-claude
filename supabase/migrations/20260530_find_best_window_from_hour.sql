-- _find_best_window_v2 gains an optional p_from_hour parameter so future
-- callers (consumer-surface RPCs viewing today) can filter to "current
-- hour forward". Default NULL = full-day picker (today's nightly daily-
-- refresh stays full-day-best so find.html ranking + future-date cards
-- continue to use a stable anchor).
--
-- The frontend (beach.html / dog-park.html) implements the equivalent
-- current-hour filter in JS over the hours array returned by the RPC
-- so today's headline reflects remaining time. See applyBeachHourlyV2
-- + dog-park analog. SQL just stays consistent.

begin;

create or replace function public._find_best_window_v2(
  p_hours jsonb,
  p_min_score integer default 0,
  p_min_len integer default 2,
  p_max_len integer default 5,
  p_from_hour integer default null
) returns jsonb
language plpgsql
stable
as $$
declare
  v_start int; v_end int; v_score int; v_label text;
begin
  with hours_data as (
    select (h->>'local_hour')::int as hr,
           (h->>'score')::int as score,
           coalesce((h->>'gate_fired')::boolean, false) as gate_fired,
           coalesce(h->>'status', '') as status,
           coalesce((h->>'is_closed')::boolean, false) as is_closed
      from jsonb_array_elements(p_hours) h
  ),
  qualifying as (
    select hr, score, hr - row_number() over (order by hr) as grp
      from hours_data
     where score >= p_min_score
       and not gate_fired
       and status != 'closed'
       and not is_closed
       and (p_from_hour is null or hr >= p_from_hour)
  ),
  runs as (
    select min(hr) as start_h, max(hr) as end_h,
           count(*)::int as len,
           max(score)::int as max_score,
           avg(score)::numeric as avg_score
      from qualifying
     group by grp
     having count(*) >= p_min_len
  ),
  best_run as (
    select start_h, end_h, len, max_score from runs
     order by avg_score desc, len desc, start_h
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
                   and coalesce(h2->>'status', '') != 'closed'
                   and not coalesce((h2->>'is_closed')::boolean, false)
               ) as window_sum
        from generate_series(v_start, v_end - p_max_len + 1) s(sub_start)
      ) candidates
      order by window_sum desc nulls last
      limit 1;
    v_end := v_start + p_max_len - 1;
  end if;

  v_label := public._format_hour_range(v_start, v_end + 1);
  return jsonb_build_object('label', v_label, 'start', v_start, 'end', v_end, 'score', v_score);
end;
$$;

commit;
