-- 20260528_closures_seasonal.sql
--
-- Extend the closures substrate to be seasonal-aware:
--   - New closure kinds: 'daily_all', 'daily_time' (any day-of-week match)
--   - Optional season_from_md / season_to_md fields on any closure
--     (handles MM-DD ranges including year-wraparound like Oct-Mar)
--   - _v2_is_hour_closed updated to honor season scoping
--   - _beach_closures_from_policy rewritten to read zone_rules.regions[0]
--     .sections.sand.time_windows[] directly (preserving seasonal scope)
--
-- Motivation: Laguna sand-summer rule "Jun 15-Sep 10, 9am-6pm" was being
-- applied year-round because beach_dog_policy.dogs_prohibited_start/end
-- columns dropped the seasonal scoping when zone_rules was flattened.
-- Per Franz 2026-05-28: ship the seasonal-aware version.

begin;

-- ── _v2_is_hour_closed: add season-range check + new kinds ─────────

create or replace function public._v2_is_hour_closed(
  p_closures jsonb,
  p_date     date,
  p_hour     int
) returns boolean language sql stable as $$
  with ctx as (
    select case extract(isodow from p_date)::int
      when 1 then 'mon' when 2 then 'tue' when 3 then 'wed'
      when 4 then 'thu' when 5 then 'fri' when 6 then 'sat'
      when 7 then 'sun' end as day_str,
      to_char(p_date, 'MM-DD') as date_md
  ),
  matches as (
    select 1
    from jsonb_array_elements(coalesce(p_closures, '[]'::jsonb)) c, ctx
    where
      -- Day-of-week filter: weekly* kinds must match day; daily_* matches any
      (
        ((c->>'kind') like 'weekly%' and lower(c->>'weekday') = ctx.day_str)
        or ((c->>'kind') like 'daily%')
      )
      -- Time-of-day filter
      and (
        -- All-day kinds OR explicit all_day flag → match all hours
        ((c->>'kind') in ('weekly','daily_all'))
        or coalesce((c->>'all_day')::boolean, false)
        or (
          -- Time window check (start inclusive, end exclusive)
          p_hour >= extract(hour from (c->>'start')::time)::int
          and p_hour <  extract(hour from (c->>'end')::time)::int
        )
      )
      -- Season filter — only apply when both bounds present.
      -- Handles wraparound (e.g. season_from_md='10-15', season_to_md='03-31'
      -- = Oct 15 to Mar 31).
      and (
        (c->>'season_from_md') is null or (c->>'season_to_md') is null
        or case
          when (c->>'season_from_md') <= (c->>'season_to_md') then
            ctx.date_md >= (c->>'season_from_md') and ctx.date_md <= (c->>'season_to_md')
          else
            ctx.date_md >= (c->>'season_from_md') or ctx.date_md <= (c->>'season_to_md')
        end
      )
  )
  select exists(select 1 from matches)
$$;

comment on function public._v2_is_hour_closed is
  'Returns true if (date, hour) matches any closure entry in p_closures. '
  'Supported kinds: weekly (weekday all-day), weekly_time (weekday time window), '
  'daily_all (every day all-hours, optionally seasonal), daily_time (every day '
  'time window, optionally seasonal). Each entry may include season_from_md / '
  'season_to_md (MM-DD strings) to limit to a date range — supports wraparound.';


-- ── _beach_closures_from_policy: read zone_rules + sand section ────

drop function if exists public._beach_closures_from_policy(text, text, text);

create or replace function public._beach_closures_from_policy(
  p_dogs_allowed text,
  p_zone_rules   jsonb
) returns jsonb language plpgsql immutable as $$
declare
  v_closures jsonb := '[]'::jsonb;
  v_section jsonb;
  v_window jsonb;
  v_evidence_quote text;
  v_start text; v_end text;
  v_kind text;
begin
  -- 1. Hard block: dogs_allowed='no' → year-round all-day closure
  if p_dogs_allowed = 'no' then
    return '[{"kind":"daily_all","reason":"dogs_prohibited"}]'::jsonb;
  end if;

  -- 2. Parse zone_rules.regions[0].sections.sand.time_windows[]
  if p_zone_rules is null or jsonb_typeof(p_zone_rules) <> 'object' then
    return '[]'::jsonb;
  end if;

  v_section := p_zone_rules->'regions'->0->'sections'->'sand';
  if v_section is null then return '[]'::jsonb; end if;

  v_evidence_quote := v_section->'evidence'->>'quote';

  for v_window in select * from jsonb_array_elements(coalesce(v_section->'time_windows', '[]'::jsonb))
  loop
    -- Only emit closures for 'not_allowed' windows. ('on_leash' / 'off_leash'
    -- time_windows describe behaviour during open hours, not closures.)
    if (v_window->>'rule') = 'not_allowed' then
      v_start := v_window->>'start';
      v_end   := v_window->>'end';
      -- Pick kind based on time-window span
      if coalesce(v_start, '00:00') = '00:00'
         and coalesce(v_end, '23:59') in ('23:59','24:00') then
        v_kind := 'daily_all';
      else
        v_kind := 'daily_time';
      end if;
      v_closures := v_closures || jsonb_build_array(
        jsonb_build_object(
          'kind', v_kind,
          'start', case when v_kind = 'daily_time' then v_start end,
          'end',   case when v_kind = 'daily_time' then v_end end,
          'season_from_md', v_window->>'effective_from_md',
          'season_to_md',   v_window->>'effective_to_md',
          'reason', coalesce(v_window->>'window_kind', 'dogs_prohibited_sand'),
          'season_label', v_window->>'season_label',
          'evidence_quote', v_evidence_quote
        )
      );
    end if;
  end loop;

  return v_closures;
end;
$$;

comment on function public._beach_closures_from_policy is
  'Build closures jsonb from beach_dog_policy fields. Reads zone_rules.regions[0]'
  '.sections.sand.time_windows[] for time-windowed prohibitions (preserves '
  'seasonal scope via effective_from_md / effective_to_md). dogs_allowed=''no'' '
  'returns a single year-round daily_all closure.';

commit;
notify pgrst, 'reload schema';
