-- 20260506_promote_exploded_payloads.sql
--
-- Promote the json_structured_explode rows into canonical consumer columns:
--   1. hours_text__open / __close            → beaches_gold.open_time/close_time
--   2. dogs_allowed_areas__zones             → beach_dog_policy.dogs_allowed_areas
--   3. dogs_time_restrictions__prohibited_hours → beach_dog_policy.dogs_prohibited_start/end
--
-- Conservative semantics: only fill when the canonical column is null
-- (don't overwrite curator/operator-set values). Per-fid consensus picks
-- the longest parsed_value (assumed most informative) when multiple
-- exploded rows exist for the same key.

begin;

-- ----------------------------------------------------------------------
-- Helper: text "8 a.m." / "10:30 PM" / "noon" → "HH:MM" 24-hour
-- ----------------------------------------------------------------------
create or replace function public.parse_clock_to_24h(t text) returns text
language plpgsql immutable as $$
declare
  v text;
  m text[];
  hh int;
  mm int;
  ampm text;
begin
  if t is null then return null; end if;
  v := lower(btrim(t));
  if v = '' then return null; end if;
  if v = 'noon'      then return '12:00'; end if;
  if v = 'midnight'  then return '00:00'; end if;
  if v in ('sunrise','sunset','dawn','dusk') then return null; end if;

  m := regexp_match(v, '^(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?|am|pm)?');
  if m is null then return null; end if;
  hh := m[1]::int;
  mm := coalesce(m[2]::int, 0);
  ampm := coalesce(m[3], '');
  ampm := replace(replace(ampm, '.', ''), ' ', '');  -- normalize a.m. → am

  -- 12-hour to 24-hour conversion
  if ampm = 'pm' and hh < 12 then hh := hh + 12; end if;
  if ampm = 'am' and hh = 12 then hh := 0;       end if;

  if hh < 0 or hh > 23 or mm < 0 or mm > 59 then return null; end if;
  return lpad(hh::text, 2, '0') || ':' || lpad(mm::text, 2, '0');
end $$;

revoke all on function public.parse_clock_to_24h(text) from public, anon, authenticated;
grant  execute on function public.parse_clock_to_24h(text) to service_role;

-- ----------------------------------------------------------------------
-- 1) hours_text__open / __close → beaches_gold.open_time / close_time
-- ----------------------------------------------------------------------
with open_per_fid as (
  select distinct on (fid) fid, public.parse_clock_to_24h(parsed_value) as t
    from public.beach_policy_extractions
   where extraction_method = 'json_structured_explode'
     and field_name = 'hours_text__open'
     and parsed_value is not null
   order by fid, length(parsed_value) desc
),
close_per_fid as (
  select distinct on (fid) fid, public.parse_clock_to_24h(parsed_value) as t
    from public.beach_policy_extractions
   where extraction_method = 'json_structured_explode'
     and field_name = 'hours_text__close'
     and parsed_value is not null
   order by fid, length(parsed_value) desc
),
hrs_open_upd as (
  update public.beaches_gold bg
     set open_time = o.t
    from open_per_fid o
   where bg.fid = o.fid and bg.open_time is null and o.t is not null
   returning bg.fid
),
hrs_close_upd as (
  update public.beaches_gold bg
     set close_time = c.t
    from close_per_fid c
   where bg.fid = c.fid and bg.close_time is null and c.t is not null
   returning bg.fid
)
select 'hours' as step,
       (select count(*) from hrs_open_upd)  as open_filled,
       (select count(*) from hrs_close_upd) as close_filled;

-- ----------------------------------------------------------------------
-- 2) dogs_allowed_areas__zones → beach_dog_policy.dogs_allowed_areas
-- ----------------------------------------------------------------------
with zones_per_fid as (
  select distinct on (fid) fid, parsed_value as v
    from public.beach_policy_extractions
   where extraction_method = 'json_structured_explode'
     and field_name = 'dogs_allowed_areas__zones'
     and parsed_value is not null
   order by fid, length(parsed_value) desc
),
zones_upd as (
  update public.beach_dog_policy p
     set dogs_allowed_areas = z.v
    from zones_per_fid z
   where p.arena_group_id = z.fid
     and p.dogs_allowed_areas is null
   returning p.arena_group_id
)
select 'zones' as step, count(*) as zones_filled from zones_upd;

-- ----------------------------------------------------------------------
-- 3) prohibited_hours prose → dogs_prohibited_start/end
--    Regex extracts the time range. Date qualifier (June 15...September 10)
--    is left in raw — date ranges go to a different field if/when wired.
-- ----------------------------------------------------------------------
with prohib_per_fid as (
  select distinct on (fid) fid, parsed_value as raw,
         regexp_match(
           parsed_value,
           '(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|AM|PM))\s*(?:to|–|—|-|−)\s*(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|AM|PM))',
           'i'
         ) as m
    from public.beach_policy_extractions
   where extraction_method = 'json_structured_explode'
     and field_name = 'dogs_time_restrictions__prohibited_hours'
     and parsed_value is not null
   order by fid, length(parsed_value) desc
),
prohib_upd as (
  update public.beach_dog_policy p
     set dogs_prohibited_start = coalesce(p.dogs_prohibited_start, public.parse_clock_to_24h(pr.m[1])),
         dogs_prohibited_end   = coalesce(p.dogs_prohibited_end,   public.parse_clock_to_24h(pr.m[2]))
    from prohib_per_fid pr
   where p.arena_group_id = pr.fid
     and pr.m is not null
     and (p.dogs_prohibited_start is null or p.dogs_prohibited_end is null)
   returning p.arena_group_id
)
select 'prohibited_hours' as step, count(*) as prohib_filled from prohib_upd;

commit;
