-- 20260509_policy_seed_phase_helpers.sql
--
-- SQL helpers and a unified seasonal-closure seed table to support
-- three new pipeline phases inserted between `precheck` and `operators`:
--
--   1. state_policy_seed     — assert state_dogs_policy has a row for
--                              the state being launched.
--   2. federal_policy_seed   — assert pad_us_unit_dogs_policy has a
--                              row for every coastal NPS/NWR/BLM unit
--                              intersecting this state.
--   3. seasonal_closure_seed — assert known species/site closures for
--                              this state are seeded.
--
-- Each phase's criterion calls a helper here, which returns a row count
-- of "pending" (un-seeded) entities for the state. Pipeline criterion
-- becomes `pending = 0`. If `pending > 0`, the phase auto-runs a Python
-- seed script (or fails loudly so an operator can curate).
--
-- This migration adds:
--   - public.coastal_pad_us_units_for_state(state)  — set-returning
--   - public.unseeded_pad_us_units_for_state(state) — pending count helper
--   - public.seasonal_closure_seed (new table)      — generic seed table
--   - public.unseeded_seasonal_closures_for_state(state) — pending count

begin;

-- ── 1. Coastal PAD-US units for a state (NPS/NWR/USFS/BLM marine) ──────
-- Returns each PAD-US unit whose geom intersects the state bbox AND
-- whose management code matches a coastal/protected agency we care
-- about. NPS national seashores, USFWS NWRs, USACE coastal, and BLM
-- coastal allocations are the major ones. State and local agencies
-- already covered by state_dogs_policy / city_dog_policy.

create or replace function public.coastal_pad_us_units_for_state(p_state text)
returns table(
  unit_id    bigint,
  mng_name   text,
  unit_name  text,
  loc_mang   text,
  loc_own    text,
  geom_centroid_lat double precision,
  geom_centroid_lon double precision
)
language sql
stable
as $function$
  with state_bbox as (
    select st_collect(geom) g from public.counties
     where state_fp = (select case p_state
       when 'AL' then '01' when 'AK' then '02' when 'AZ' then '04' when 'AR' then '05'
       when 'CA' then '06' when 'CO' then '08' when 'CT' then '09' when 'DE' then '10'
       when 'FL' then '12' when 'GA' then '13' when 'HI' then '15' when 'ID' then '16'
       when 'IL' then '17' when 'IN' then '18' when 'IA' then '19' when 'KS' then '20'
       when 'KY' then '21' when 'LA' then '22' when 'ME' then '23' when 'MD' then '24'
       when 'MA' then '25' when 'MI' then '26' when 'MN' then '27' when 'MS' then '28'
       when 'MO' then '29' when 'MT' then '30' when 'NE' then '31' when 'NV' then '32'
       when 'NH' then '33' when 'NJ' then '34' when 'NM' then '35' when 'NY' then '36'
       when 'NC' then '37' when 'ND' then '38' when 'OH' then '39' when 'OK' then '40'
       when 'OR' then '41' when 'PA' then '42' when 'RI' then '44' when 'SC' then '45'
       when 'SD' then '46' when 'TN' then '47' when 'TX' then '48' when 'UT' then '49'
       when 'VT' then '50' when 'VA' then '51' when 'WA' then '53' when 'WV' then '54'
       when 'WI' then '55' when 'WY' then '56'
     end)
  )
  select pu.unit_id::bigint,
         pu.mng_name::text,
         pu.unit_name::text,
         pu.loc_mang::text,
         pu.loc_own::text,
         st_y(st_centroid(pu.geom))::double precision,
         st_x(st_centroid(pu.geom))::double precision
    from public.pad_us_units pu, state_bbox sb
   where st_intersects(pu.geom, sb.g)
     and (
       -- NPS units (national seashore, monument, recreation area)
       (pu.loc_mang ilike '%NATIONAL PARK%'
        or pu.mng_type = 'FED' and pu.loc_mang ilike '%NPS%')
       -- USFWS national wildlife refuges
       or pu.loc_mang ilike '%FISH%WILDLIFE%'
       or pu.loc_own  ilike '%FISH%WILDLIFE%'
       -- BLM coastal
       or pu.loc_mang ilike '%BUREAU OF LAND%'
       -- USFS coastal
       or pu.loc_mang ilike '%FOREST SERVICE%'
       -- Generic federal markers (cast wider net; downstream filtering by name)
       or pu.mng_type = 'FED'
     );
$function$;

create or replace function public.unseeded_pad_us_units_for_state(p_state text)
returns bigint
language sql
stable
as $function$
  select count(*)::bigint
    from public.coastal_pad_us_units_for_state(p_state) c
   where not exists (
     select 1 from public.pad_us_unit_dogs_policy pdp
      where pdp.unit_id = c.unit_id
   );
$function$;


-- ── 2. Unified seasonal-closure seed table ─────────────────────────────
-- Generalizes plover_seed_pending to support any species or closure
-- type. Per-beach-or-region records of yearly-recurring date windows.
-- Mirrors the dogs_prohibited_start/end columns on beach_dog_policy
-- but at the seed/source level (so we don't lose the source quote and
-- can apply to multiple beaches).

create table if not exists public.seasonal_closure_seed (
  id            bigserial primary key,
  state_code    text not null,
  county        text,
  beach_name    text,
  approx_lat    double precision,
  approx_lon    double precision,
  closure_type  text not null
                check (closure_type in
                  ('snowy_plover','piping_plover','least_tern',
                   'sea_turtle_nesting','salmon_spawning',
                   'swim_beach_summer_ban','beach_grooming','other')),
  start_md      text not null check (start_md ~ '^\d{2}-\d{2}$'),
  end_md        text not null check (end_md ~ '^\d{2}-\d{2}$'),
  source_url    text,
  source_quote  text,
  notes         text,
  status        text default 'pending'
                check (status in ('pending','applied','no_match','superseded')),
  matched_fid   bigint,
  recorded_at   timestamptz default now(),
  unique (state_code, beach_name, closure_type)
);

grant select on public.seasonal_closure_seed to anon, authenticated, service_role;

-- Backfill from plover_seed_pending (one-time)
insert into public.seasonal_closure_seed
  (state_code, county, beach_name, approx_lat, approx_lon,
   closure_type, start_md, end_md, source_url, source_quote, notes, status)
select state_code, county, name, approx_lat, approx_lon,
       'snowy_plover', '03-15', '09-15',
       'https://www.fws.gov/species/western-snowy-plover-charadrius-nivosus-nivosus',
       'USFWS Western Snowy Plover Pacific Coast Recovery Plan: nesting season Mar 15 – Sep 15.',
       reason,
       case when reason ilike '%no match%' then 'no_match' else 'pending' end
  from public.plover_seed_pending
on conflict (state_code, beach_name, closure_type) do nothing;

-- Pending count for a state
create or replace function public.unseeded_seasonal_closures_for_state(p_state text)
returns bigint
language sql
stable
as $function$
  -- Pending = seeds in 'pending' status for this state. 'no_match' and
  -- 'applied' are terminal states that don't block the canon.
  select count(*)::bigint
    from public.seasonal_closure_seed
   where state_code = p_state
     and status = 'pending';
$function$;


-- ── 3. State policy missing-rows helper ────────────────────────────────
-- Used by the state_policy_seed phase's criterion. Returns 0 if the
-- state has at least one row in state_dogs_policy.

create or replace function public.unseeded_state_policy_for_state(p_state text)
returns bigint
language sql
stable
as $function$
  select case when exists (
    select 1 from public.state_dogs_policy where state_code = p_state
  ) then 0 else 1 end::bigint;
$function$;


grant execute on function public.coastal_pad_us_units_for_state(text)      to anon, authenticated, service_role;
grant execute on function public.unseeded_pad_us_units_for_state(text)     to anon, authenticated, service_role;
grant execute on function public.unseeded_seasonal_closures_for_state(text) to anon, authenticated, service_role;
grant execute on function public.unseeded_state_policy_for_state(text)     to anon, authenticated, service_role;

commit;
