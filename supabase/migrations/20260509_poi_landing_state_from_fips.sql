-- 20260509_poi_landing_state_from_fips.sql
--
-- Issue #22: poi_landing.state was only populated for CA/OR/WA. The
-- promote_poi_landing_to_arena() function filters by state, so any row
-- with state=NULL was invisible to per-state launches. DE had 49
-- address_state='DE' rows in poi_landing but state=NULL → 0 reached
-- arena, leaving DE with 6 osm-only beaches instead of ~50+.
--
-- Spec correction: poi_landing.state should ALWAYS be derived from
-- county_geoid (the structural FIPS code that every promoted record
-- carries), not from address_state (Google's text inference, which
-- can disagree with FIPS for cross-border addresses).
--
-- Three pieces:
--   1. _backfill_poi_landing_state() — one-shot UPDATE for existing rows
--   2. trigger to set state on INSERT/UPDATE when county_geoid changes
--      so future loads can never produce state=NULL
--   3. backfill apply (call the function once)

begin;

-- ── 1. Backfill function ────────────────────────────────────────────────
create or replace function public._backfill_poi_landing_state()
returns table(updated_rows bigint, before_null bigint, after_null bigint)
language plpgsql
as $$
declare
  v_before  bigint;
  v_updated bigint;
  v_after   bigint;
begin
  select count(*) into v_before from public.poi_landing where state is null;

  with upd as (
    update public.poi_landing pl
       set state = public.state_from_fips(pl.county_geoid)
     where pl.state is null
       and pl.county_geoid is not null
    returning 1
  )
  select count(*) into v_updated from upd;

  -- Fallback: rows with no county_geoid but a non-empty address_state
  with upd as (
    update public.poi_landing pl
       set state = pl.address_state
     where pl.state is null
       and pl.county_geoid is null
       and pl.address_state is not null
       and length(pl.address_state) = 2
    returning 1
  )
  select v_updated + count(*) into v_updated from upd;

  select count(*) into v_after from public.poi_landing where state is null;

  return query select v_updated, v_before, v_after;
end $$;


-- ── 2. Trigger: keep state in sync with county_geoid ───────────────────
-- On INSERT or county_geoid UPDATE, set state from county_geoid (or
-- fall back to address_state).

create or replace function public._poi_landing_set_state()
returns trigger
language plpgsql
as $$
begin
  if new.state is null then
    if new.county_geoid is not null then
      new.state := public.state_from_fips(new.county_geoid);
    elsif new.address_state is not null and length(new.address_state) = 2 then
      new.state := new.address_state;
    end if;
  end if;
  return new;
end $$;

drop trigger if exists poi_landing_state_sync on public.poi_landing;
create trigger poi_landing_state_sync
  before insert or update of county_geoid, address_state
  on public.poi_landing
  for each row
  execute function public._poi_landing_set_state();


-- ── 3. Apply the backfill ──────────────────────────────────────────────
do $$
declare r record;
begin
  select * into r from public._backfill_poi_landing_state();
  raise notice 'poi_landing state backfill: updated=%, before_null=%, after_null=%',
    r.updated_rows, r.before_null, r.after_null;
end $$;


grant execute on function public._backfill_poi_landing_state() to anon, authenticated, service_role;

commit;
