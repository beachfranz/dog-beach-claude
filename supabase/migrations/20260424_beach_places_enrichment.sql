-- Place/city enrichment on us_beach_points (2026-04-24)
-- Phase 2e of the pipeline: spatial-join every beach to its TIGER place
-- (incorporated city / CDP / other) so downstream consumers know the
-- governing jurisdiction.
--
-- Stored columns (persisted so query-time joins stay fast):
--   place_fips   — FIPS place code (fips_state + fips_place = 7-digit GEOID)
--   place_name   — name of matched TIGER place, NULL if none within 500m
--   place_type   — TIGER CLASSFP (C1/C2/U1/etc.)
--
-- Derived at query time (no storage):
--   place_kind          = 'incorporated'|'cdp'|'other' per place_type prefix
--   governing_body_type = 'city' if incorporated, else 'county'
--   governing_body_name = place_name if incorporated, else county_name
--
-- Refreshed via refresh_beach_places('CA'). Run it after bulk coord
-- changes (orphan_geocode resolutions, geocode-admin edits), or
-- periodically if TIGER data is reloaded.

alter table public.us_beach_points
  add column if not exists place_fips  text,
  add column if not exists place_name  text,
  add column if not exists place_type  text;

comment on column public.us_beach_points.place_fips is
  'TIGER Places GEOID (fips_state + fips_place) for the incorporated place or CDP containing this beach point. NULL if not inside any place within 500m. Populated by refresh_beach_places().';
comment on column public.us_beach_points.place_type is
  'TIGER CLASSFP: C1-C7 = incorporated city/town; U1 = CDP (unincorporated). Use: governing body is the city when CLASSFP starts with C, else the county.';

create index if not exists us_beach_points_place_fips_idx
  on public.us_beach_points(place_fips)
  where place_fips is not null;

-- ── Refresh function: populate place_* columns from spatial join ──────────
-- 500m geography buffer handles beaches whose coords sit just offshore of
-- the city polygon. When multiple places match (e.g., overlapping CDP +
-- incorporated), prefer incorporated (C%) over CDP (U1) by sort order.
create or replace function public.refresh_beach_places(p_state text)
returns int
language plpgsql
security definer
as $$
declare
  n int;
begin
  with best as (
    select distinct on (b.fid)
      b.fid,
      j.fips_state || j.fips_place as place_fips,
      j.name       as place_name,
      j.place_type
    from public.us_beach_points b
    join public.jurisdictions j
      on ST_DWithin(j.geom, b.geom, 0.01)
     and ST_DWithin(j.geom::geography, b.geom::geography, 500)
    where b.state = p_state
    -- Tiebreaker: incorporated wins over CDP when both match
    order by b.fid,
             (case when j.place_type like 'C%' then 0 else 1 end),
             ST_Distance(j.geom::geography, b.geom::geography)
  )
  update public.us_beach_points b
  set place_fips = best.place_fips,
      place_name = best.place_name,
      place_type = best.place_type
  from best
  where b.fid = best.fid;

  get diagnostics n = row_count;

  -- Also clear any previously-set place on rows that no longer match
  update public.us_beach_points b
  set place_fips = null, place_name = null, place_type = null
  where b.state = p_state
    and b.place_fips is not null
    and not exists (
      select 1 from public.jurisdictions j
      where j.fips_state || j.fips_place = b.place_fips
        and ST_DWithin(j.geom::geography, b.geom::geography, 500)
    );

  return n;
end;
$$;

revoke all on function public.refresh_beach_places(text) from public, anon, authenticated;
grant  execute on function public.refresh_beach_places(text) to service_role;
