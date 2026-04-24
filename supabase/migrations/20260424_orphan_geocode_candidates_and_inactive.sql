-- Enhancements to orphan_geocode review flow (2026-04-24):
--   1. is_active column on us_beach_points
--   2. list_orphan_geocode_flags returns candidates[] (CPAD + CCC) instead of
--      a single pre-picked candidate
--   3. confirm_geocode_move takes (p_candidate_source, p_candidate_name,
--      p_candidate_county) so admin can pick a specific candidate
--   4. mark_beach_inactive RPC for the 4th resolution path

-- ── 1. is_active flag ─────────────────────────────────────────────────────
alter table public.us_beach_points
  add column if not exists is_active boolean not null default true;

comment on column public.us_beach_points.is_active is
  'Admin-set: false means the beach is archived from the active inventory (not a real beach, data-scrape artifact, permanently unplaceable, etc.). Downstream pipeline queries should filter is_active = true.';

create index if not exists us_beach_points_is_active_idx
  on public.us_beach_points(is_active)
  where is_active = false;

-- ── 2. list_orphan_geocode_flags returns candidates[] ────────────────────
-- Each flag now carries an array of candidates (CPAD top ≤300m, CPAD top
-- in-county name-match, CCC same-county name-matches within 10 km). UI picks
-- one; detection still tier-classifies the primary candidate for sorting.
create or replace function public.list_orphan_geocode_flags(p_state text)
returns table(data jsonb)
language sql
stable
security definer
as $$
  with flagged as (
    select
      b.fid, b.name, b.county_name,
      ST_Y(b.geom) as beach_lat, ST_X(b.geom) as beach_lon, b.geom,
      b.validation_flags,
      b.is_active,
      (select f from jsonb_array_elements(b.validation_flags) f
       where (f->>'check') = 'orphan_geocode' limit 1) as flag
    from public.us_beach_points b
    where b.state = p_state
      and b.is_active = true
      and b.geocode_admin_confirmed = false
      and exists (
        select 1 from jsonb_array_elements(b.validation_flags) f
        where (f->>'check') = 'orphan_geocode'
      )
  ),
  cpad_candidates as (
    -- Top-5 CPAD name-matches per beach, within 30km + same county.
    -- 30km geom pre-filter (0.3°) + geography-precise DWithin keeps the
    -- shared_name_tokens cross-join small enough to finish in sub-second.
    select f.fid,
           jsonb_agg(jsonb_build_object(
             'source',       'cpad',
             'name',         c.unit_name,
             'county',       c.county,
             'distance_m',   c.dist_m,
             'snap_lat',     c.snap_lat,
             'snap_lon',     c.snap_lon,
             'centroid_lat', c.centroid_lat,
             'centroid_lon', c.centroid_lon,
             'same_county',  true
           ) order by c.dist_m) as cands
    from flagged f
    cross join lateral (
      select u.unit_name,
             u.county,
             round(ST_Distance(u.geom::geography, f.geom::geography)::numeric, 0)::int as dist_m,
             ST_Y(ST_ClosestPoint(u.geom, f.geom)) as snap_lat,
             ST_X(ST_ClosestPoint(u.geom, f.geom)) as snap_lon,
             ST_Y(ST_Centroid(u.geom))             as centroid_lat,
             ST_X(ST_Centroid(u.geom))             as centroid_lon
      from public.cpad_units u
      where ST_DWithin(u.geom, f.geom, 0.3)
        and ST_DWithin(u.geom::geography, f.geom::geography, 30000)
        and trim(u.county) = trim(replace(f.county_name, ' County',''))
        and u.name_tokens_cache && public.name_tokens(f.name)
      order by ST_Distance(u.geom::geography, f.geom::geography)
      limit 5
    ) c
    group by f.fid
  ),
  ccc_candidates as (
    -- CCC name-matches in same county within 10 km; top-3 per beach
    select f.fid,
           jsonb_agg(jsonb_build_object(
             'source',       'ccc',
             'name',         p.name,
             'county',       p.county,
             'distance_m',   p.dist_m,
             'snap_lat',     p.snap_lat,
             'snap_lon',     p.snap_lon,
             'centroid_lat', p.snap_lat,
             'centroid_lon', p.snap_lon,
             'same_county',  true,
             'open_to_public', p.open_to_public
           ) order by p.dist_m) as cands
    from flagged f
    cross join lateral (
      select a.name,
             a.county,
             a.open_to_public,
             round(ST_Distance(a.geom::geography, f.geom::geography)::numeric, 0)::int as dist_m,
             ST_Y(a.geom) as snap_lat,
             ST_X(a.geom) as snap_lon
      from public.ccc_access_points a
      where ST_DWithin(a.geom::geography, f.geom::geography, 10000)
        and trim(a.county) = trim(replace(f.county_name, ' County',''))
        and a.name_tokens_cache && public.name_tokens(f.name)
      order by ST_Distance(a.geom::geography, f.geom::geography)
      limit 3
    ) p
    group by f.fid
  )
  select jsonb_build_object(
    'fid',               f.fid,
    'name',              f.name,
    'county_name',       f.county_name,
    'beach_lat',         f.beach_lat,
    'beach_lon',         f.beach_lon,
    'tier',              f.flag->>'tier',
    'primary_candidate_name',   f.flag->>'expected',
    'primary_candidate_source', f.flag->>'candidate_source',
    'primary_distance_m',       (f.flag->>'distance_m')::int,
    'detected_at',       f.flag->>'detected_at',
    'candidates', coalesce(cp.cands, '[]'::jsonb) || coalesce(cc.cands, '[]'::jsonb)
  ) as data
  from flagged f
  left join cpad_candidates cp on cp.fid = f.fid
  left join ccc_candidates  cc on cc.fid = f.fid
  order by (f.flag->>'tier'), (f.flag->>'distance_m')::int;
$$;

revoke all on function public.list_orphan_geocode_flags(text) from public, anon, authenticated;
grant  execute on function public.list_orphan_geocode_flags(text) to service_role;

-- ── 3. confirm_geocode_move: takes candidate source/name/county ──────────
-- Previous signature hardcoded the top CPAD candidate. New signature lets
-- the admin pick a specific candidate (CPAD or CCC) from the list.
drop function if exists public.confirm_geocode_move(int);

create or replace function public.confirm_geocode_move(
  p_fid              int,
  p_candidate_source text,   -- 'cpad' | 'ccc'
  p_candidate_name   text,
  p_candidate_county text
)
returns table(before jsonb, after jsonb)
language plpgsql
security definer
as $$
declare
  b record;
  flag jsonb;
  new_lat float8;
  new_lon float8;
  old_lat float8;
  old_lon float8;
begin
  select * into b from public.us_beach_points where fid = p_fid;
  if not found then raise exception 'fid % not found', p_fid; end if;

  select f into flag from jsonb_array_elements(b.validation_flags) f
    where (f->>'check') = 'orphan_geocode' limit 1;
  -- flag may be null if admin hits confirm after an external clear; that's fine

  if p_candidate_source = 'cpad' then
    select ST_Y(ST_ClosestPoint(c.geom, b.geom)),
           ST_X(ST_ClosestPoint(c.geom, b.geom))
      into new_lat, new_lon
      from public.cpad_units c
      where c.unit_name = p_candidate_name
        and trim(c.county) = trim(p_candidate_county)
      order by ST_Distance(c.geom::geography, b.geom::geography)
      limit 1;
  elsif p_candidate_source = 'ccc' then
    select ST_Y(p.geom), ST_X(p.geom)
      into new_lat, new_lon
      from public.ccc_access_points p
      where p.name = p_candidate_name
        and trim(p.county) = trim(p_candidate_county)
      order by ST_Distance(p.geom::geography, b.geom::geography)
      limit 1;
  else
    raise exception 'unknown candidate_source %, expected cpad or ccc', p_candidate_source;
  end if;

  if new_lat is null then
    raise exception 'could not find % candidate %/% for fid %',
      p_candidate_source, p_candidate_name, p_candidate_county, p_fid;
  end if;

  old_lat := ST_Y(b.geom);
  old_lon := ST_X(b.geom);

  before := jsonb_build_object(
    'fid', p_fid, 'lat', old_lat, 'lon', old_lon,
    'validation_status', b.validation_status,
    'orphan_geocode_flag', flag
  );

  update public.us_beach_points set
    geom = ST_SetSRID(ST_MakePoint(new_lon, new_lat), 4326),
    validation_flags = coalesce((
      select jsonb_agg(f) from jsonb_array_elements(validation_flags) f
      where (f->>'check') is distinct from 'orphan_geocode'
    ), '[]'::jsonb),
    validation_status = case
      when jsonb_array_length(coalesce((
        select jsonb_agg(f) from jsonb_array_elements(validation_flags) f
        where (f->>'check') is distinct from 'orphan_geocode'
      ), '[]'::jsonb)) = 0 then 'valid'
      else validation_status
    end,
    validated_at = now()
  where fid = p_fid;

  after := jsonb_build_object(
    'fid', p_fid, 'lat', new_lat, 'lon', new_lon,
    'validation_status', 'valid',
    'resolution', 'snapped_to_candidate',
    'candidate_source', p_candidate_source,
    'candidate_name', p_candidate_name,
    'candidate_county', p_candidate_county
  );
  return next;
end;
$$;

revoke all on function public.confirm_geocode_move(int, text, text, text)
  from public, anon, authenticated;
grant  execute on function public.confirm_geocode_move(int, text, text, text)
  to service_role;

-- ── 4. mark_beach_inactive RPC ───────────────────────────────────────────
create or replace function public.mark_beach_inactive(p_fid int)
returns table(before jsonb, after jsonb)
language plpgsql
security definer
as $$
declare
  b record;
begin
  select * into b from public.us_beach_points where fid = p_fid;
  if not found then raise exception 'fid % not found', p_fid; end if;

  before := jsonb_build_object(
    'fid', p_fid,
    'lat', ST_Y(b.geom), 'lon', ST_X(b.geom),
    'is_active', b.is_active,
    'validation_status', b.validation_status,
    'validation_flags', b.validation_flags
  );

  -- Mark inactive, clear the orphan_geocode flag (since it's no longer
  -- relevant — beach is out of active inventory)
  update public.us_beach_points set
    is_active = false,
    validation_flags = coalesce((
      select jsonb_agg(f) from jsonb_array_elements(validation_flags) f
      where (f->>'check') is distinct from 'orphan_geocode'
    ), '[]'::jsonb),
    validation_status = case
      when jsonb_array_length(coalesce((
        select jsonb_agg(f) from jsonb_array_elements(validation_flags) f
        where (f->>'check') is distinct from 'orphan_geocode'
      ), '[]'::jsonb)) = 0 then 'valid'
      else validation_status
    end,
    validated_at = now()
  where fid = p_fid;

  after := jsonb_build_object(
    'fid', p_fid,
    'lat', ST_Y(b.geom), 'lon', ST_X(b.geom),
    'is_active', false,
    'resolution', 'marked_inactive'
  );
  return next;
end;
$$;

revoke all on function public.mark_beach_inactive(int)
  from public, anon, authenticated;
grant  execute on function public.mark_beach_inactive(int) to service_role;
