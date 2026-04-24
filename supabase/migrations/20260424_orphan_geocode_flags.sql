-- orphan_geocode validation check (2026-04-24)
-- Phase 2d of the us_beach_points pipeline: flag beach points that are
-- geocoded to coordinates where no authority signal exists, but whose name
-- matches a polygon somewhere else — suggesting the point is at the wrong spot.
--
-- Two tiers (strongest to weakest):
--   A — name-matched authority polygon within 5 km (but outside reclaim range)
--   B — name-matched authority polygon in the same county (>5 km)
--
-- Each flagged row gets validation_status='invalid' and a validation_flags
-- entry with the tier, candidate polygon name, distance, and source.
--
-- Admin can dismiss via geocode_admin_confirmed=true to suppress re-flagging.

-- ── Schema: admin-confirmed suppression flag ──────────────────────────────
alter table public.us_beach_points
  add column if not exists geocode_admin_confirmed boolean not null default false;

comment on column public.us_beach_points.geocode_admin_confirmed is
  'True when an admin has reviewed an orphan_geocode flag and confirmed the point is genuinely at these coordinates. Suppresses re-flagging by flag_orphan_geocodes.';

-- ── Detection function ────────────────────────────────────────────────────
-- Scoped by state per project_dedupe_scope_ca_only.md. Returns per-flagged
-- row for the caller to log; actual validation_flags update happens here too.
create or replace function public.flag_orphan_geocodes(p_state text)
returns table(
  fid             int,
  tier            text,
  candidate_name  text,
  candidate_source text,
  candidate_county text,
  distance_m      int
)
language plpgsql
security definer
as $$
declare
  r record;
  flag_obj jsonb;
begin
  -- Clear prior orphan_geocode flags on rows not admin-confirmed,
  -- so re-running re-computes from current data.
  update public.us_beach_points set
    validation_flags = coalesce((
      select jsonb_agg(f)
      from jsonb_array_elements(validation_flags) f
      where (f->>'check') is distinct from 'orphan_geocode'
    ), '[]'::jsonb),
    validation_status = case
      -- If removing orphan_geocode leaves no flags, revert to valid
      when jsonb_array_length(coalesce((
        select jsonb_agg(f)
        from jsonb_array_elements(validation_flags) f
        where (f->>'check') is distinct from 'orphan_geocode'
      ), '[]'::jsonb)) = 0 then 'valid'
      else validation_status
    end
  where state = p_state
    and exists (
      select 1 from jsonb_array_elements(validation_flags) f
      where (f->>'check') = 'orphan_geocode'
    )
    and geocode_admin_confirmed = false;

  -- Detect Tier A + B candidates and apply flags
  for r in
    with orphan_beaches as (
      select b.fid, b.name, b.county_name, b.geom
      from public.us_beach_points b
      join public.beach_access_source v on v.fid = b.fid
      where b.state = p_state
        and b.validation_status in ('valid', 'invalid')  -- reflag even if other flags exist
        and v.access_bucket = 'orphan'
        and b.geocode_admin_confirmed = false
    ),
    -- Tier A: name-matched CPAD polygon within 5 km, outside reclaim range
    tier_a as (
      select distinct on (b.fid)
        b.fid, b.name as beach, b.county_name as beach_county,
        c.unit_name as candidate_name,
        c.county    as candidate_county,
        'cpad'      as candidate_source,
        round(ST_Distance(c.geom::geography, b.geom::geography)::numeric, 0)::int as distance_m,
        'A'         as tier
      from orphan_beaches b
      join public.cpad_units c
        on ST_DWithin(c.geom, b.geom, 0.05)
       and ST_DWithin(c.geom::geography, b.geom::geography, 5000)
       and cardinality(public.shared_name_tokens(b.name, c.unit_name)) > 0
      order by b.fid, ST_Distance(c.geom::geography, b.geom::geography) asc
    ),
    -- Tier B: name-matched CPAD in same county, >5 km away
    tier_b as (
      select distinct on (b.fid)
        b.fid, b.name as beach, b.county_name as beach_county,
        c.unit_name as candidate_name,
        c.county    as candidate_county,
        'cpad'      as candidate_source,
        round(ST_Distance(c.geom::geography, b.geom::geography)::numeric, 0)::int as distance_m,
        'B'         as tier
      from orphan_beaches b
      join public.cpad_units c
        on trim(c.county) = trim(replace(b.county_name, ' County',''))
       and cardinality(public.shared_name_tokens(b.name, c.unit_name)) > 0
      where b.fid not in (select t.fid from tier_a t)
      order by b.fid, ST_Distance(c.geom::geography, b.geom::geography) asc
    )
    select * from tier_a
    union all
    select * from tier_b
  loop
    flag_obj := jsonb_build_object(
      'check',            'orphan_geocode',
      'tier',             r.tier,
      'expected',         r.candidate_name,
      'details',          format('Tier %s: named %s polygon "%s" at %s m%s',
                                 r.tier,
                                 r.candidate_source,
                                 r.candidate_name,
                                 r.distance_m,
                                 case when r.tier = 'B' then ' (same county)' else '' end),
      'candidate_source', r.candidate_source,
      'candidate_county', r.candidate_county,
      'distance_m',       r.distance_m,
      'process',          'flag_orphan_geocodes',
      'detected_at',      now()
    );

    update public.us_beach_points set
      validation_status = 'invalid',
      validation_flags  = (
        select coalesce(jsonb_agg(f), '[]'::jsonb)
        from jsonb_array_elements(validation_flags) as f
        where (f->>'check') is distinct from 'orphan_geocode'
      ) || jsonb_build_array(flag_obj),
      validated_at      = now()
    where us_beach_points.fid = r.fid;

    -- Emit for return
    fid              := r.fid;
    tier             := r.tier;
    candidate_name   := r.candidate_name;
    candidate_source := r.candidate_source;
    candidate_county := r.candidate_county;
    distance_m       := r.distance_m;
    return next;
  end loop;
end;
$$;

revoke all on function public.flag_orphan_geocodes(text) from public, anon, authenticated;
grant  execute on function public.flag_orphan_geocodes(text) to service_role;

-- ── Admin helper: list flagged beaches with candidate polygon geometry ────
-- Used by the geocode-review UI. Joins us_beach_points + CPAD polygon
-- geometry so the map can render both the current position and the candidate.
create or replace function public.list_orphan_geocode_flags(p_state text)
returns table(data jsonb)
language sql
stable
security definer
as $$
  with flagged as (
    select
      b.fid,
      b.name,
      b.county_name,
      ST_Y(b.geom) as beach_lat,
      ST_X(b.geom) as beach_lon,
      b.validation_flags,
      b.geocode_admin_confirmed,
      (select f from jsonb_array_elements(b.validation_flags) f
       where (f->>'check') = 'orphan_geocode' limit 1) as flag
    from public.us_beach_points b
    where b.state = p_state
      and b.geocode_admin_confirmed = false
      and exists (
        select 1 from jsonb_array_elements(b.validation_flags) f
        where (f->>'check') = 'orphan_geocode'
      )
  )
  select jsonb_build_object(
    'fid',               f.fid,
    'name',              f.name,
    'county_name',       f.county_name,
    'beach_lat',         f.beach_lat,
    'beach_lon',         f.beach_lon,
    'tier',              f.flag->>'tier',
    'candidate_name',    f.flag->>'expected',
    'candidate_source',  f.flag->>'candidate_source',
    'candidate_county',  f.flag->>'candidate_county',
    'distance_m',        (f.flag->>'distance_m')::int,
    'detected_at',       f.flag->>'detected_at',
    -- Candidate polygon centroid + closest-point-to-beach for map display
    'candidate_centroid', (
      select jsonb_build_object('lat', ST_Y(ST_Centroid(c.geom)), 'lon', ST_X(ST_Centroid(c.geom)))
      from public.cpad_units c
      where c.unit_name = f.flag->>'expected'
        and trim(c.county) = trim(f.flag->>'candidate_county')
      order by ST_Distance(c.geom::geography,
                           ST_SetSRID(ST_MakePoint(f.beach_lon, f.beach_lat), 4326)::geography)
      limit 1
    ),
    'candidate_snap_point', (
      select jsonb_build_object(
        'lat', ST_Y(ST_ClosestPoint(c.geom,
                                     ST_SetSRID(ST_MakePoint(f.beach_lon, f.beach_lat), 4326))),
        'lon', ST_X(ST_ClosestPoint(c.geom,
                                     ST_SetSRID(ST_MakePoint(f.beach_lon, f.beach_lat), 4326)))
      )
      from public.cpad_units c
      where c.unit_name = f.flag->>'expected'
        and trim(c.county) = trim(f.flag->>'candidate_county')
      order by ST_Distance(c.geom::geography,
                           ST_SetSRID(ST_MakePoint(f.beach_lon, f.beach_lat), 4326)::geography)
      limit 1
    )
  ) as data
  from flagged f
  order by (f.flag->>'tier'), (f.flag->>'distance_m')::int;
$$;

revoke all on function public.list_orphan_geocode_flags(text) from public, anon, authenticated;
grant  execute on function public.list_orphan_geocode_flags(text) to service_role;

-- ── Resolution functions ──────────────────────────────────────────────────
-- All three return before/after row-pair for admin_audit logging at the
-- edge-function layer.

-- Confirm move: snap beach point to the closest point on the candidate polygon
create or replace function public.confirm_geocode_move(p_fid int)
returns table(before jsonb, after jsonb)
language plpgsql
security definer
as $$
declare
  b record;
  flag jsonb;
  candidate_name text;
  candidate_county text;
  new_lat float8;
  new_lon float8;
  old_lat float8;
  old_lon float8;
begin
  select * into b from public.us_beach_points where fid = p_fid;
  if not found then raise exception 'fid % not found', p_fid; end if;

  select f into flag from jsonb_array_elements(b.validation_flags) f
    where (f->>'check') = 'orphan_geocode' limit 1;
  if flag is null then raise exception 'fid % has no orphan_geocode flag', p_fid; end if;

  candidate_name   := flag->>'expected';
  candidate_county := flag->>'candidate_county';

  select ST_Y(ST_ClosestPoint(c.geom, b.geom)),
         ST_X(ST_ClosestPoint(c.geom, b.geom))
    into new_lat, new_lon
    from public.cpad_units c
    where c.unit_name = candidate_name
      and trim(c.county) = trim(candidate_county)
    order by ST_Distance(c.geom::geography, b.geom::geography)
    limit 1;

  if new_lat is null then
    raise exception 'could not find candidate polygon % in %', candidate_name, candidate_county;
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
    'resolution', 'snapped_to_candidate_polygon',
    'candidate_name', candidate_name
  );
  return next;
end;
$$;

revoke all on function public.confirm_geocode_move(int) from public, anon, authenticated;
grant  execute on function public.confirm_geocode_move(int) to service_role;

-- Dismiss: admin says "beach is actually here, don't re-flag"
create or replace function public.dismiss_geocode_flag(p_fid int)
returns table(before jsonb, after jsonb)
language plpgsql
security definer
as $$
declare
  b record;
  flag jsonb;
begin
  select * into b from public.us_beach_points where fid = p_fid;
  if not found then raise exception 'fid % not found', p_fid; end if;

  select f into flag from jsonb_array_elements(b.validation_flags) f
    where (f->>'check') = 'orphan_geocode' limit 1;

  before := jsonb_build_object(
    'fid', p_fid,
    'lat', ST_Y(b.geom), 'lon', ST_X(b.geom),
    'validation_status', b.validation_status,
    'orphan_geocode_flag', flag,
    'geocode_admin_confirmed', b.geocode_admin_confirmed
  );

  update public.us_beach_points set
    geocode_admin_confirmed = true,
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
    'validation_status', 'valid',
    'geocode_admin_confirmed', true,
    'resolution', 'admin_dismissed'
  );
  return next;
end;
$$;

revoke all on function public.dismiss_geocode_flag(int) from public, anon, authenticated;
grant  execute on function public.dismiss_geocode_flag(int) to service_role;

-- Edit coords: admin types exact lat/lon
create or replace function public.edit_geocode_coords(p_fid int, p_lat float8, p_lon float8)
returns table(before jsonb, after jsonb)
language plpgsql
security definer
as $$
declare
  b record;
  flag jsonb;
begin
  if p_lat is null or p_lon is null then
    raise exception 'lat + lon required';
  end if;
  if p_lat < -90 or p_lat > 90 or p_lon < -180 or p_lon > 180 then
    raise exception 'coords out of range';
  end if;

  select * into b from public.us_beach_points where fid = p_fid;
  if not found then raise exception 'fid % not found', p_fid; end if;

  select f into flag from jsonb_array_elements(b.validation_flags) f
    where (f->>'check') = 'orphan_geocode' limit 1;

  before := jsonb_build_object(
    'fid', p_fid,
    'lat', ST_Y(b.geom), 'lon', ST_X(b.geom),
    'validation_status', b.validation_status,
    'orphan_geocode_flag', flag
  );

  update public.us_beach_points set
    geom = ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326),
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
    'fid', p_fid, 'lat', p_lat, 'lon', p_lon,
    'validation_status', 'valid',
    'resolution', 'admin_edited_coords'
  );
  return next;
end;
$$;

revoke all on function public.edit_geocode_coords(int, float8, float8) from public, anon, authenticated;
grant  execute on function public.edit_geocode_coords(int, float8, float8) to service_role;
