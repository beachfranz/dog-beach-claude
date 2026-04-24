-- Explicit canonical association on us_beach_points (2026-04-24)
--
-- Adds canonical_source / canonical_name / canonical_county columns so the
-- admin can explicitly pin a beach to an authority record (CPAD polygon or
-- CCC access point), independent of the beach's geometry. Used both for
-- orphan_geocode "associate only" resolution and for future overrides of
-- ambiguous spatial matches (Miramar-style).
--
-- The beach_access_source view is updated to prefer the explicit reference
-- when set; falls back to spatial logic when it's null.

-- ── 1. Canonical-ref columns ──────────────────────────────────────────────
alter table public.us_beach_points
  add column if not exists canonical_source text,
  add column if not exists canonical_name   text,
  add column if not exists canonical_county text;

alter table public.us_beach_points
  drop constraint if exists us_beach_points_canonical_source_chk;
alter table public.us_beach_points
  add  constraint us_beach_points_canonical_source_chk
  check (canonical_source is null or canonical_source in ('cpad','ccc','csp'));

comment on column public.us_beach_points.canonical_source is
  'Admin-set authority source pinned to this beach (cpad/ccc/csp). Independent of geometry. When set, beach_access_source view prefers it over spatial lookup. NULL = fall back to spatial inference.';

create index if not exists us_beach_points_canonical_ref_idx
  on public.us_beach_points(canonical_source, canonical_name)
  where canonical_source is not null;

-- ── 2. RPC: associate_beach_only (keep geom, set canonical_*) ────────────
create or replace function public.associate_beach_only(
  p_fid              int,
  p_candidate_source text,
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
begin
  if p_candidate_source not in ('cpad','ccc','csp') then
    raise exception 'candidate_source must be cpad, ccc, or csp';
  end if;

  select * into b from public.us_beach_points where fid = p_fid;
  if not found then raise exception 'fid % not found', p_fid; end if;

  -- Verify the candidate exists (fail early if the admin picked something
  -- that's since been renamed)
  if p_candidate_source = 'cpad' then
    perform 1 from public.cpad_units
      where unit_name = p_candidate_name and trim(county) = trim(p_candidate_county);
    if not found then raise exception 'CPAD % / % not found', p_candidate_name, p_candidate_county; end if;
  elsif p_candidate_source = 'ccc' then
    perform 1 from public.ccc_access_points
      where name = p_candidate_name and trim(county) = trim(p_candidate_county);
    if not found then raise exception 'CCC % / % not found', p_candidate_name, p_candidate_county; end if;
  end if;

  select f into flag from jsonb_array_elements(b.validation_flags) f
    where (f->>'check') = 'orphan_geocode' limit 1;

  before := jsonb_build_object(
    'fid', p_fid,
    'lat', ST_Y(b.geom), 'lon', ST_X(b.geom),
    'validation_status', b.validation_status,
    'canonical_source',  b.canonical_source,
    'canonical_name',    b.canonical_name,
    'orphan_geocode_flag', flag
  );

  update public.us_beach_points set
    canonical_source = p_candidate_source,
    canonical_name   = p_candidate_name,
    canonical_county = p_candidate_county,
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
    'canonical_source', p_candidate_source,
    'canonical_name',   p_candidate_name,
    'canonical_county', p_candidate_county,
    'resolution', 'associate_only_kept_coords'
  );
  return next;
end;
$$;

revoke all on function public.associate_beach_only(int, text, text, text)
  from public, anon, authenticated;
grant  execute on function public.associate_beach_only(int, text, text, text)
  to service_role;

-- ── 3. Teach confirm_geocode_move to ALSO set canonical_* ────────────────
-- "Associate + move" = both move the coord AND record explicit association,
-- so the association survives if spatial drift happens later.
drop function if exists public.confirm_geocode_move(int, text, text, text);

create or replace function public.confirm_geocode_move(
  p_fid              int,
  p_candidate_source text,
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
    raise exception 'could not find % candidate %/%', p_candidate_source, p_candidate_name, p_candidate_county;
  end if;

  old_lat := ST_Y(b.geom);
  old_lon := ST_X(b.geom);

  before := jsonb_build_object(
    'fid', p_fid, 'lat', old_lat, 'lon', old_lon,
    'validation_status',   b.validation_status,
    'canonical_source',    b.canonical_source,
    'canonical_name',      b.canonical_name,
    'orphan_geocode_flag', flag
  );

  update public.us_beach_points set
    geom             = ST_SetSRID(ST_MakePoint(new_lon, new_lat), 4326),
    canonical_source = p_candidate_source,
    canonical_name   = p_candidate_name,
    canonical_county = p_candidate_county,
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
    'canonical_source', p_candidate_source,
    'canonical_name',   p_candidate_name,
    'canonical_county', p_candidate_county,
    'resolution', 'snapped_and_associated'
  );
  return next;
end;
$$;

revoke all on function public.confirm_geocode_move(int, text, text, text)
  from public, anon, authenticated;
grant  execute on function public.confirm_geocode_move(int, text, text, text)
  to service_role;
