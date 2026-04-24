-- RPC functions for the dedupe-review admin flow (2026-04-24)
--
-- get_dupe_cluster(cluster_id)       — read all rows in a cluster with full
--                                       per-layer provenance (CPAD, CCC, CSP,
--                                       tribal, PLZ, waterbodies, MPA, NOAA)
--
-- list_dupe_clusters()               — summary for the cluster-picker list
--
-- resolve_dupe_cluster(...)          — promote one row as canonical, mark
--                                       the others as duplicate
--
-- bulk_resolve_identical_dupes()     — auto-resolve the subset where all
--                                       rows are ST_Equals AND name-identical
--
-- All four are security definer, granted only to service_role — the edge
-- functions wrap them behind requireAdmin() + admin_audit logging.

-- ── 1. get_dupe_cluster ───────────────────────────────────────────────────
-- Inlines the access bucket detection rather than joining beach_access_source.
-- The view materializes provenance for all 8k rows before filtering, which
-- blows past the statement timeout. Inline per-row subqueries are GIST-indexed
-- and cheap — a cluster of 2 rows is ~30 subqueries total.
create or replace function public.get_dupe_cluster(p_cluster_id int)
returns table(data jsonb)
language sql
stable
security definer
as $$
  with rows_in_cluster as (
    select b.*
    from public.us_beach_points b
    where b.duplicate_cluster_id = p_cluster_id
  ),
  -- Dual pass per source: nearest NAME-MATCHED polygon/point and nearest
  -- OVERALL. Mirrors the dual-CTE approach in beach_access_source so
  -- bucket labels stay consistent across the two paths.
  per_row as (
    select
      r.*,
      -- CPAD: nearest name-matched
      (select c.unit_name
         from public.cpad_units c
        where ST_DWithin(c.geom, r.geom, 0.003)
          and ST_DWithin(c.geom::geography, r.geom::geography, 300)
          and cardinality(public.shared_name_tokens(r.name, c.unit_name)) > 0
        order by c.geom <-> r.geom
        limit 1) as cpad_named_name,
      (select round(ST_Distance(c.geom::geography, r.geom::geography)::numeric, 0)::int
         from public.cpad_units c
        where ST_DWithin(c.geom, r.geom, 0.003)
          and ST_DWithin(c.geom::geography, r.geom::geography, 300)
          and cardinality(public.shared_name_tokens(r.name, c.unit_name)) > 0
        order by c.geom <-> r.geom
        limit 1) as cpad_named_dist,
      (select public.shared_name_tokens(r.name, c.unit_name)
         from public.cpad_units c
        where ST_DWithin(c.geom, r.geom, 0.003)
          and ST_DWithin(c.geom::geography, r.geom::geography, 300)
          and cardinality(public.shared_name_tokens(r.name, c.unit_name)) > 0
        order by c.geom <-> r.geom
        limit 1) as cpad_named_tokens,

      -- CPAD: nearest overall (may or may not name-match)
      (select c.unit_name
         from public.cpad_units c
        where ST_DWithin(c.geom, r.geom, 0.003)
          and ST_DWithin(c.geom::geography, r.geom::geography, 300)
        order by c.geom <-> r.geom
        limit 1) as cpad_any_name,
      (select round(ST_Distance(c.geom::geography, r.geom::geography)::numeric, 0)::int
         from public.cpad_units c
        where ST_DWithin(c.geom, r.geom, 0.003)
          and ST_DWithin(c.geom::geography, r.geom::geography, 300)
        order by c.geom <-> r.geom
        limit 1) as cpad_any_dist,
      (select c.access_typ
         from public.cpad_units c
        where ST_DWithin(c.geom, r.geom, 0.003)
          and ST_DWithin(c.geom::geography, r.geom::geography, 300)
        order by c.geom <-> r.geom
        limit 1) as cpad_any_access_typ,
      (select c.agncy_lev
         from public.cpad_units c
        where ST_DWithin(c.geom, r.geom, 0.003)
          and ST_DWithin(c.geom::geography, r.geom::geography, 300)
        order by c.geom <-> r.geom
        limit 1) as cpad_any_agncy_lev,

      -- CCC: nearest name-matched
      (select c.name
         from public.ccc_access_points c
        where ST_DWithin(c.geom, r.geom, 0.003)
          and ST_DWithin(c.geom::geography, r.geom::geography, 300)
          and cardinality(public.shared_name_tokens(r.name, c.name)) > 0
        order by c.geom <-> r.geom
        limit 1) as ccc_named_name,
      (select round(ST_Distance(c.geom::geography, r.geom::geography)::numeric, 0)::int
         from public.ccc_access_points c
        where ST_DWithin(c.geom, r.geom, 0.003)
          and ST_DWithin(c.geom::geography, r.geom::geography, 300)
          and cardinality(public.shared_name_tokens(r.name, c.name)) > 0
        order by c.geom <-> r.geom
        limit 1) as ccc_named_dist,
      (select public.shared_name_tokens(r.name, c.name)
         from public.ccc_access_points c
        where ST_DWithin(c.geom, r.geom, 0.003)
          and ST_DWithin(c.geom::geography, r.geom::geography, 300)
          and cardinality(public.shared_name_tokens(r.name, c.name)) > 0
        order by c.geom <-> r.geom
        limit 1) as ccc_named_tokens,

      -- CCC: nearest overall
      (select c.name
         from public.ccc_access_points c
        where ST_DWithin(c.geom, r.geom, 0.003)
          and ST_DWithin(c.geom::geography, r.geom::geography, 300)
        order by c.geom <-> r.geom
        limit 1) as ccc_any_name,
      (select round(ST_Distance(c.geom::geography, r.geom::geography)::numeric, 0)::int
         from public.ccc_access_points c
        where ST_DWithin(c.geom, r.geom, 0.003)
          and ST_DWithin(c.geom::geography, r.geom::geography, 300)
        order by c.geom <-> r.geom
        limit 1) as ccc_any_dist,
      (select c.open_to_public
         from public.ccc_access_points c
        where ST_DWithin(c.geom, r.geom, 0.003)
          and ST_DWithin(c.geom::geography, r.geom::geography, 300)
        order by c.geom <-> r.geom
        limit 1) as ccc_any_open
    from rows_in_cluster r
  )
  select jsonb_build_object(
    'fid',                         r.fid,
    'name',                        r.name,
    'state',                       r.state,
    'county_name',                 r.county_name,
    'county_fips',                 r.county_fips,
    'lat',                         ST_Y(r.geom),
    'lon',                         ST_X(r.geom),
    'validation_status',           r.validation_status,
    'validation_flags',            r.validation_flags,
    'validated_at',                r.validated_at,
    'duplicate_cluster_id',        r.duplicate_cluster_id,
    'duplicate_status',            r.duplicate_status,

    'access_bucket', case
      when r.cpad_named_dist <= 100 then 'cpad_named'
      when r.ccc_named_dist  <= 200 then 'ccc_named'
      when r.cpad_any_dist   <= 100 then 'cpad_plain'
      when r.ccc_any_dist    <= 200 then 'ccc_plain'
      when exists (select 1 from public.csp_parks s where ST_DWithin(s.geom, r.geom, 0.001))       then 'csp'
      when exists (select 1 from public.tribal_lands t where ST_DWithin(t.geom, r.geom, 0.001))    then 'tribal'
      when exists (select 1 from public.private_land_zones z
                    where z.active = true
                      and ST_Y(r.geom) between z.min_lat and z.max_lat
                      and ST_X(r.geom) between z.min_lon and z.max_lon)                            then 'plz'
      when r.cpad_named_dist <= 300 then 'reclaim_cpad'
      when r.ccc_named_dist  <= 300 then 'reclaim_ccc'
      else 'orphan'
    end,
    'access_match_name', case
      when r.cpad_named_dist <= 100 then r.cpad_named_name
      when r.ccc_named_dist  <= 200 then r.ccc_named_name
      when r.cpad_any_dist   <= 100 then r.cpad_any_name
      when r.ccc_any_dist    <= 200 then r.ccc_any_name
      when r.cpad_named_dist <= 300 then r.cpad_named_name
      when r.ccc_named_dist  <= 300 then r.ccc_named_name
    end,
    'access_match_distance_m', case
      when r.cpad_named_dist <= 100 then r.cpad_named_dist
      when r.ccc_named_dist  <= 200 then r.ccc_named_dist
      when r.cpad_any_dist   <= 100 then r.cpad_any_dist
      when r.ccc_any_dist    <= 200 then r.ccc_any_dist
      when r.cpad_named_dist <= 300 then r.cpad_named_dist
      when r.ccc_named_dist  <= 300 then r.ccc_named_dist
    end,
    'access_match_shared_tokens', case
      when r.cpad_named_dist <= 100 then r.cpad_named_tokens
      when r.ccc_named_dist  <= 200 then r.ccc_named_tokens
      when r.cpad_any_dist   <= 100 then null
      when r.ccc_any_dist    <= 200 then null
      when r.cpad_named_dist <= 300 then r.cpad_named_tokens
      when r.ccc_named_dist  <= 300 then r.ccc_named_tokens
    end,
    'cpad_access_typ',             r.cpad_any_access_typ,
    'cpad_agncy_lev',              r.cpad_any_agncy_lev,
    'ccc_open_to_public',          r.ccc_any_open,
    'csp_subtype',                 (select s.subtype from public.csp_parks s where ST_DWithin(s.geom, r.geom, 0.001) order by s.geom <-> r.geom limit 1),
    'plz_reason',                  (select z.reason from public.private_land_zones z where z.active and ST_Y(r.geom) between z.min_lat and z.max_lat and ST_X(r.geom) between z.min_lon and z.max_lon limit 1),

    'cpad', coalesce((
      select jsonb_agg(jsonb_build_object(
        'unit_name',  c.unit_name,
        'agncy_name', c.agncy_name,
        'agncy_lev',  c.agncy_lev,
        'mng_agncy',  c.mng_agncy,
        'mng_ag_lev', c.mng_ag_lev,
        'access_typ', c.access_typ,
        'distance_m', round(ST_Distance(c.geom::geography, r.geom::geography)::numeric, 0)
      ) order by ST_Distance(c.geom::geography, r.geom::geography))
      from public.cpad_units c
      where ST_DWithin(c.geom, r.geom, 0.003)
        and ST_DWithin(c.geom::geography, r.geom::geography, 300)
    ), '[]'::jsonb),

    'ccc', coalesce((
      select jsonb_agg(jsonb_build_object(
        'name',            p.name,
        'open_to_public',  p.open_to_public,
        'dog_friendly',    p.dog_friendly,
        'fee',             p.fee,
        'restrictions',    p.restrictions,
        'county',          p.county,
        'distance_m',      round(ST_Distance(p.geom::geography, r.geom::geography)::numeric, 0)
      ) order by ST_Distance(p.geom::geography, r.geom::geography))
      from public.ccc_access_points p
      where ST_DWithin(p.geom, r.geom, 0.003)
        and ST_DWithin(p.geom::geography, r.geom::geography, 300)
    ), '[]'::jsonb),

    'csp', coalesce((
      select jsonb_agg(jsonb_build_object(
        'unit_name',  s.unit_name,
        'subtype',    s.subtype,
        'distance_m', round(ST_Distance(s.geom::geography, r.geom::geography)::numeric, 0)
      ) order by ST_Distance(s.geom::geography, r.geom::geography))
      from public.csp_parks s
      where ST_DWithin(s.geom, r.geom, 0.002)
    ), '[]'::jsonb),

    'tribal', coalesce((
      select jsonb_agg(jsonb_build_object(
        'lar_name',   t.lar_name,
        'distance_m', round(ST_Distance(t.geom::geography, r.geom::geography)::numeric, 0)
      ) order by ST_Distance(t.geom::geography, r.geom::geography))
      from public.tribal_lands t
      where ST_DWithin(t.geom, r.geom, 0.002)
    ), '[]'::jsonb),

    'plz', coalesce((
      select jsonb_agg(jsonb_build_object(
        'name',   z.name,
        'reason', z.reason
      ))
      from public.private_land_zones z
      where z.active = true
        and ST_Y(r.geom) between z.min_lat and z.max_lat
        and ST_X(r.geom) between z.min_lon and z.max_lon
    ), '[]'::jsonb),

    'waterbodies', coalesce((
      select jsonb_agg(jsonb_build_object(
        'gnis_name',  w.gnis_name,
        'ftype',      w.ftype,
        'distance_m', round(ST_Distance(w.geom::geography, r.geom::geography)::numeric, 0)
      ) order by ST_Distance(w.geom::geography, r.geom::geography))
      from public.waterbodies w
      where ST_DWithin(w.geom, r.geom, 0.015)
    ), '[]'::jsonb),

    'mpas', coalesce((
      select jsonb_agg(jsonb_build_object(
        'name',       m.name,
        'mpa_type',   m.mpa_type,
        'study_region', m.study_region,
        'distance_m', round(ST_Distance(m.geom::geography, r.geom::geography)::numeric, 0)
      ) order by ST_Distance(m.geom::geography, r.geom::geography))
      from public.mpas m
      where ST_DWithin(m.geom, r.geom, 0.002)
    ), '[]'::jsonb),

    'noaa_nearest', (
      select jsonb_build_object(
        'station_id',   n.station_id,
        'name',         n.name,
        'station_type', n.station_type,
        'distance_m',   round(ST_Distance(n.geom::geography, r.geom::geography)::numeric, 0)
      )
      from public.noaa_stations n
      where n.reference_id is null
      order by n.geom <-> r.geom
      limit 1
    )
  ) as data
  from per_row r
  order by r.fid;
$$;

revoke all on function public.get_dupe_cluster(int) from public, anon, authenticated;
grant  execute on function public.get_dupe_cluster(int) to service_role;

-- ── 2. list_dupe_clusters ─────────────────────────────────────────────────
-- Summary row per cluster for the picker: counts, sample names, states,
-- and an is_auto_resolvable flag (all rows ST_Equals + identical trim+lower name).
create or replace function public.list_dupe_clusters()
returns table(data jsonb)
language sql
stable
security definer
as $$
  with cluster_rows as (
    select duplicate_cluster_id as cluster_id,
           fid, name, state, county_name, geom, duplicate_status
    from public.us_beach_points
    where duplicate_cluster_id is not null
  ),
  cluster_stats as (
    select
      cluster_id,
      count(*) as row_count,
      array_agg(distinct state)                                  as states,
      array_agg(distinct replace(county_name,' County',''))      as counties,
      array_agg(distinct name order by name)                     as names,
      bool_and(st_equals_lookup.eq) and bool_and(name_lookup.eq) as auto_resolvable,
      bool_and(duplicate_status in ('canonical','duplicate'))    as resolved,
      (array_agg(fid) filter (where duplicate_status = 'canonical'))[1] as canonical_fid,
      min(fid)                                                   as min_fid
    from cluster_rows cr
    cross join lateral (
      select bool_and(ST_Equals(cr.geom, other.geom)) as eq
      from cluster_rows other
      where other.cluster_id = cr.cluster_id
    ) st_equals_lookup
    cross join lateral (
      select bool_and(lower(trim(cr.name)) = lower(trim(other.name))) as eq
      from cluster_rows other
      where other.cluster_id = cr.cluster_id
    ) name_lookup
    group by cluster_id
  )
  select jsonb_build_object(
    'cluster_id',       cluster_id,
    'row_count',        row_count,
    'states',           states,
    'counties',         counties,
    'names',            names,
    'auto_resolvable',  auto_resolvable,
    'resolved',         resolved,
    'canonical_fid',    canonical_fid,
    'min_fid',          min_fid
  ) as data
  from cluster_stats
  -- Unresolved first (so pending work is up top). Within each section:
  -- auto-resolvable first, then CA, then cluster_id.
  order by resolved,
           auto_resolvable desc nulls last,
           (case when 'CA' = any(states) then 0 else 1 end),
           cluster_id;
$$;

revoke all on function public.list_dupe_clusters() from public, anon, authenticated;
grant  execute on function public.list_dupe_clusters() to service_role;

-- ── 3. resolve_dupe_cluster ───────────────────────────────────────────────
-- Promotes p_canonical_fid as canonical within its cluster; marks every
-- other row in the same cluster as 'duplicate'. Enforces the invariant
-- that the canonical row belongs to the same cluster (no cross-cluster
-- promotion).
--
-- Returns the before/after state for the affected rows so the caller can
-- log to admin_audit.
create or replace function public.resolve_dupe_cluster(
  p_cluster_id     int,
  p_canonical_fid  int
)
returns table(before jsonb, after jsonb)
language plpgsql
security definer
as $$
declare
  row_before jsonb;
  row_after  jsonb;
  r record;
begin
  -- Pre-check: canonical row must belong to the cluster
  perform 1 from public.us_beach_points
    where fid = p_canonical_fid and duplicate_cluster_id = p_cluster_id;
  if not found then
    raise exception 'fid % is not in cluster %', p_canonical_fid, p_cluster_id;
  end if;

  -- Iterate every row in the cluster, update + emit before/after pair
  for r in
    select fid, duplicate_status as prev_status
    from public.us_beach_points
    where duplicate_cluster_id = p_cluster_id
    order by fid
  loop
    row_before := jsonb_build_object(
      'fid', r.fid,
      'duplicate_status', r.prev_status,
      'duplicate_cluster_id', p_cluster_id
    );

    if r.fid = p_canonical_fid then
      update public.us_beach_points
        set duplicate_status = 'canonical'
        where fid = r.fid;
      row_after := jsonb_build_object(
        'fid', r.fid,
        'duplicate_status', 'canonical',
        'duplicate_cluster_id', p_cluster_id
      );
    else
      update public.us_beach_points
        set duplicate_status = 'duplicate'
        where fid = r.fid;
      row_after := jsonb_build_object(
        'fid', r.fid,
        'duplicate_status', 'duplicate',
        'duplicate_cluster_id', p_cluster_id
      );
    end if;

    before := row_before;
    after  := row_after;
    return next;
  end loop;
end;
$$;

revoke all on function public.resolve_dupe_cluster(int, int) from public, anon, authenticated;
grant  execute on function public.resolve_dupe_cluster(int, int) to service_role;

-- ── 4. unpin_dupe_cluster ─────────────────────────────────────────────────
-- Clears dupe markings on every row in a cluster — use when human review
-- decides the rows weren't actually duplicates (e.g. Trinidad/Carmel River
-- misgeocode case where two different beaches share coords by error).
create or replace function public.unpin_dupe_cluster(p_cluster_id int)
returns table(before jsonb, after jsonb)
language plpgsql
security definer
as $$
declare
  r record;
begin
  for r in
    select fid, duplicate_status as prev_status
    from public.us_beach_points
    where duplicate_cluster_id = p_cluster_id
    order by fid
  loop
    before := jsonb_build_object(
      'fid', r.fid,
      'duplicate_status', r.prev_status,
      'duplicate_cluster_id', p_cluster_id
    );
    update public.us_beach_points
      set duplicate_status = null,
          duplicate_cluster_id = null
      where fid = r.fid;
    after := jsonb_build_object(
      'fid', r.fid,
      'duplicate_status', null,
      'duplicate_cluster_id', null
    );
    return next;
  end loop;
end;
$$;

revoke all on function public.unpin_dupe_cluster(int) from public, anon, authenticated;
grant  execute on function public.unpin_dupe_cluster(int) to service_role;

-- ── 5. bulk_resolve_identical_dupes ───────────────────────────────────────
-- Auto-resolves the subset of clusters where ALL rows are ST_Equals AND
-- have identical trim+lower name. Canonical = min(fid) in the cluster.
-- Other rows → 'duplicate'. Returns per-row before/after for audit.
-- Drop the no-arg signature before recreating with the new (text) signature
drop function if exists public.bulk_resolve_identical_dupes();

create or replace function public.bulk_resolve_identical_dupes(
  p_state text default null
)
returns table(cluster_id int, before jsonb, after jsonb)
language plpgsql
security definer
as $$
declare
  c record;
  r record;
begin
  for c in
    with cluster_rows as (
      select duplicate_cluster_id as cid, fid, name, geom, state
      from public.us_beach_points
      where duplicate_cluster_id is not null
        and duplicate_status = 'needs_review'
        and (p_state is null or state = p_state)
    ),
    qualifying as (
      -- A cluster qualifies only when ALL rows in it share the same geom
      -- AND the same (trim+lower) name. count(distinct …) = 1 enforces that
      -- — catches the case the earlier exists() pattern missed, where every
      -- row trivially ST_Equals itself so the filter admitted every cluster.
      select cid
      from cluster_rows
      group by cid
      having count(distinct ST_AsBinary(geom))   = 1
         and count(distinct lower(trim(name))) = 1
    )
    select q.cid,
           (select min(fid) from cluster_rows where cid = q.cid) as canonical_fid
    from qualifying q
    order by q.cid
  loop
    for r in
      select fid, duplicate_status as prev_status
      from public.us_beach_points
      where duplicate_cluster_id = c.cid
      order by fid
    loop
      cluster_id := c.cid;
      before := jsonb_build_object(
        'fid', r.fid,
        'duplicate_status', r.prev_status,
        'duplicate_cluster_id', c.cid
      );
      if r.fid = c.canonical_fid then
        update public.us_beach_points
          set duplicate_status = 'canonical'
          where fid = r.fid;
        after := jsonb_build_object(
          'fid', r.fid,
          'duplicate_status', 'canonical',
          'duplicate_cluster_id', c.cid
        );
      else
        update public.us_beach_points
          set duplicate_status = 'duplicate'
          where fid = r.fid;
        after := jsonb_build_object(
          'fid', r.fid,
          'duplicate_status', 'duplicate',
          'duplicate_cluster_id', c.cid
        );
      end if;
      return next;
    end loop;
  end loop;
end;
$$;

revoke all on function public.bulk_resolve_identical_dupes(text) from public, anon, authenticated;
grant  execute on function public.bulk_resolve_identical_dupes(text) to service_role;
