-- Dedupe columns + v2_ algorithm port for us_beach_points_staging (2026-04-24)
--
-- Two parts:
--   1. Add duplicate_cluster_id + duplicate_status to us_beach_points_staging
--      (mirrors us_beach_points; lets dedupe state live separately from
--      review_status so they don't collide).
--   2. Port v2_find_dedup_pairs + v2_find_neighbor_inheritance from the old
--      beaches_staging_new pipeline to target us_beach_points_staging,
--      upgraded to use PostGIS ST_DWithin (leverages the GIST index on geom)
--      instead of inline Haversine.

-- ── 1. Dedupe columns ────────────────────────────────────────────────────────

alter table public.us_beach_points_staging
  add column if not exists duplicate_cluster_id int,
  add column if not exists duplicate_status     text
    check (duplicate_status is null or duplicate_status in
      ('needs_review','duplicate','canonical'));

-- One canonical per cluster guardrail (matches us_beach_points)
create unique index if not exists ubps_one_canonical_per_cluster
  on public.us_beach_points_staging(duplicate_cluster_id)
  where duplicate_status = 'canonical';

create index if not exists ubps_dupe_cluster_idx
  on public.us_beach_points_staging(duplicate_cluster_id)
  where duplicate_cluster_id is not null;

comment on column public.us_beach_points_staging.duplicate_cluster_id is
  'Cluster id grouping duplicates of the same beach. Set by v2_find_dedup_pairs (or its derivative). Null = no cluster pinned.';
comment on column public.us_beach_points_staging.duplicate_status is
  'Per-row dedupe state: needs_review (cluster pinned, awaiting human), duplicate (resolved as a copy), canonical (resolved as the keeper). Null = not part of any cluster.';

-- ── 2a. Port v2_find_dedup_pairs ─────────────────────────────────────────────
-- Returns candidate pairs to be pinned as a cluster. Uses PostGIS ST_DWithin
-- on geom (geography) instead of inline Haversine — leverages the GIST index
-- and is geographic-aware (no equator distortion).
--
-- Tie-breaker chain (preserves the row most likely to be the keeper):
--   1. 'verified' or null review_status beats 'flagged' / 'needs_review'
--   2. Longer name beats shorter (assumes more specific)
--   3. Lower fid wins (deterministic fallback)
--
-- Excludes rows already marked duplicate or flagged.

create or replace function public.staging_find_dedup_pairs(
  max_distance_m  double precision default 50,
  min_similarity  double precision default 0.5
)
returns table (
  winner_fid  int,
  winner_name text,
  loser_fid   int,
  loser_name  text,
  dist_m      double precision,
  name_sim    double precision
)
language sql stable
as $$
  with candidates as (
    select fid, display_name, geom, review_status, duplicate_status
    from public.us_beach_points_staging
    where geom is not null
      and (duplicate_status is null or duplicate_status = 'needs_review')
      and (review_status is null or review_status <> 'flagged')
  ),
  pairs as (
    select
      a.fid as a_fid, a.display_name as a_name, a.review_status as a_status,
      b.fid as b_fid, b.display_name as b_name, b.review_status as b_status,
      similarity(a.display_name, b.display_name) as name_sim,
      st_distance(a.geom, b.geom) as dist_m
    from candidates a
    join candidates b
      on a.fid < b.fid
     and st_dwithin(a.geom, b.geom, max_distance_m)
  ),
  scored as (
    select
      case
        when (a_status is null or a_status = 'verified')
         and (b_status is distinct from null and b_status <> 'verified') then a_fid
        when (b_status is null or b_status = 'verified')
         and (a_status is distinct from null and a_status <> 'verified') then b_fid
        when length(a_name) > length(b_name) then a_fid
        when length(b_name) > length(a_name) then b_fid
        when a_fid < b_fid then a_fid
        else b_fid
      end as winner_fid,
      case
        when (a_status is null or a_status = 'verified')
         and (b_status is distinct from null and b_status <> 'verified') then a_name
        when (b_status is null or b_status = 'verified')
         and (a_status is distinct from null and a_status <> 'verified') then b_name
        when length(a_name) > length(b_name) then a_name
        when length(b_name) > length(a_name) then b_name
        when a_fid < b_fid then a_name
        else b_name
      end as winner_name,
      case
        when (a_status is null or a_status = 'verified')
         and (b_status is distinct from null and b_status <> 'verified') then b_fid
        when (b_status is null or b_status = 'verified')
         and (a_status is distinct from null and a_status <> 'verified') then a_fid
        when length(a_name) > length(b_name) then b_fid
        when length(b_name) > length(a_name) then a_fid
        when a_fid < b_fid then b_fid
        else a_fid
      end as loser_fid,
      case
        when (a_status is null or a_status = 'verified')
         and (b_status is distinct from null and b_status <> 'verified') then b_name
        when (b_status is null or b_status = 'verified')
         and (a_status is distinct from null and a_status <> 'verified') then a_name
        when length(a_name) > length(b_name) then b_name
        when length(b_name) > length(a_name) then a_name
        when a_fid < b_fid then b_name
        else a_name
      end as loser_name,
      dist_m,
      name_sim
    from pairs
    where name_sim >= min_similarity
  )
  -- One row per loser: closest winner takes precedence if a row matches multiple
  select distinct on (loser_fid)
    winner_fid, winner_name, loser_fid, loser_name, dist_m, name_sim
  from scored
  order by loser_fid, dist_m asc;
$$;

comment on function public.staging_find_dedup_pairs(double precision, double precision) is
  'Pair-discovery for us_beach_points_staging. Returns candidate (winner, loser) pairs within max_distance_m AND name similarity >= min_similarity. Defaults: 50m, 0.5 similarity. Caller decides whether to pin the resulting clusters via duplicate_cluster_id/duplicate_status. Ported from v2_find_dedup_pairs with PostGIS upgrade (ST_DWithin on geom).';

-- ── 2b. Port v2_find_neighbor_inheritance ────────────────────────────────────
-- Suggests governance inheritance from trusted-polygon-classified neighbors
-- to un-enriched rows. Trusted = governance source is one of cpad, pad_us,
-- tiger_places (all polygon-based). Point-source / LLM classifications are
-- intentionally excluded from the inheritance pool — polygons are spatially
-- authoritative.
--
-- Reads source from beach_enrichment_provenance (side table).
-- Returns suggestions only — caller decides whether to apply.

create or replace function public.staging_find_neighbor_inheritance(
  max_distance_m   double precision default 200,
  trusted_sources  text[] default array['cpad','pad_us','tiger_places']
)
returns table (
  unlocked_fid    int,
  unlocked_name   text,
  locked_fid      int,
  locked_name     text,
  locked_type     text,
  locked_body     text,
  locked_source   text,
  dist_m          double precision
)
language sql stable
as $$
  with unlocked as (
    -- Rows with no governance assigned yet
    select s.fid, s.display_name, s.geom
    from public.us_beach_points_staging s
    left join public.beach_enrichment_provenance p
      on p.fid = s.fid and p.field_group = 'governance'
    where s.geom is not null
      and p.fid is null
  ),
  locked as (
    -- Rows with governance from a trusted polygon source
    select s.fid, s.display_name, s.geom,
           s.governing_body_type, s.governing_body_name,
           p.source as gov_source
    from public.us_beach_points_staging s
    join public.beach_enrichment_provenance p
      on p.fid = s.fid and p.field_group = 'governance'
    where s.geom is not null
      and p.source = any (trusted_sources)
      and s.governing_body_type is not null
  ),
  pairs as (
    select
      u.fid as u_fid, u.display_name as u_name,
      l.fid as l_fid, l.display_name as l_name,
      l.governing_body_type as l_type,
      l.governing_body_name as l_body,
      l.gov_source as l_source,
      st_distance(u.geom, l.geom) as dist_m
    from unlocked u
    join locked l
      on st_dwithin(u.geom, l.geom, max_distance_m)
  )
  -- One inheritance per unlocked row: closest trusted neighbor wins
  select distinct on (u_fid)
    u_fid as unlocked_fid, u_name as unlocked_name,
    l_fid as locked_fid,   l_name as locked_name,
    l_type as locked_type, l_body as locked_body, l_source as locked_source,
    dist_m
  from pairs
  order by u_fid, dist_m asc;
$$;

comment on function public.staging_find_neighbor_inheritance(double precision, text[]) is
  'Suggests governance inheritance from trusted-polygon-classified neighbors to un-enriched rows in us_beach_points_staging. Trusted sources read from beach_enrichment_provenance.source. Returns suggestions; caller applies. Ported from v2_find_neighbor_inheritance with PostGIS upgrade and side-table provenance integration.';
