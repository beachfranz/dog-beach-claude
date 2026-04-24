-- Duplicate marking for us_beach_points (2026-04-25)
-- Adds cluster tracking columns + marks confirmed-duplicate clusters as
-- needs_review per project_dedupe_canonical_rule.md. No automatic canonical
-- selection — credible-source metadata must differ before a canonical can
-- be picked, and for coord-identical dupes it doesn't.
--
-- Confirmed-dupe scope: exact-name match (trim + lower) within 100m in the
-- same state. Captures T0 coord-identical + T1 exact-name + ≤100m bands.
--
-- Idempotent: clears prior markings first, then recomputes from current data.

-- ── Schema ────────────────────────────────────────────────────────────────

alter table public.us_beach_points
  add column if not exists duplicate_cluster_id int,
  add column if not exists duplicate_status     text;

alter table public.us_beach_points
  drop constraint if exists us_beach_points_duplicate_status_chk;
alter table public.us_beach_points
  add constraint us_beach_points_duplicate_status_chk
  check (duplicate_status is null
         or duplicate_status in ('needs_review','canonical','duplicate'));

create index if not exists us_beach_points_duplicate_cluster_idx
  on public.us_beach_points(duplicate_cluster_id)
  where duplicate_cluster_id is not null;

comment on column public.us_beach_points.duplicate_cluster_id is
  'Integer cluster id for duplicate rows; equals min(fid) across the cluster. NULL = not part of any known dupe cluster.';
comment on column public.us_beach_points.duplicate_status is
  'needs_review (unresolved) / canonical (kept row) / duplicate (filter out). NULL = no dupe tracking.';

-- ── Data: identify + mark confirmed dupes ─────────────────────────────────

-- Clear any prior markings so re-running is idempotent
update public.us_beach_points
set duplicate_cluster_id = null,
    duplicate_status     = null
where duplicate_cluster_id is not null;

-- Identify pairs, transitively close into clusters, assign min-fid as cluster id
with recursive
pairs as (
  select a.fid as a_fid, b.fid as b_fid
  from public.us_beach_points a
  join public.us_beach_points b
    on a.fid < b.fid
   and a.state = b.state
   and ST_DWithin(a.geom, b.geom, 0.005)
   and ST_Distance(a.geom::geography, b.geom::geography) <= 100
   and lower(trim(a.name)) = lower(trim(b.name))
),
edges as (
  select a_fid as src, b_fid as dst from pairs
  union all
  select b_fid as src, a_fid as dst from pairs
),
nodes as (
  select distinct src as fid from edges
),
reach(fid, visited) as (
  select fid, fid from nodes
  union
  select r.fid, e.dst
  from reach r
  join edges e on e.src = r.visited
),
clusters as (
  select fid, min(visited) as cluster_id
  from reach
  group by fid
)
update public.us_beach_points b
set duplicate_cluster_id = c.cluster_id,
    duplicate_status     = 'needs_review'
from clusters c
where b.fid = c.fid;
