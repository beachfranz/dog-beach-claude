-- beach_cpad_candidates (2026-04-25)
--
-- One row per (beach, CPAD-Unit-within-300m) pair. Mirrors the candidate-
-- enumeration logic the dedupe-review page uses (admin-get-dupe-cluster
-- RPC, migration 20260424_dupe_cluster_rpcs.sql lines 192–205): bbox
-- prefilter at 0.003 deg, then exact 300m geography distance.
--
-- Purpose: expose the multi-candidate CPAD set per beach so that URL
-- discovery (park_url + agncy_web) can fan out across all nearby
-- managers, not just the single smallest containing polygon. Today the
-- park_url_scrape_queue view picks one CPAD per beach via DISTINCT ON
-- area; this table preserves the full candidate set for richer fan-out
-- and for ad-hoc queries.

drop table if exists public.beach_cpad_candidates;

create table public.beach_cpad_candidates (
  fid             integer not null,
  objectid        bigint  not null,
  candidate_rank  integer not null,
  distance_m      numeric not null,
  unit_id         text,
  unit_name       text,
  agncy_name      text,
  agncy_lev       text,
  mng_ag_lev      text,
  access_typ      text,
  park_url        text,
  agncy_web       text,
  area_m2         numeric,
  built_at        timestamptz not null default now(),
  primary key (fid, objectid)
);

create index beach_cpad_candidates_fid_idx
  on public.beach_cpad_candidates (fid);

create index beach_cpad_candidates_dist_idx
  on public.beach_cpad_candidates (fid, distance_m);

insert into public.beach_cpad_candidates
  (fid, objectid, candidate_rank, distance_m, unit_id, unit_name, agncy_name,
   agncy_lev, mng_ag_lev, access_typ, park_url, agncy_web, area_m2)
select
  b.fid,
  c.objectid,
  row_number() over (
    partition by b.fid
    order by ST_Distance(c.geom::geography, b.geom::geography) asc, c.objectid
  ) as candidate_rank,
  round(ST_Distance(c.geom::geography, b.geom::geography)::numeric, 0) as distance_m,
  c.unit_id, c.unit_name, c.agncy_name, c.agncy_lev, c.mng_ag_lev,
  c.access_typ, c.park_url, c.agncy_web,
  round(ST_Area(c.geom::geography)::numeric, 0) as area_m2
from public.us_beach_points b
join public.cpad_units c
  on ST_DWithin(c.geom, b.geom, 0.003)
 and ST_DWithin(c.geom::geography, b.geom::geography, 300)
where b.geom is not null;

analyze public.beach_cpad_candidates;
