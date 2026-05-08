-- 20260508_run_late_stage_dedup.sql
--
-- Step 4 of the bulletproofing chain: codify late-stage dedup as a routine
-- pipeline step (Pin #29).
--
-- For every active arena group with multiple active beaches_gold members:
--   1. Apply pairwise _dedup_pair_safe() — skip the whole cluster if ANY
--      pair fails the safety gate (dog-suffix mismatch or sim < 0.7)
--   2. Score each member via _dedup_richness_score() with curator-touched
--      override (was_moved=true OR needs_review=true forces winner)
--   3. Manual-source rows are skipped from kill (highest tier; never lose)
--   4. Set winner's beaches_gold.geom to the group's OSM polygon centroid
--   5. Deactivate losers with inactive_reason='dupe_of_<keeper_fid>'
--
-- Returns counts: clusters_processed, clusters_skipped, kills, geom_swaps.
--
-- Safe to run repeatedly (idempotent — a cluster that already has only
-- one active member won't reappear in the work set).

begin;

create or replace function public.run_late_stage_dedup()
returns table(clusters_processed bigint, clusters_skipped_unsafe bigint, kills bigint, geom_swaps bigint)
language plpgsql
security definer
as $function$
declare
  v_processed bigint := 0;
  v_skipped bigint := 0;
  v_kills bigint := 0;
  v_geom_swaps bigint := 0;
begin
  with clusters as (
    select a.group_id,
           array_agg(g.fid order by g.fid) as fids,
           array_agg(g.name order by g.fid) as names
      from public.beaches_gold g
      join public.arena a on a.fid = g.fid
     where g.is_active = true and g.state = 'CA'
     group by a.group_id
    having count(*) > 1
  ),
  -- For each cluster, check pairwise safety. If ANY pair fails, the
  -- cluster is skipped.
  cluster_safety as (
    select c.group_id, c.fids, c.names,
           coalesce(
             (select bool_and(public._dedup_pair_safe(n1, n2, 0.7))
                from unnest(c.names) n1 cross join unnest(c.names) n2
               where n1 < n2),
             true  -- single name (all identical) is safe
           ) as is_safe
      from clusters c
  ),
  safe_clusters as (
    select group_id, fids from cluster_safety where is_safe
  ),
  unsafe_clusters as (
    select group_id from cluster_safety where not is_safe
  ),
  -- Expand safe clusters into one row per fid
  expanded as (
    select sc.group_id, unnest(sc.fids) as fid from safe_clusters sc
  ),
  -- Score each fid + curator-touched + manual-source flags
  scored as (
    select e.group_id, e.fid,
           public._dedup_richness_score(e.fid) as score,
           exists(select 1 from public.beach_location_curation_status s
                   where s.arena_group_id = e.fid and (s.was_moved or s.needs_review)) as is_curator,
           exists(select 1 from public.arena ar
                   where ar.fid = e.fid and ar.source_code = 'manual') as is_manual
      from expanded e
  ),
  -- Pick winner: curator-touched OR manual highest, then richness, then low fid.
  -- Manual + curator tiebreak with each other by lower fid.
  ranked as (
    select group_id, fid, is_curator, is_manual,
           row_number() over (
             partition by group_id
             order by (is_curator or is_manual) desc, score desc, fid asc
           ) as rnk
      from scored
  ),
  winners as (select group_id, fid as winner_fid from ranked where rnk = 1),
  -- Losers exclude manual-source rows and curator-touched rows. Those
  -- never get deactivated even if they didn't win (highest tier wins by
  -- default but if the cluster has multiple manual+curator rows we keep
  -- both rather than picking — the cluster is contested).
  candidate_losers as (
    select r.group_id, r.fid as loser_fid
      from ranked r
     where r.rnk > 1
       and not r.is_curator
       and not r.is_manual
  ),
  losers as (
    select cl.group_id, cl.loser_fid
      from candidate_losers cl
      join winners w on w.group_id = cl.group_id
  ),
  -- For each safe cluster, find the OSM polygon centroid coord.
  osm_centroid as (
    select sc.group_id,
           st_setsrid(st_makepoint(a_osm.nav_lon, a_osm.nav_lat), 4326) as geom
      from safe_clusters sc
      cross join lateral (
        select nav_lat, nav_lon
          from public.arena
         where group_id = sc.group_id
           and source_code = 'osm'
           and is_active = true
           and nav_lat is not null and nav_lon is not null
         limit 1
      ) a_osm
  ),
  do_geom as (
    update public.beaches_gold g
       set geom = oc.geom
      from winners w
      join osm_centroid oc on oc.group_id = w.group_id
     where g.fid = w.winner_fid
       and g.is_active = true
    returning g.fid
  ),
  do_kills as (
    update public.beaches_gold g
       set is_active = false,
           inactive_reason = 'dupe_of_'||w.winner_fid::text
      from losers l
      join winners w on w.group_id = l.group_id
     where g.fid = l.loser_fid
       and g.is_active = true
    returning g.fid
  )
  select
    (select count(*) from safe_clusters),
    (select count(*) from unsafe_clusters),
    (select count(*) from do_kills),
    (select count(*) from do_geom)
  into v_processed, v_skipped, v_kills, v_geom_swaps;

  return query select v_processed, v_skipped, v_kills, v_geom_swaps;
end;
$function$;

grant execute on function public.run_late_stage_dedup() to anon, authenticated, service_role;

commit;
