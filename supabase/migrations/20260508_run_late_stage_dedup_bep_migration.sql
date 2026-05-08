-- 20260508_run_late_stage_dedup_bep_migration.sql
--
-- Step 6 of the bulletproofing chain: BEP data preservation on dedup.
--
-- Earlier run_late_stage_dedup() deactivated losers but left their BEP rows
-- attached to loser_fid. The consumer surface only reads BEP for the winner
-- fid, so any unique evidence the loser had (manual_curator entries,
-- richer zone_rules sources, photos, etc.) goes dark.
--
-- This migration updates run_late_stage_dedup() to:
--   1. Migrate BEP rows from loser → winner (INSERT with ON CONFLICT DO
--      NOTHING so winner's existing rows win; loser's NEW evidence
--      gets copied)
--   2. Migrate beach_location_curation_status if loser is curator-touched
--      and winner isn't
--   3. Deactivate loser as before
--   4. Re-fire compute_beach_field_consensus + promote_canonical_to_
--      consumer_tables for each winner so the new evidence flows
--      through to beach_dog_policy / beach_amenities / zone_rules
--
-- Loser BEP rows are LEFT in place (gold_fid still points at deactivated
-- fid) for audit/rollback. They're orphaned but not destructive.
--
-- Photos (beach_photos) — separate cleanup pass, deferred.

begin;

create or replace function public.run_late_stage_dedup()
returns table(clusters_processed bigint, clusters_skipped_unsafe bigint, kills bigint, geom_swaps bigint, bep_rows_migrated bigint, winners_re_promoted bigint)
language plpgsql
security definer
as $function$
declare
  v_processed bigint := 0;
  v_skipped bigint := 0;
  v_kills bigint := 0;
  v_geom_swaps bigint := 0;
  v_bep_migrated bigint := 0;
  v_winners_repromoted bigint := 0;
  winner_fid_iter bigint;
begin
  -- Build the work set: clusters with multi-active gold + safe pairwise sims.
  create temp table _dedup_work on commit drop as
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
  cluster_safety as (
    select c.group_id, c.fids, c.names,
           coalesce(
             (select bool_and(public._dedup_pair_safe(n1, n2, 0.7))
                from unnest(c.names) n1 cross join unnest(c.names) n2
               where n1 < n2),
             true
           ) as is_safe
      from clusters c
  ),
  safe_clusters as (
    select group_id, fids from cluster_safety where is_safe
  )
  select sc.group_id, e.fid, public._dedup_richness_score(e.fid) as score,
         exists(select 1 from public.beach_location_curation_status s
                 where s.arena_group_id = e.fid and (s.was_moved or s.needs_review)) as is_curator,
         exists(select 1 from public.arena ar
                 where ar.fid = e.fid and ar.source_code = 'manual') as is_manual
    from safe_clusters sc, lateral unnest(sc.fids) as e(fid);

  v_skipped := (select count(*) from (
    select distinct group_id from (
      select a.group_id from public.beaches_gold g join public.arena a on a.fid = g.fid
      where g.is_active and g.state='CA' group by a.group_id having count(*) > 1
    ) all_clusters
    where group_id not in (select group_id from _dedup_work)
  ) _);

  v_processed := (select count(distinct group_id) from _dedup_work);

  -- Pick winners + losers
  create temp table _winners on commit drop as
  with ranked as (
    select group_id, fid, is_curator, is_manual,
           row_number() over (
             partition by group_id
             order by (is_curator or is_manual) desc, score desc, fid asc
           ) as rnk
      from _dedup_work
  )
  select group_id, fid as winner_fid from ranked where rnk = 1;

  create temp table _losers on commit drop as
  with ranked as (
    select group_id, fid, is_curator, is_manual,
           row_number() over (
             partition by group_id
             order by (is_curator or is_manual) desc, score desc, fid asc
           ) as rnk
      from _dedup_work
  )
  select r.group_id, r.fid as loser_fid, w.winner_fid
    from ranked r
    join _winners w on w.group_id = r.group_id
   where r.rnk > 1
     and not r.is_curator
     and not r.is_manual;

  -- BEP MIGRATION: copy loser's rows to winner where winner doesn't already
  -- have a row for the same (field_group, source). Preserves winner's
  -- canonical rows; adds loser's unique evidence.
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, source_url,
     cpad_unit_name, extraction_type, cpad_role)
  select l.winner_fid, bep.field_group, bep.source, bep.confidence,
         bep.claimed_values, bep.source_url,
         bep.cpad_unit_name, bep.extraction_type, bep.cpad_role
    from public.beach_enrichment_provenance bep
    join _losers l on l.loser_fid = bep.gold_fid
   on conflict (gold_fid, field_group, source) where gold_fid is not null
   do nothing;
  get diagnostics v_bep_migrated = row_count;

  -- CURATOR STATUS MIGRATION: copy was_moved/needs_review from loser to
  -- winner if loser is curator-touched and winner isn't.
  insert into public.beach_location_curation_status
    (arena_group_id, curated_at, curated_by, was_moved, needs_review,
     before_lat, before_lng, after_lat, after_lng, distance_moved_m, notes)
  select l.winner_fid, s.curated_at, s.curated_by, s.was_moved, s.needs_review,
         s.before_lat, s.before_lng, s.after_lat, s.after_lng,
         s.distance_moved_m,
         coalesce(s.notes, '') || ' [migrated_from_'||l.loser_fid::text||']'
    from public.beach_location_curation_status s
    join _losers l on l.loser_fid = s.arena_group_id
   where (s.was_moved or s.needs_review)
     and not exists (
       select 1 from public.beach_location_curation_status w
        where w.arena_group_id = l.winner_fid
          and (w.was_moved or w.needs_review)
     )
   on conflict (arena_group_id) do nothing;

  -- GEOM SWAP: set winner geom to OSM polygon centroid where available
  with osm_centroid as (
    select w.group_id, w.winner_fid,
           st_setsrid(st_makepoint(a_osm.nav_lon, a_osm.nav_lat), 4326) as geom
      from _winners w
      cross join lateral (
        select nav_lat, nav_lon
          from public.arena
         where group_id = w.group_id
           and source_code = 'osm'
           and is_active = true
           and nav_lat is not null and nav_lon is not null
         limit 1
      ) a_osm
  ),
  applied as (
    update public.beaches_gold g set geom = oc.geom
      from osm_centroid oc
     where g.fid = oc.winner_fid and g.is_active = true
    returning g.fid
  )
  select count(*) into v_geom_swaps from applied;

  -- DEACTIVATE LOSERS
  with applied as (
    update public.beaches_gold g
       set is_active = false,
           inactive_reason = 'dupe_of_'||l.winner_fid::text
      from _losers l
     where g.fid = l.loser_fid and g.is_active = true
    returning g.fid
  )
  select count(*) into v_kills from applied;

  -- RE-FIRE consensus + promote for each winner so migrated BEP flows
  -- to consumer surface
  for winner_fid_iter in select winner_fid from _winners loop
    perform public.compute_beach_field_consensus(winner_fid_iter);
    perform public.promote_canonical_to_consumer_tables(winner_fid_iter);
    v_winners_repromoted := v_winners_repromoted + 1;
  end loop;

  return query select v_processed, v_skipped, v_kills, v_geom_swaps,
                      v_bep_migrated, v_winners_repromoted;
end;
$function$;

commit;
