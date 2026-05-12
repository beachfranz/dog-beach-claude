-- Refine run_late_stage_dedup further: instead of picking one winner per
-- arena cluster, group by (arena cluster, normalized name) into sub-clusters
-- and pick a winner per sub-cluster. Arena clustering is coarse — a single
-- cluster can contain multiple distinct beaches (Long Beach + Covell's +
-- Joshua's Pond all together). Name-aware sub-clustering is the right unit.

create or replace function public.run_late_stage_dedup(p_state text default 'CA')
returns table(clusters_processed bigint, clusters_skipped_unsafe bigint, kills bigint,
              geom_swaps bigint, bep_rows_migrated bigint, winners_re_promoted bigint)
language plpgsql
security definer
set statement_timeout to '300s'
set search_path to 'public', 'pg_catalog'
as $$
declare
  v_processed bigint := 0;
  v_skipped bigint := 0;
  v_kills bigint := 0;
  v_geom_swaps bigint := 0;
  v_bep_migrated bigint := 0;
  v_winners_repromoted bigint := 0;
  winner_fid_iter bigint;
begin
  -- Every active gold beach in a multi-active arena cluster, tagged with
  -- normalized name. Sub-cluster key = (group_id, name_lc).
  create temp table _dedup_work on commit drop as
  select g.fid, g.name,
         lower(coalesce(g.display_name_override, g.name)) as name_lc,
         a.group_id,
         public._dedup_richness_score(g.fid) as score,
         exists(select 1 from public.beach_location_curation_status s
                 where s.arena_group_id = g.fid and (s.was_moved or s.needs_review)) as is_curator,
         (a.source_code = 'manual') as is_manual
    from public.beaches_gold g
    join public.arena a on a.fid = g.fid
   where g.is_active = true and g.state = p_state;

  -- Filter to sub-clusters with ≥2 active beaches sharing a normalized name.
  -- Each (group_id, name_lc) sub-cluster is a real dup set.
  create temp table _subclusters on commit drop as
  select group_id, name_lc, count(*) as n_active
    from _dedup_work
   group by group_id, name_lc
  having count(*) > 1;

  v_processed := (select count(*) from _subclusters);

  -- Pick winner per sub-cluster: highest (curator/manual desc, score desc, fid asc)
  create temp table _winners on commit drop as
  with ranked as (
    select w.fid, w.group_id, w.name_lc, w.name,
           row_number() over (
             partition by w.group_id, w.name_lc
             order by (w.is_curator or w.is_manual) desc, w.score desc, w.fid asc
           ) as rnk
      from _dedup_work w
      join _subclusters s on s.group_id = w.group_id and s.name_lc = w.name_lc
  )
  select group_id, name_lc, fid as winner_fid, name as winner_name from ranked where rnk = 1;

  -- Losers: rank>1 within sub-cluster, not curator/manual-protected,
  -- and pair-safe vs winner (which is trivially true for identical names).
  create temp table _losers on commit drop as
  with ranked as (
    select w.fid, w.group_id, w.name_lc, w.name, w.is_curator, w.is_manual, w.score,
           row_number() over (
             partition by w.group_id, w.name_lc
             order by (w.is_curator or w.is_manual) desc, w.score desc, w.fid asc
           ) as rnk
      from _dedup_work w
      join _subclusters s on s.group_id = w.group_id and s.name_lc = w.name_lc
  )
  select r.group_id, r.name_lc, r.fid as loser_fid, r.name as loser_name,
         w.winner_fid, w.winner_name,
         public._dedup_pair_safe(r.name, w.winner_name, 0.7) as pair_safe
    from ranked r
    join _winners w on w.group_id = r.group_id and w.name_lc = r.name_lc
   where r.rnk > 1
     and not r.is_curator
     and not r.is_manual;

  v_skipped := (select count(*) from _losers where not pair_safe);

  -- BEP migration (only pair-safe losers)
  insert into public.beach_enrichment_provenance
    (gold_fid, field_group, source, confidence, claimed_values, source_url,
     cpad_unit_name, extraction_type, cpad_role)
  select l.winner_fid, bep.field_group, bep.source, bep.confidence,
         bep.claimed_values, bep.source_url,
         bep.cpad_unit_name, bep.extraction_type, bep.cpad_role
    from public.beach_enrichment_provenance bep
    join _losers l on l.loser_fid = bep.gold_fid
   where l.pair_safe
   on conflict (gold_fid, field_group, source) where gold_fid is not null
   do nothing;
  get diagnostics v_bep_migrated = row_count;

  -- Curator status migration
  insert into public.beach_location_curation_status
    (arena_group_id, curated_at, curated_by, was_moved, needs_review,
     before_lat, before_lng, after_lat, after_lng, distance_moved_m, notes)
  select l.winner_fid, s.curated_at, s.curated_by, s.was_moved, s.needs_review,
         s.before_lat, s.before_lng, s.after_lat, s.after_lng,
         s.distance_moved_m,
         coalesce(s.notes, '') || ' [migrated_from_'||l.loser_fid::text||']'
    from public.beach_location_curation_status s
    join _losers l on l.loser_fid = s.arena_group_id
   where l.pair_safe
     and (s.was_moved or s.needs_review)
     and not exists (
       select 1 from public.beach_location_curation_status w
        where w.arena_group_id = l.winner_fid
          and (w.was_moved or w.needs_review)
     )
   on conflict (arena_group_id) do nothing;

  -- Geom swap: prefer the OSM polygon centroid associated with the same
  -- arena cluster as the winner (winner gets the canonical centroid).
  with osm_centroid as (
    select w.winner_fid,
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

  -- Deactivate pair-safe losers
  with applied as (
    update public.beaches_gold g
       set is_active = false,
           inactive_reason = 'dupe_of_'||l.winner_fid::text
      from _losers l
     where g.fid = l.loser_fid
       and g.is_active = true
       and l.pair_safe
    returning g.fid
  )
  select count(*) into v_kills from applied;

  -- Re-fire consensus + promote for each winner that actually had kills
  for winner_fid_iter in
    select distinct w.winner_fid from _winners w
    where exists (select 1 from _losers l
                   where l.winner_fid = w.winner_fid and l.pair_safe)
  loop
    perform public.compute_beach_field_consensus(winner_fid_iter);
    perform public.promote_canonical_to_consumer_tables(winner_fid_iter);
    v_winners_repromoted := v_winners_repromoted + 1;
  end loop;

  return query select v_processed, v_skipped, v_kills, v_geom_swaps,
                      v_bep_migrated, v_winners_repromoted;
end;
$$;
