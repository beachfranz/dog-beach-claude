-- 20260504_unified_pipeline_phase1_promote_to_gold_v2.sql
--
-- Bring promote_to_gold up to spec. Earlier version (move #1) predates
-- populate_from_unified_v1_gold (move #3) and consensus ensemble (Phase B).
-- The auto-promoter Phase B reads from beach_field_consensus, but that
-- table wasn't being populated inside promote_to_gold — so the chain
-- was missing two steps:
--   1. populate_from_unified_v1_gold (turn unified_v1 extractions into evidence)
--   2. compute_beach_field_consensus (Layer 2 cross-source ensemble)
--
-- This migration extends the promote_to_gold function so every step in
-- the pipeline is invoked. Per Franz's directive: "every process used is
-- a bonafide part of the pipeline."

begin;

create or replace function public.promote_to_gold(
  p_fids bigint[],
  p_score boolean default false
)
returns table(
  rows_promoted    bigint,
  rows_already_in_gold bigint,
  containment_evidence  bigint,
  source_evidence       bigint,
  beach_dog_policy_changed bigint,
  beach_amenities_changed  bigint,
  noaa_assigned         bigint,
  slugs_assigned        bigint
) as $$
declare
  promoted    int := 0;
  existing    int := 0;
  fid_iter    bigint;
  contain_ev  int := 0;
  source_ev   int := 0;
  bdp_changed int := 0;
  ba_changed  int := 0;
  noaa_count  int := 0;
  slug_count  int := 0;
begin
  if p_fids is null or array_length(p_fids, 1) is null then
    return query select 0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint;
    return;
  end if;

  -- 1. Insert from arena into beaches_gold (Layer B spine)
  with to_promote as (
    select a.* from public.arena a
     where a.fid = any(p_fids) and a.is_active = true
       and not exists (select 1 from public.beaches_gold g where g.fid = a.fid)
  ),
  ins as (
    insert into public.beaches_gold
      (fid, location_id, name, lat, lon, county_name,
       source_code, source_id, group_id,
       nav_lat, nav_lon, nav_source, name_source,
       park_name, state, promoted_from, promoted_at, is_active,
       noaa_station_id, timezone, open_time, close_time,
       is_scoreable, geom)
    select t.fid,
      public._make_location_slug(t.name, t.county_name, t.fid),
      t.name, t.lat, t.lon, t.county_name,
      t.source_code, t.source_id, coalesce(t.group_id, t.fid),
      t.nav_lat, t.nav_lon, t.nav_source, t.name_source,
      null,
      public._infer_state_from_county(t.county_name),
      'promote_to_gold_v2', now(), true,
      public._nearest_noaa_station(
        st_setsrid(st_makepoint(t.lon, t.lat), 4326)::geography),
      'America/Los_Angeles', '05:00', '22:00',
      coalesce(p_score, false),
      st_setsrid(st_makepoint(t.lon, t.lat), 4326)
    from to_promote t
    returning fid, location_id, noaa_station_id
  )
  select count(*),
         count(*) filter (where location_id is not null),
         count(*) filter (where noaa_station_id is not null)
    into promoted, slug_count, noaa_count
    from ins;

  existing := array_length(p_fids, 1) - promoted;

  -- 2. Layer C: spatial containment populators (per-fid)
  for fid_iter in select unnest(p_fids) loop
    perform public.populate_polygon_containment_gold(fid_iter);
  end loop;

  -- 3. Layer D source populators (per-fid; idempotent — emit only when
  --    extraction data already exists)
  for fid_iter in select unnest(p_fids) loop
    perform public.populate_from_cpad_gold(fid_iter);
    perform public.populate_from_park_operators_gold(fid_iter);
    perform public.populate_from_research_gold(fid_iter);
    perform public.populate_from_park_url_gold(fid_iter);
    perform public.populate_from_park_url_governance_gold(fid_iter);
    -- NEW: unified_v1 populator (Phase 1 move #3 catch-up)
    perform public.populate_from_unified_v1_gold(fid_iter);
  end loop;

  -- 4. Per-source resolvers (mark canonical evidence per beach × field_group × source)
  perform public._resolve_polygon_containment(null);
  perform public._resolve_governance_gold(null);
  perform public._resolve_dogs_gold(null);
  perform public._resolve_practical_gold(null);
  perform public._resolve_field_group_gold('access', null);

  -- 5. Layer 2 consensus: cross-source weighted voting (Phase B catch-up)
  --    Populates beach_field_consensus per (beach, voted-field).
  perform public.compute_beach_field_consensus(null);

  -- 6. Count evidence captured for the promoted set (after all populators)
  select count(*) into contain_ev
    from public.beach_enrichment_provenance
   where field_group = 'polygon_containment' and gold_fid = any(p_fids);
  select count(*) into source_ev
    from public.beach_enrichment_provenance
   where field_group in ('dogs','practical','access','governance')
     and gold_fid = any(p_fids);

  -- 7. Auto-promote canonical/consensus evidence into consumer tables
  for fid_iter in select unnest(p_fids) loop
    perform public.promote_canonical_to_consumer_tables(fid_iter);
  end loop;

  -- Count consumer-side changes for the promoted set
  select count(*) into bdp_changed
    from public.beach_dog_policy
   where arena_group_id = any(p_fids)
     and source = 'auto_promoted_from_consensus';
  select count(*) into ba_changed
    from public.beach_amenities
   where arena_group_id = any(p_fids)
     and source = 'auto_promoted_from_consensus';

  return query select
    promoted::bigint, existing::bigint, contain_ev::bigint, source_ev::bigint,
    bdp_changed::bigint, ba_changed::bigint,
    noaa_count::bigint, slug_count::bigint;
end;
$$ language plpgsql;

comment on function public.promote_to_gold(bigint[], boolean) is
  'Phase 1 unified pipeline v2 (2026-05-04). Steps: (1) INSERT to '
  'beaches_gold + auto-derive slug/noaa/timezone, (2) Layer C spatial '
  'containment, (3) Layer D source populators including unified_v1, '
  '(4) per-source resolvers, (5) Layer 2 consensus '
  '(compute_beach_field_consensus), (6) Layer D auto-promoter '
  '(promote_canonical_to_consumer_tables — reads consensus). '
  'Idempotent. Every step is a bonafide pipeline member.';

commit;
