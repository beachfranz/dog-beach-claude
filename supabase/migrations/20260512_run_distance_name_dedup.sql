-- Complementary dedup pass: catches same-normalized-name + same-county pairs
-- within p_max_distance_m, regardless of arena cluster membership.
--
-- Problem this solves: the late-stage dedup pipeline operates on arena
-- clusters. When OSM ingest and POI ingest create separate arena clusters
-- for the same beach, the cluster-based dedup can't see them as related.
-- Examples found 2026-05-12:
--   * Ocean Shores Bulkhead (WA) — fids 1115m apart, different arena clusters
--   * Picnic Beach (Orange CA) — fids 290m apart, different arena clusters
--   * Pico Beach (Plymouth MA) — fids in different arena clusters
--
-- Safety: same county + same normalized name is a strong signal. Pair-safety
-- (`_dedup_pair_safe`) still applies. 1500m default chosen because the dups
-- we've seen sit within ~1.1km; tighter would miss them, wider risks legit
-- distinct beaches in dense coastal counties (Cape Cod / Long Island patterns).

create or replace function public.find_distance_name_dup_pairs(
  p_state text,
  p_max_distance_m int default 1500
) returns table(f1 bigint, f2 bigint, lname text, county text, dist_m int)
language sql
stable
as $$
  with active as (
    select fid, lower(coalesce(display_name_override, name)) as lname,
           county_name, state, lat, lon
      from public.beaches_gold
     where is_active and is_scoreable and lat is not null and state = p_state
  )
  select a.fid, b.fid, a.lname, a.county_name,
         round(st_distance(
           st_makepoint(a.lon, a.lat)::geography,
           st_makepoint(b.lon, b.lat)::geography
         ))::int as dist_m
    from active a join active b
      on a.lname = b.lname
     and a.county_name = b.county_name
     and a.fid < b.fid
   where st_distance(
           st_makepoint(a.lon, a.lat)::geography,
           st_makepoint(b.lon, b.lat)::geography
         ) < p_max_distance_m;
$$;


create or replace function public.run_distance_name_dedup(
  p_state text,
  p_max_distance_m int default 1500
) returns table(pairs_found bigint, kills bigint, photos_migrated bigint, bep_migrated bigint)
language plpgsql
security definer
set statement_timeout to '300s'
set search_path to 'public', 'pg_catalog'
as $$
declare
  v_pairs bigint := 0;
  v_kills bigint := 0;
  v_photos_migrated bigint := 0;
  v_bep_migrated bigint := 0;
  v_tmp bigint;
  rec record;
begin
  -- Build candidate pairs into a temp table
  create temp table _dist_pairs on commit drop as
  select * from public.find_distance_name_dup_pairs(p_state, p_max_distance_m);

  v_pairs := (select count(*) from _dist_pairs);

  -- Score each fid involved and decide winner per pair.
  -- Skip pairs where either side is curator/manual-protected.
  for rec in
    with scored as (
      select p.f1, p.f2,
             public._dedup_richness_score(p.f1) s1,
             public._dedup_richness_score(p.f2) s2,
             exists(select 1 from public.beach_location_curation_status s
                     where s.arena_group_id in (p.f1, p.f2)
                       and (s.was_moved or s.needs_review)) any_curator,
             exists(select 1 from public.arena a
                     where a.fid in (p.f1, p.f2) and a.source_code='manual') any_manual
        from _dist_pairs p
    )
    select case when s1 >= s2 then f1 else f2 end as winner_fid,
           case when s1 >= s2 then f2 else f1 end as loser_fid
      from scored where not any_curator and not any_manual
  loop
    -- BEP migrate (skip on conflict)
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, source_url,
       cpad_unit_name, extraction_type, cpad_role)
    select rec.winner_fid, bep.field_group, bep.source, bep.confidence,
           bep.claimed_values, bep.source_url,
           bep.cpad_unit_name, bep.extraction_type, bep.cpad_role
      from public.beach_enrichment_provenance bep
     where bep.gold_fid = rec.loser_fid
     on conflict (gold_fid, field_group, source) where gold_fid is not null
     do nothing;
    get diagnostics v_tmp = row_count;
    v_bep_migrated := v_bep_migrated + v_tmp;

    -- Promote curated metadata on conflicting photo rows
    update public.beach_photos w
       set curated_at    = l.curated_at,
           curated_by    = l.curated_by,
           sort_order    = l.sort_order,
           match_quality = coalesce(l.match_quality, w.match_quality)
      from public.beach_photos l
     where l.arena_group_id = rec.loser_fid and w.arena_group_id = rec.winner_fid
       and l.source = w.source and l.external_id = w.external_id
       and l.curated_at is not null and w.curated_at is null;

    -- Migrate non-conflicting loser photos, recompute distance vs winner
    update public.beach_photos bp
       set arena_group_id = rec.winner_fid,
           distance_m = round(st_distance(
             st_makepoint(bp.lng, bp.lat)::geography,
             (select st_makepoint(lon, lat)::geography
                from public.beaches_gold where fid = rec.winner_fid)
           ))::int
     where bp.arena_group_id = rec.loser_fid
       and not exists (
         select 1 from public.beach_photos p2
          where p2.arena_group_id = rec.winner_fid
            and p2.source = bp.source
            and p2.external_id = bp.external_id
       );
    get diagnostics v_tmp = row_count;
    v_photos_migrated := v_photos_migrated + v_tmp;

    -- Delete remaining (conflict) loser photos
    delete from public.beach_photos where arena_group_id = rec.loser_fid;

    -- Reset curation_status: drop both, re-insert winner if it has any curated photos
    delete from public.beach_curation_status
     where arena_group_id in (rec.winner_fid, rec.loser_fid);
    insert into public.beach_curation_status (arena_group_id, curated_at)
    select rec.winner_fid, max(curated_at) from public.beach_photos
     where arena_group_id = rec.winner_fid and curated_at is not null
    having max(curated_at) is not null;

    -- Deactivate loser
    update public.beaches_gold
       set is_active = false, is_scoreable = false,
           inactive_reason = 'dupe_of_' || rec.winner_fid::text
     where fid = rec.loser_fid and is_active = true;

    v_kills := v_kills + 1;
  end loop;

  return query select v_pairs, v_kills, v_photos_migrated, v_bep_migrated;
end;
$$;
