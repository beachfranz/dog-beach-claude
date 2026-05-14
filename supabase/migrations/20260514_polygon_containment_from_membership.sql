-- 20260514_polygon_containment_from_membership.sql
--
-- Phase 2 of beach_polygon_membership. Rewrites populate_polygon_containment_gold
-- to read from the materialized beach_polygon_membership table instead of
-- running 6 separate ST_Intersects scans per fid.
--
-- Wire-in: promote_to_gold runs refresh_beach_polygon_membership(state) ONCE
-- per launch (set-based, fast) before its per-fid build_beach_evidence loop.
-- Then build_beach_evidence calls populate_polygon_containment_gold which
-- now reads from the cache. Net cost: ~18s/fid → ~2s/fid.
--
-- Preserved semantics:
-- - Top-1 per (fid, polygon_kind) selected via window function (same sort)
-- - Computed fields (is_env_overlay, has_beach_word, name_token_overlap)
--   derived at read time from polygon_name + beach name
-- - claimed_values jsonb shape unchanged so downstream consensus/resolvers
--   continue to read the same fields
-- - Confidence via _polygon_match_confidence (joint matrix)
--
-- The 6 existing sub-populators (populate_cpad_containment_gold etc.) are
-- LEFT IN PLACE for ad-hoc / one-off use but no longer called from the
-- canonical chain.

begin;

-- ────────────────────────────────────────────────────────────────────────
-- Replace populate_polygon_containment_gold to use membership
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.populate_polygon_containment_gold(p_fid bigint default null)
returns table(emitted integer, resolved integer, promoted integer)
language plpgsql
set statement_timeout to '600s'
as $function$
declare
  v_emitted int := 0;
  v_resolved int := 0;
  v_promoted int := 0;
  v_state text;
begin
  -- ── Lazy populate membership if absent (defensive — promote_to_gold
  --    pre-warms per state, but geom-change-sensor + manual callers
  --    don't). For p_fid=null (whole table) we skip the lazy populate.
  if p_fid is not null
     and not exists (select 1 from public.beach_polygon_membership where gold_fid = p_fid)
  then
    select state into v_state from public.beaches_gold where fid = p_fid;
    if v_state is not null then
      perform * from public.refresh_beach_polygon_membership(v_state, p_fid);
    end if;
  end if;

  -- ── Read from beach_polygon_membership, pick top 1 per (fid, kind) ───
  with cands as (
    select m.gold_fid, m.polygon_kind, m.polygon_id, m.polygon_name,
           m.match_strength, m.dist_m, m.area_m2,
           m.mng_name, m.mng_type, m.own_name, m.own_type, m.loc_mang, m.raw_attrs,
           g.name as beach_name, g.display_name_override as beach_dn_override,
           -- Computed fields (matching populate_pad_us_containment_gold_v2 semantics)
           cardinality(public.shared_name_tokens(
             coalesce(g.display_name_override, g.name), m.polygon_name)) as name_token_overlap,
           coalesce(m.polygon_name ~* '\mbeach\M', false) as has_beach_word,
           coalesce(m.polygon_name ~* '(wilderness|wsa|acec|wsr|ira|nca)', false) as is_env_overlay
      from public.beach_polygon_membership m
      join public.beaches_gold g on g.fid = m.gold_fid
     where (p_fid is null or m.gold_fid = p_fid)
       and m.match_strength > 0
  ),
  ranked as (
    select c.*,
           row_number() over (
             partition by gold_fid, polygon_kind
             order by match_strength desc,
                      is_env_overlay asc,
                      has_beach_word desc,
                      name_token_overlap desc,
                      area_m2 asc,
                      polygon_id asc
           ) as rk
      from cands c
  ),
  best as (select * from ranked where rk = 1),
  source_map as (
    -- Map polygon_kind → BEP source string for legacy compatibility
    select b.*,
           case b.polygon_kind
             when 'pad_us_unit'  then 'pad_us_v2'
             when 'cpad_unit'    then 'cpad'
             when 'c1_city'      then 'jurisdictions'
             when 'cdp'          then 'jurisdictions'
             when 'county'       then 'counties'
             when 'military_base' then 'military_bases'
             when 'tribal_land'  then 'tribal_lands'
             else b.polygon_kind
           end as bep_source
      from best b
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, updated_at)
    select gold_fid, 'polygon_containment', bep_source,
           public._polygon_match_confidence(match_strength, name_token_overlap),
           jsonb_strip_nulls(jsonb_build_object(
             'polygon_kind',    polygon_kind,
             'polygon_id',      polygon_id,
             'polygon_name',    polygon_name,
             'own_type',        own_type,
             'own_name',        own_name,
             'mng_type',        mng_type,
             'mng_name',        mng_name,
             'loc_mang',        loc_mang,
             'des_tp',          raw_attrs->>'des_tp',
             'category',        raw_attrs->>'category',
             'pub_access',      raw_attrs->>'Pub_Access',
             'date_est',        raw_attrs->>'Date_Est',
             'gis_acres',       raw_attrs->>'GIS_Acres',
             'loc_ds',          raw_attrs->>'Loc_Ds',
             'is_env_overlay',  is_env_overlay,
             'has_beach_word',  has_beach_word,
             'name_token_overlap', name_token_overlap,
             'area_m2',         area_m2,
             'dist_m',          dist_m,
             'match_strength',  match_strength
           )), now()
      from source_map
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update set confidence     = excluded.confidence,
                  claimed_values = excluded.claimed_values,
                  updated_at     = now(),
                  is_canonical   = false
    returning 1
  )
  select count(*) into v_emitted from upserted;

  v_resolved := public._resolve_polygon_containment(p_fid);
  v_promoted := public._promote_polygon_containment_to_gold(p_fid);

  return query select v_emitted, v_resolved, v_promoted;
end $function$;

-- ────────────────────────────────────────────────────────────────────────
-- promote_to_gold: refresh membership once per state, before per-fid loop
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.promote_to_gold(
  p_fids bigint[],
  p_score boolean default false,
  p_publish boolean default true
)
returns table(rows_promoted bigint, rows_already_in_gold bigint,
              containment_evidence bigint, source_evidence bigint,
              beach_dog_policy_changed bigint, beach_amenities_changed bigint,
              noaa_assigned bigint, slugs_assigned bigint)
language plpgsql
as $function$
declare promoted int := 0; existing int := 0; fid_iter bigint;
        contain_ev int := 0; source_ev int := 0;
        bdp_changed int := 0; ba_changed int := 0;
        noaa_count int := 0; slug_count int := 0;
        new_fids bigint[];
        v_states text[];
        v_state text;
begin
  if p_fids is null or array_length(p_fids, 1) is null then
    return query select 0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint;
    return;
  end if;

  -- ── Step 1: INSERT new beaches_gold rows
  with to_promote as (
    select a.* from public.arena a
     where a.fid = any(p_fids) and a.is_active = true
       and not exists (select 1 from public.beaches_gold g where g.fid = a.fid)
       and not exists (
         select 1 from public.beaches_gold g
           join public.arena a2 on a2.fid = g.fid
          where a2.group_id = coalesce(a.group_id, a.fid)
            and g.is_active and g.fid <> a.fid
       )
  ),
  to_promote_with_canonical as (
    select t.*,
           coalesce(osm.nav_lat, t.nav_lat, t.lat) as canonical_lat,
           coalesce(osm.nav_lon, t.nav_lon, t.lon) as canonical_lon
      from to_promote t
      left join lateral (
        select nav_lat, nav_lon
          from public.arena
         where group_id = coalesce(t.group_id, t.fid)
           and source_code = 'osm' and is_active = true
           and nav_lat is not null and nav_lon is not null
         limit 1
      ) osm on true
  ),
  ins as (
    insert into public.beaches_gold
      (fid, location_id, name, lat, lon, county_name, county_fips,
       source_code, source_id, group_id,
       nav_lat, nav_lon, nav_source, name_source, park_name, state, promoted_from, promoted_at,
       is_active, noaa_station_id, timezone, open_time, close_time, geom)
    select t.fid, public._make_location_slug(t.name, t.county_name, t.fid),
      t.name, t.canonical_lat, t.canonical_lon, t.county_name, t.county_fips,
      t.source_code, t.source_id, coalesce(t.group_id, t.fid),
      t.nav_lat, t.nav_lon, t.nav_source, t.name_source, t.park_name,
      coalesce(public._infer_state_from_county_fips(t.county_fips),
               public._infer_state_from_county(t.county_name)),
      'promote_to_gold_v3', now(), true,
      public._nearest_noaa_station(st_setsrid(st_makepoint(t.canonical_lon, t.canonical_lat), 4326)::geography),
      'America/Los_Angeles', '05:00', '22:00',
      st_setsrid(st_makepoint(t.canonical_lon, t.canonical_lat), 4326)
    from to_promote_with_canonical t
    returning fid, location_id, noaa_station_id
  )
  select array_agg(fid), count(*), count(*) filter (where location_id is not null),
         count(*) filter (where noaa_station_id is not null)
    into new_fids, promoted, slug_count, noaa_count from ins;
  existing := array_length(p_fids, 1) - promoted;

  -- ── Step 1.5 (NEW 2026-05-14): membership lookup populated per-fid via
  -- lazy populate inside populate_polygon_containment_gold. State-batch
  -- refresh was tried but hit Cloudflare 524 timeouts on large states.
  -- Per-fid lazy populate is fast enough (bbox-pruned ~ms per fid) and
  -- handles new states without explicit pre-warming.

  -- ── Step 2: build the beach evidence chain for every fid (delegates)
  foreach fid_iter in array p_fids loop
    perform public.build_beach_evidence(fid_iter);
  end loop;

  select count(*) into contain_ev from public.beach_enrichment_provenance
   where field_group = 'polygon_containment' and gold_fid = any(p_fids);
  select count(*) into source_ev from public.beach_enrichment_provenance
   where field_group in ('dogs','practical','access','governance')
     and gold_fid = any(p_fids);

  select count(*) into bdp_changed from public.beach_dog_policy
   where arena_group_id = any(p_fids) and source = 'auto_promoted_from_consensus';
  select count(*) into ba_changed from public.beach_amenities
   where arena_group_id = any(p_fids) and source = 'auto_promoted_from_consensus';

  return query select promoted::bigint, existing::bigint, contain_ev::bigint, source_ev::bigint,
                      bdp_changed::bigint, ba_changed::bigint, noaa_count::bigint, slug_count::bigint;
end;
$function$;

commit;
