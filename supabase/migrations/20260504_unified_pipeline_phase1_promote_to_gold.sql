-- 20260504_unified_pipeline_phase1_promote_to_gold.sql
--
-- Unified pipeline phase 1, move 1: ONE canonical promotion path from
-- arena → beaches_gold. Replaces 4 competing paths (seed_arena_beach.py,
-- bulk_promote_socal.py, promote_arena_to_beaches_gold.py, direct INSERT)
-- with a single function that produces consistent end-state.
--
-- Closes the 2026-05-03 bug where 9 newly-promoted test beaches ended up
-- with NULL noaa_station_id and no location_id slug because direct INSERT
-- skipped the auto-derive steps.
--
-- What promote_to_gold(fids[], score=false) does:
--   1. INSERT FROM arena into beaches_gold (skip if already there)
--   2. Auto-derive: location_id slug, state, noaa_station_id (nearest
--      reference station within 100km via ST_DistanceKNN), timezone,
--      default open/close
--   3. Run populate_polygon_containment_gold per fid (5 sub-populators:
--      cpad, jurisdictions, counties, military, tribal)
--   4. Run populate_from_*_gold per fid (cpad, park_operators, research,
--      park_url, park_url_governance) — emits evidence from existing
--      extraction tables
--   5. Run resolvers (dogs, governance, practical, access)
--   6. Run promote_canonical_to_consumer_tables — auto-fills
--      beach_dog_policy + beach_amenities + beaches_gold open_time/close_time
--   7. Optionally flip is_scoreable=true (score=true)
--   8. Return summary: rows_promoted, beaches_with_evidence, etc.
--
-- Idempotent: re-running on the same fids re-runs the populators (which
-- ON CONFLICT DO NOTHING) and refreshes resolvers + auto-promoter.

begin;

-- ─────────────────────────────────────────────────────────────────────────
-- Helpers
-- ─────────────────────────────────────────────────────────────────────────

-- Slugify name + county into location_id (e.g. "Fiesta Island", "San Diego"
-- → "fiesta-island-san-diego"). Collision-safe: appends -<fid> on conflict.
create or replace function public._make_location_slug(p_name text, p_county text, p_fid bigint)
returns text language plpgsql immutable as $$
declare
  base text;
  s    text;
begin
  if p_name is null then return null; end if;
  base := lower(coalesce(p_name, '') || '-' || coalesce(p_county, ''));
  base := regexp_replace(base, '[''’]', '', 'g');
  base := regexp_replace(base, '[^a-z0-9]+', '-', 'g');
  s    := trim(both '-' from base);
  if s = '' then s := 'unnamed-' || p_fid::text; end if;
  return s;
end;
$$;

-- State default by county lookup (CA today; OR added; WA later)
create or replace function public._infer_state_from_county(p_county text)
returns text language sql immutable as $$
  select case
    when p_county is null then null
    when p_county in ('Clatsop','Tillamook','Lincoln','Lane','Coos','Curry')
      then 'OR'
    when p_county in ('Pacific','Grays Harbor','Jefferson','Clallam','Whatcom',
                      'Skagit','Snohomish','King','Pierce','Thurston','Mason',
                      'Kitsap','Island','San Juan','Wahkiakum')
      then 'WA'
    else 'CA'
  end;
$$;

-- Nearest NOAA reference (station_type='R') station within p_max_km. Uses KNN.
-- 64 of 192 NOAA stations are reference type; the rest are subordinate.
-- Reference stations are the canonical tide-data sources.
create or replace function public._nearest_noaa_station(p_geom geography, p_max_km numeric default 100)
returns text language sql stable as $$
  select station_id
    from public.noaa_stations
   where station_type = 'R'
     and st_distance(geom::geography, p_geom) <= (p_max_km * 1000)
   order by st_distance(geom::geography, p_geom)
   limit 1;
$$;

-- ─────────────────────────────────────────────────────────────────────────
-- The main promotion function
-- ─────────────────────────────────────────────────────────────────────────

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

  -- 1. Insert from arena into beaches_gold for fids that aren't already there
  with to_promote as (
    select a.*
      from public.arena a
     where a.fid = any(p_fids)
       and a.is_active = true
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
    select
      t.fid,
      public._make_location_slug(t.name, t.county_name, t.fid),
      t.name, t.lat, t.lon, t.county_name,
      t.source_code, t.source_id, coalesce(t.group_id, t.fid),
      t.nav_lat, t.nav_lon, t.nav_source, t.name_source,
      null,
      public._infer_state_from_county(t.county_name),
      'promote_to_gold_v1', now(), true,
      public._nearest_noaa_station(
        st_setsrid(st_makepoint(t.lon, t.lat), 4326)::geography),
      'America/Los_Angeles',
      '05:00', '22:00',
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

  -- 2. Run containment + source populators per promoted fid (and any
  --    fid in p_fids that was already in gold but might lack evidence)
  for fid_iter in select unnest(p_fids) loop
    -- Spatial containment
    perform public.populate_polygon_containment_gold(fid_iter);

    -- Source populators (idempotent — emit only if extraction data exists)
    perform public.populate_from_cpad_gold(fid_iter);
    perform public.populate_from_park_operators_gold(fid_iter);
    perform public.populate_from_research_gold(fid_iter);
    perform public.populate_from_park_url_gold(fid_iter);
    perform public.populate_from_park_url_governance_gold(fid_iter);
  end loop;

  -- 3. Run resolvers across all field groups (each picks canonical per beach)
  perform public._resolve_polygon_containment(null);
  perform public._resolve_governance_gold(null);
  perform public._resolve_dogs_gold(null);
  perform public._resolve_practical_gold(null);
  perform public._resolve_field_group_gold('access', null);

  -- Count evidence captured for the promoted set
  select count(*) into contain_ev
    from public.beach_enrichment_provenance
   where field_group = 'polygon_containment'
     and gold_fid = any(p_fids);
  select count(*) into source_ev
    from public.beach_enrichment_provenance
   where field_group in ('dogs','practical','access','governance')
     and gold_fid = any(p_fids);

  -- 4. Auto-promote canonical evidence into consumer tables
  for fid_iter in select unnest(p_fids) loop
    perform public.promote_canonical_to_consumer_tables(fid_iter);
  end loop;

  -- Count consumer-side changes for the promoted set
  select count(*) into bdp_changed
    from public.beach_dog_policy
   where arena_group_id = any(p_fids)
     and source = 'auto_promoted_from_resolver';
  select count(*) into ba_changed
    from public.beach_amenities
   where arena_group_id = any(p_fids)
     and source = 'auto_promoted_from_resolver';

  return query select
    promoted::bigint, existing::bigint, contain_ev::bigint, source_ev::bigint,
    bdp_changed::bigint, ba_changed::bigint,
    noaa_count::bigint, slug_count::bigint;
end;
$$ language plpgsql;

comment on function public.promote_to_gold(bigint[], boolean) is
  'Phase 1 unified pipeline (2026-05-04). ONE canonical path from arena to '
  'beaches_gold + all containment + source populators + resolvers + '
  'auto-promotion to consumer tables. Replaces seed_arena_beach.py, '
  'bulk_promote_socal.py, promote_arena_to_beaches_gold.py, and direct INSERT. '
  'Idempotent. Closes the 2026-05-03 bug where direct INSERTs left NULL '
  'noaa_station_id + missing slug. See docs/unified-pipeline.md "First moves #1".';

commit;
