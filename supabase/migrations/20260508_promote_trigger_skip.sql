-- 20260508_promote_trigger_skip.sql
--
-- tg_after_insert_gold_promote_chain fires for_each_row on INSERT into
-- beaches_gold and does FOUR per-row calls (consensus + resolve + promote
-- canonical + zone_rules). For single-row inserts (manual seed) this is
-- correct and useful. For bulk inserts via promote_to_gold (500+ rows
-- in WA's first launch) it's catastrophic — even with 120s per row,
-- aggregate runtime exceeds any reasonable budget.
--
-- promote_to_gold already does the same work via its foreach loops in
-- a saner order (populators → resolvers → publish). So during a bulk
-- promote, the trigger work is redundant.
--
-- Mirror the existing app.arena_clustering_active pattern: set
-- app.promote_to_gold_active='true' inside promote_to_gold; trigger
-- checks it and skips when set.

begin;

create or replace function public.tg_after_insert_gold_promote_chain()
returns trigger
language plpgsql
set statement_timeout to '120s'
as $function$
begin
  -- Skip during bulk promote_to_gold; the function's own foreach loops
  -- handle consensus + resolve + publish in a saner batch.
  if current_setting('app.promote_to_gold_active', true) = 'true' then
    return NEW;
  end if;
  perform public.compute_beach_field_consensus(NEW.fid);
  perform public._resolve_dogs_gold(NEW.fid);
  perform public.promote_canonical_dogs_to_beach_dog_policy(NEW.fid, 0.5);
  perform public._promote_zone_rules_for_fid(NEW.fid);
  return NEW;
end;
$function$;

-- Wrap promote_to_gold's body to set/clear the flag. We can't easily
-- modify the function body without re-pasting the whole thing; instead
-- create a thin wrapper that sets the flag, calls the existing function,
-- and clears the flag. But we must preserve the same signature so
-- existing callers work — so re-create the function with set_config
-- as the first line.

create or replace function public.promote_to_gold(
  p_fids bigint[], p_score boolean default false, p_publish boolean default true
)
returns table(
  rows_promoted bigint, rows_already_in_gold bigint,
  containment_evidence bigint, source_evidence bigint,
  beach_dog_policy_changed bigint, beach_amenities_changed bigint,
  noaa_assigned bigint, slugs_assigned bigint
)
language plpgsql
as $function$
declare promoted int := 0; existing int := 0; fid_iter bigint;
        contain_ev int := 0; source_ev int := 0;
        bdp_changed int := 0; ba_changed int := 0;
        noaa_count int := 0; slug_count int := 0;
        new_fids bigint[];
begin
  if p_fids is null or array_length(p_fids, 1) is null then
    return query select 0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint;
    return;
  end if;

  -- ★ Tell the after-insert trigger to stand down for this bulk operation.
  perform set_config('app.promote_to_gold_active', 'true', true);

  with to_promote as (
    select a.* from public.arena a
     where a.fid = any(p_fids) and a.is_active = true
       and not exists (select 1 from public.beaches_gold g where g.fid = a.fid)
       and not exists (
         select 1 from public.beaches_gold g
         join public.arena a2 on a2.fid = g.fid
         where a2.group_id = coalesce(a.group_id, a.fid) and g.fid <> a.fid
       )
  ),
  to_promote_with_canonical as (
    select t.*,
           coalesce(osm.nav_lat, t.nav_lat, t.lat) as canonical_lat,
           coalesce(osm.nav_lon, t.nav_lon, t.lon) as canonical_lon
      from to_promote t
      left join lateral (
        select nav_lat, nav_lon from public.arena
         where group_id = coalesce(t.group_id, t.fid)
           and source_code = 'osm' and is_active = true
           and nav_lat is not null and nav_lon is not null
         limit 1
      ) osm on true
  ),
  ins as (
    insert into public.beaches_gold
      (fid, location_id, name, lat, lon, county_name, source_code, source_id, group_id,
       nav_lat, nav_lon, nav_source, name_source, park_name, state, promoted_from, promoted_at,
       is_active, noaa_station_id, timezone, open_time, close_time, is_scoreable, geom)
    select t.fid, public._make_location_slug(t.name, t.county_name, t.fid),
      t.name, t.canonical_lat, t.canonical_lon, t.county_name, t.source_code, t.source_id,
      coalesce(t.group_id, t.fid),
      t.nav_lat, t.nav_lon, t.nav_source, t.name_source, null,
      coalesce(
        public._infer_state_from_county_fips(t.county_fips),
        public._infer_state_from_county(t.county_name)
      ),
      'promote_to_gold_v3', now(), true,
      public._nearest_noaa_station(st_setsrid(st_makepoint(t.canonical_lon, t.canonical_lat), 4326)::geography),
      'America/Los_Angeles', '05:00', '22:00', coalesce(p_score, false),
      st_setsrid(st_makepoint(t.canonical_lon, t.canonical_lat), 4326)
    from to_promote_with_canonical t
    returning fid, location_id, noaa_station_id
  )
  select array_agg(fid), count(*), count(*) filter (where location_id is not null),
         count(*) filter (where noaa_station_id is not null)
    into new_fids, promoted, slug_count, noaa_count from ins;
  existing := array_length(p_fids, 1) - promoted;

  foreach fid_iter in array p_fids loop
    perform public.populate_polygon_containment_gold(fid_iter);
    perform public.populate_from_cpad_gold(fid_iter);
    perform public.populate_from_pad_us_gold(fid_iter);
    perform public.populate_from_park_operators_gold(fid_iter);
    perform public.populate_from_research_gold(fid_iter);
    perform public.populate_from_park_url_gold(fid_iter);
    perform public.populate_from_park_url_governance_gold(fid_iter);
    perform public.populate_from_unified_v1_gold(fid_iter);
    perform public.populate_from_city_dog_policy_gold(fid_iter);
    perform public.populate_from_county_dog_policy_gold(fid_iter);
    perform public._resolve_polygon_containment(fid_iter);
    perform public._resolve_governance_gold(fid_iter);
    perform public._resolve_dogs_gold(fid_iter);
    perform public._resolve_practical_gold(fid_iter);
    perform public._resolve_field_group_gold('access', fid_iter);
  end loop;

  select count(*) into contain_ev from public.beach_enrichment_provenance
   where field_group = 'polygon_containment' and gold_fid = any(p_fids);
  select count(*) into source_ev from public.beach_enrichment_provenance
   where field_group in ('dogs','practical','access','governance')
     and gold_fid = any(p_fids);

  if p_publish then
    foreach fid_iter in array p_fids loop
      perform public.compute_beach_field_consensus(fid_iter);
      perform public.promote_canonical_to_consumer_tables(fid_iter);
    end loop;
    select count(*) into bdp_changed from public.beach_dog_policy
     where arena_group_id = any(p_fids) and source = 'auto_promoted_from_consensus';
    select count(*) into ba_changed from public.beach_amenities
     where arena_group_id = any(p_fids) and source = 'auto_promoted_from_consensus';
  end if;

  perform set_config('app.promote_to_gold_active', 'false', true);

  return query select promoted::bigint, existing::bigint, contain_ev::bigint, source_ev::bigint,
                      bdp_changed::bigint, ba_changed::bigint, noaa_count::bigint, slug_count::bigint;
end;
$function$;

alter function public.promote_to_gold(bigint[], boolean, boolean)
  set statement_timeout = '900s';

commit;
