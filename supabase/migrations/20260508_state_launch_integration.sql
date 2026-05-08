-- 20260508_state_launch_integration.sql
--
-- Two integration fixes from the OR launch lessons:
--
-- 1. FIPS-based state inference. _infer_state_from_county() has a
--    hardcoded list of OR/WA coastal counties + everything-else-is-CA
--    fallback. It misses Multnomah/Marion/Columbia/Hood River/Umatilla/
--    Clackamas (all OR, all caused 13–15 misclassifications per OR
--    launch).
--    Fix: derive state from county_fips (first 2 digits = state_fp),
--    fall back to the name-based function only when fips is null.
--
-- 2. POI landing state-launch reactivator. The CA pipeline marked
--    every non-CA us_beach_points row inactive (is_active=false,
--    state=null, is_dog_beach_signal=null/false, inactive_reason=
--    'not_ca_county'). Without manual intervention, every state launch
--    sees zero POI candidates. New function flips them all back from
--    us_beach_points truth in one call.

begin;

-- ── Fix #1 ──────────────────────────────────────────────────────────────
create or replace function public._infer_state_from_county_fips(p_fips text)
returns text language sql immutable as $$
  select case left(coalesce(p_fips, ''), 2)
           when '01' then 'AL' when '02' then 'AK' when '04' then 'AZ' when '05' then 'AR'
           when '06' then 'CA' when '08' then 'CO' when '09' then 'CT' when '10' then 'DE'
           when '11' then 'DC' when '12' then 'FL' when '13' then 'GA' when '15' then 'HI'
           when '16' then 'ID' when '17' then 'IL' when '18' then 'IN' when '19' then 'IA'
           when '20' then 'KS' when '21' then 'KY' when '22' then 'LA' when '23' then 'ME'
           when '24' then 'MD' when '25' then 'MA' when '26' then 'MI' when '27' then 'MN'
           when '28' then 'MS' when '29' then 'MO' when '30' then 'MT' when '31' then 'NE'
           when '32' then 'NV' when '33' then 'NH' when '34' then 'NJ' when '35' then 'NM'
           when '36' then 'NY' when '37' then 'NC' when '38' then 'ND' when '39' then 'OH'
           when '40' then 'OK' when '41' then 'OR' when '42' then 'PA' when '44' then 'RI'
           when '45' then 'SC' when '46' then 'SD' when '47' then 'TN' when '48' then 'TX'
           when '49' then 'UT' when '50' then 'VT' when '51' then 'VA' when '53' then 'WA'
           when '54' then 'WV' when '55' then 'WI' when '56' then 'WY'
           else null
         end;
$$;

grant execute on function public._infer_state_from_county_fips(text) to anon, authenticated, service_role;

-- promote_to_gold: prefer FIPS-derived state, fall back to county-name fn
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
      -- ★ FIPS-first state inference; fall back to name-based when fips null
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

  return query select promoted::bigint, existing::bigint, contain_ev::bigint, source_ev::bigint,
                      bdp_changed::bigint, ba_changed::bigint, noaa_count::bigint, slug_count::bigint;
end;
$function$;

-- One-shot backfill on existing gold rows whose state mismatches county_fips.
update public.beaches_gold g
   set state = public._infer_state_from_county_fips(a.county_fips)
  from public.arena a
 where a.fid = g.fid
   and a.county_fips is not null
   and public._infer_state_from_county_fips(a.county_fips) is not null
   and g.state is distinct from public._infer_state_from_county_fips(a.county_fips);


-- ── Fix #2 ──────────────────────────────────────────────────────────────
-- POI landing reactivator. Flips is_active, state, county_geoid,
-- county_name, is_dog_beach_signal, clears 'not_ca_county' inactive_reason
-- for every us_beach_points row in the requested state. Idempotent.

create or replace function public.reactivate_poi_landing_for_state(p_state text)
returns table(
  reactivated_count bigint,
  signal_flipped_count bigint,
  state_set_count bigint
)
language plpgsql
security definer
as $function$
declare
  v_reactivated bigint := 0;
  v_signal bigint := 0;
  v_state bigint := 0;
begin
  -- Reactivate + restore state/county from us_beach_points truth
  with u as (
    update public.poi_landing pl
       set is_active = true,
           state = ubp.state,
           county_geoid = coalesce(pl.county_geoid, ubp.county_fips),
           county_name  = coalesce(pl.county_name,  replace(coalesce(ubp.county_name,''),' County','')),
           inactive_reason = case when pl.inactive_reason = 'not_ca_county' then null
                                  else pl.inactive_reason end
      from public.us_beach_points ubp
     where pl.fid = ubp.fid and ubp.state = p_state and ubp.is_active = true
       and (pl.is_active is not true or pl.state is null
            or pl.inactive_reason = 'not_ca_county')
    returning pl.fid
  )
  select count(*) into v_reactivated from u;

  -- Flip is_dog_beach_signal=true (us_beach_points is by definition curated)
  with u as (
    update public.poi_landing pl
       set is_dog_beach_signal = true
      from public.us_beach_points ubp
     where pl.fid = ubp.fid and ubp.state = p_state and ubp.is_active = true
       and (pl.is_dog_beach_signal is null or pl.is_dog_beach_signal = false)
    returning pl.fid
  )
  select count(*) into v_signal from u;

  -- Set state where currently null
  with u as (
    update public.poi_landing pl
       set state = ubp.state
      from public.us_beach_points ubp
     where pl.fid = ubp.fid and ubp.state = p_state and ubp.is_active = true
       and pl.state is null
    returning pl.fid
  )
  select count(*) into v_state from u;

  return query select v_reactivated, v_signal, v_state;
end;
$function$;

grant execute on function public.reactivate_poi_landing_for_state(text) to anon, authenticated, service_role;

-- Wire into run_pipeline_for_state — call reactivator before fid selection.
-- Drop+recreate to keep return-shape aligned with the existing version.
drop function if exists public.run_pipeline_for_state(text, bigint[]);
create or replace function public.run_pipeline_for_state(
  p_state text default 'CA',
  p_fids  bigint[] default null
)
returns table(
  arena_grouped bigint,
  arena_extras_grouped bigint,
  promoted bigint,
  rows_already_in_gold bigint,
  dedup_kills bigint,
  bep_rows_migrated bigint,
  queue_processed bigint,
  extractions_enqueued bigint,
  poi_reactivated bigint,
  ran_at timestamptz
)
language plpgsql
security definer
as $function$
declare
  v_fids bigint[];
  v_cluster_grp bigint := 0;
  v_extras_grp bigint := 0;
  v_promoted bigint := 0;
  v_existing bigint := 0;
  v_kills bigint := 0;
  v_bep_mig bigint := 0;
  v_queue bigint := 0;
  v_eq bigint := 0;
  v_poi bigint := 0;
  v_pf_rec record; v_pe_rec record; v_pg_rec record;
  v_dd_rec record; v_qq_rec record; v_eq_rec record; v_poi_rec record;
begin
  -- ★ Phase 0: Reactivate POI landing for this state (idempotent)
  select * into v_poi_rec from public.reactivate_poi_landing_for_state(p_state);
  v_poi := coalesce(v_poi_rec.reactivated_count, 0);

  -- Promote any newly-reactivated POI rows to arena
  perform public.promote_poi_landing_to_arena();

  select * into v_pf_rec from public.populate_arena_group_id();
  v_cluster_grp := coalesce(v_pf_rec.relation_grouped, 0)
                 + coalesce(v_pf_rec.name_clustered, 0)
                 + coalesce(v_pf_rec.poi_matched, 0);

  select * into v_pe_rec from public.populate_arena_extras();
  v_extras_grp := coalesce(v_pe_rec.intra_osm_trigram, 0)
                + coalesce(v_pe_rec.intra_poi, 0)
                + coalesce(v_pe_rec.cross_source_name, 0);

  if p_fids is not null and array_length(p_fids, 1) > 0 then
    v_fids := p_fids;
  else
    select array_agg(a.fid) into v_fids
      from public.arena a
     where a.is_active = true
       and not exists (select 1 from public.beaches_gold g where g.fid = a.fid)
       and a.county_fips is not null
       and public._infer_state_from_county_fips(a.county_fips) = p_state;
  end if;

  if v_fids is not null and array_length(v_fids, 1) > 0 then
    select * into v_pg_rec from public.promote_to_gold(v_fids, false);
    v_promoted := coalesce(v_pg_rec.rows_promoted, 0);
    v_existing := coalesce(v_pg_rec.rows_already_in_gold, 0);
  end if;

  select * into v_dd_rec from public.run_late_stage_dedup();
  v_kills := coalesce(v_dd_rec.kills, 0);
  v_bep_mig := coalesce(v_dd_rec.bep_rows_migrated, 0);

  select * into v_qq_rec from public.process_geom_change_queue(100);
  v_queue := coalesce(v_qq_rec.fids_processed, 0);

  select * into v_eq_rec from public.enqueue_missing_extractions();
  v_eq := coalesce(v_eq_rec.queue_total, 0);

  return query select v_cluster_grp, v_extras_grp, v_promoted, v_existing,
                       v_kills, v_bep_mig, v_queue, v_eq, v_poi, now();
end;
$function$;

alter function public.run_pipeline_for_state(text, bigint[])
  set statement_timeout = '180s';

commit;
