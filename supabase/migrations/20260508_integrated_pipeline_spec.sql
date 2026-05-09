-- 20260508_integrated_pipeline_spec.sql
--
-- Comprehensive pipeline-spec migration. This is the durable answer to
-- "what does run_pipeline_for_state do, and what data does it require".
--
-- Adds:
--
-- 1. public.assert_state_upstream_loaded(state) — raises if any of the
--    required external data sources for that state isn't 'ok' in the
--    public.external_source_status tracker. Run before pipeline so we
--    don't ship partial state launches like OR/WA were before today.
--
-- 2. Two new wired enrichment steps in run_pipeline_for_state, executed
--    after promote_to_gold and before late-stage dedup:
--      a. _enrich_address_city_for_state(state) — PIP against
--         public.jurisdictions to fill beaches_gold.address_city.
--      b. _enrich_name_source_for_state(state) — derive arena.name_source
--         from arena.source_code where null, then propagate to gold.
--
-- Scoring trigger (daily-beach-refresh) is intentionally NOT wired here
-- because the existing pattern bakes the admin secret into pg_proc as
-- plaintext, which the harness security classifier flagged. Trigger
-- daily-beach-refresh manually after a state launch (or wait for the
-- 9am UTC cron). The right fix is to read the secret from a config
-- table, deferred to a follow-up migration.
--
-- Full chain after this migration:
--   precheck → cluster → extras → promote (with all populators / resolvers
--   / OSM amenities emitter) → enrich address_city → enrich name_source
--   → late-stage dedup → drain geom queue.

begin;

-- ---------------------------------------------------------------
-- 1. Upstream-data precheck
-- ---------------------------------------------------------------

create or replace function public.assert_state_upstream_loaded(p_state text)
returns table(source text, status text, row_count int, last_loaded_at timestamptz)
language plpgsql
as $$
declare
  v_required text[] := array['pad_us', 'osm_landing', 'osm_amenities', 'tiger_places'];
  v_missing  text[] := array[]::text[];
  v_src text;
begin
  foreach v_src in array v_required loop
    if not exists (
      select 1 from public.external_source_status ess
       where ess.source = v_src and ess.state = p_state and ess.status = 'ok'
    ) then
      v_missing := v_missing || v_src;
    end if;
  end loop;
  if array_length(v_missing, 1) > 0 then
    raise exception
      'state %: missing upstream sources in external_source_status: % (status=ok required)',
      p_state, v_missing;
  end if;
  return query
    select s.source, s.status, s.row_count, s.last_loaded_at
      from public.external_source_status s
     where s.state = p_state
       and s.source = any(v_required)
     order by s.source;
end $$;

-- ---------------------------------------------------------------
-- 2. Post-promote enrichment helpers
-- ---------------------------------------------------------------

create or replace function public._enrich_address_city_for_state(p_state text)
returns int language plpgsql as $$
declare n int;
begin
  update public.beaches_gold g
     set address_city = j.name
    from public.jurisdictions j
   where g.state = p_state
     and g.is_active
     and g.address_city is null
     and ST_Contains(j.geom, g.geom);
  get diagnostics n = row_count;
  return n;
end $$;

create or replace function public._enrich_name_source_for_state(p_state text)
returns int language plpgsql as $$
declare n int := 0; m int;
begin
  -- Backfill arena.name_source from source_code (mirror of what landing
  -- triggers do for new rows; needed for already-promoted rows whose
  -- inheritance never fired).
  update public.arena a
     set name_source = case a.source_code
       when 'osm'             then 'osm_tag'
       when 'us_beach_points' then 'us_beach_points'
       when 'ccc'             then 'ccc'
       when 'cpad'            then 'cpad'
       when 'pad_us'          then 'pad_us'
       when 'manual'          then 'manual_seed'
       when 'poi'             then 'us_beach_points'
       else a.source_code
     end
   where a.name_source is null
     and a.name is not null
     and a.is_active
     and exists(
       select 1 from public.beaches_gold g
        where g.fid = a.fid and g.state = p_state
     );
  get diagnostics m = row_count;
  n := n + m;

  -- Roll forward to beaches_gold.
  update public.beaches_gold g
     set name_source = a.name_source
    from public.arena a
   where a.fid = g.fid
     and g.state = p_state
     and g.is_active
     and g.name_source is null
     and a.name_source is not null;
  get diagnostics m = row_count;
  n := n + m;

  return n;
end $$;

-- ---------------------------------------------------------------
-- 3. Updated run_pipeline_for_state — full chain (no scoring trigger)
-- ---------------------------------------------------------------

create or replace function public.run_pipeline_for_state(
  p_state text default 'CA',
  p_fids bigint[] default null,
  p_skip_precheck boolean default false
)
returns table(
  arena_grouped bigint,
  arena_extras_grouped bigint,
  promoted bigint,
  rows_already_in_gold bigint,
  address_city_enriched int,
  name_source_enriched int,
  dedup_kills bigint,
  bep_rows_migrated bigint,
  queue_processed bigint,
  ran_at timestamptz
)
language plpgsql
security definer
as $function$
declare
  v_fids bigint[];
  v_cluster_grp bigint := 0;
  v_extras_grp  bigint := 0;
  v_promoted    bigint := 0;
  v_existing    bigint := 0;
  v_addr_n      int    := 0;
  v_nsrc_n      int    := 0;
  v_kills       bigint := 0;
  v_bep_mig     bigint := 0;
  v_queue       bigint := 0;
  v_pf_rec      record;
  v_pe_rec      record;
  v_pg_rec      record;
  v_dd_rec      record;
  v_qq_rec      record;
  v_state_fp    text := case p_state
    when 'AL' then '01' when 'AK' then '02' when 'AZ' then '04' when 'AR' then '05'
    when 'CA' then '06' when 'CO' then '08' when 'CT' then '09' when 'DE' then '10'
    when 'FL' then '12' when 'GA' then '13' when 'HI' then '15' when 'ID' then '16'
    when 'IL' then '17' when 'IN' then '18' when 'IA' then '19' when 'KS' then '20'
    when 'KY' then '21' when 'LA' then '22' when 'ME' then '23' when 'MD' then '24'
    when 'MA' then '25' when 'MI' then '26' when 'MN' then '27' when 'MS' then '28'
    when 'MO' then '29' when 'MT' then '30' when 'NE' then '31' when 'NV' then '32'
    when 'NH' then '33' when 'NJ' then '34' when 'NM' then '35' when 'NY' then '36'
    when 'NC' then '37' when 'ND' then '38' when 'OH' then '39' when 'OK' then '40'
    when 'OR' then '41' when 'PA' then '42' when 'RI' then '44' when 'SC' then '45'
    when 'SD' then '46' when 'TN' then '47' when 'TX' then '48' when 'UT' then '49'
    when 'VT' then '50' when 'VA' then '51' when 'WA' then '53' when 'WV' then '54'
    when 'WI' then '55' when 'WY' then '56'
    else null
  end;
begin
  if not coalesce(p_skip_precheck, false) then
    perform * from public.assert_state_upstream_loaded(p_state);
  end if;

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
       and (
         exists(select 1 from public.beaches_gold g
                 where g.fid = a.fid and g.state = p_state)
         or (a.county_fips is not null and v_state_fp is not null and exists(
              select 1 from public.counties c
               where c.geoid = a.county_fips and c.state_fp = v_state_fp))
       );
  end if;

  if v_fids is not null and array_length(v_fids, 1) > 0 then
    select * into v_pg_rec from public.promote_to_gold(v_fids, false);
    v_promoted := coalesce(v_pg_rec.rows_promoted, 0);
    v_existing := coalesce(v_pg_rec.rows_already_in_gold, 0);
  end if;

  v_addr_n := public._enrich_address_city_for_state(p_state);
  v_nsrc_n := public._enrich_name_source_for_state(p_state);

  select * into v_dd_rec from public.run_late_stage_dedup();
  v_kills   := coalesce(v_dd_rec.kills, 0);
  v_bep_mig := coalesce(v_dd_rec.bep_rows_migrated, 0);

  select * into v_qq_rec from public.process_geom_change_queue(100);
  v_queue := coalesce(v_qq_rec.fids_processed, 0);

  return query select
    v_cluster_grp, v_extras_grp, v_promoted, v_existing,
    v_addr_n, v_nsrc_n,
    v_kills, v_bep_mig, v_queue, now();
end;
$function$;

grant execute on function public.run_pipeline_for_state(text, bigint[], boolean)
  to anon, authenticated, service_role;

comment on function public.run_pipeline_for_state(text, bigint[], boolean) is
  'Integrated state-launch pipeline: upstream precheck (PAD-US, osm_landing, osm_amenities, tiger_places must all be ''ok'' in external_source_status) → cluster → extras → promote_to_gold (with all populators, resolvers, OSM amenity emitter) → address_city PIP → name_source backfill → late-stage dedup → geom queue drain. Pass p_skip_precheck=true for ad-hoc reruns. Daily-beach-refresh is NOT triggered by this function (cron handles it nightly); call manually for a same-day refresh.';

commit;
