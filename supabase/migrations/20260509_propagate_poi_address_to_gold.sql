-- 20260509_propagate_poi_address_to_gold.sql
--
-- Add address propagation from poi_landing → beaches_gold to the
-- pipeline. poi_landing already has address_full / address_street /
-- address_city / address_state / address_zip filled (POI ingest pulls
-- structured addresses from Google Places). promote_to_gold wasn't
-- copying them to beaches_gold for any state — CA's coverage came from
-- a different (legacy) path.
--
-- This adds _enrich_address_from_poi_for_state(state) and wires it into
-- run_pipeline_for_state alongside _enrich_address_city_for_state.

begin;

create or replace function public._enrich_address_from_poi_for_state(p_state text)
returns int language plpgsql as $$
declare n int;
begin
  update public.beaches_gold g
     set address        = coalesce(g.address,        pl.address_full),
         address_street = coalesce(g.address_street, pl.address_street),
         address_city   = coalesce(g.address_city,   pl.address_city),
         address_state  = coalesce(g.address_state,  pl.address_state),
         address_zip    = coalesce(g.address_zip,    pl.address_zip)
    from public.poi_landing pl
   where pl.fid = g.fid
     and g.state = p_state
     and g.is_active
     and pl.address_full is not null;
  get diagnostics n = row_count;
  return n;
end $$;

-- Drop the existing 3-arg run_pipeline_for_state so we can change return type
drop function if exists public.run_pipeline_for_state(text, bigint[], boolean);

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
  cities_added int,
  counties_added int,
  address_city_enriched int,
  address_from_poi_enriched int,
  name_source_enriched int,
  dedup_kills bigint,
  bep_rows_migrated bigint,
  queue_processed bigint,
  ran_at timestamptz
)
language plpgsql security definer as $function$
declare
  v_fids bigint[];
  v_cluster_grp bigint := 0; v_extras_grp bigint := 0;
  v_promoted    bigint := 0; v_existing  bigint := 0;
  v_cities int := 0; v_counties int := 0;
  v_addr_n int := 0; v_addr_poi int := 0; v_nsrc_n int := 0;
  v_kills bigint := 0; v_bep_mig bigint := 0; v_queue bigint := 0;
  v_pf_rec record; v_pe_rec record; v_pg_rec record; v_dd_rec record; v_qq_rec record; v_op_rec record;
  v_state_fp text := case p_state
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
    when 'WI' then '55' when 'WY' then '56' else null end;
begin
  if not coalesce(p_skip_precheck, false) then
    perform * from public.assert_state_upstream_loaded(p_state);
  end if;

  select * into v_op_rec from public.populate_operators_for_state(p_state);
  v_cities := coalesce(v_op_rec.cities_added, 0);
  v_counties := coalesce(v_op_rec.counties_added, 0);

  select * into v_pf_rec from public.populate_arena_group_id();
  v_cluster_grp := coalesce(v_pf_rec.relation_grouped,0)+coalesce(v_pf_rec.name_clustered,0)+coalesce(v_pf_rec.poi_matched,0);
  select * into v_pe_rec from public.populate_arena_extras();
  v_extras_grp := coalesce(v_pe_rec.intra_osm_trigram,0)+coalesce(v_pe_rec.intra_poi,0)+coalesce(v_pe_rec.cross_source_name,0);

  if p_fids is not null and array_length(p_fids,1) > 0 then
    v_fids := p_fids;
  else
    select array_agg(a.fid) into v_fids from public.arena a
     where a.is_active = true and (
       exists(select 1 from public.beaches_gold g where g.fid = a.fid and g.state = p_state)
       or (a.county_fips is not null and v_state_fp is not null and exists(
           select 1 from public.counties c where c.geoid = a.county_fips and c.state_fp = v_state_fp))
     );
  end if;

  if v_fids is not null and array_length(v_fids,1) > 0 then
    select * into v_pg_rec from public.promote_to_gold(v_fids, false);
    v_promoted := coalesce(v_pg_rec.rows_promoted, 0);
    v_existing := coalesce(v_pg_rec.rows_already_in_gold, 0);
  end if;

  v_addr_n   := public._enrich_address_city_for_state(p_state);
  v_addr_poi := public._enrich_address_from_poi_for_state(p_state);  -- ★ NEW
  v_nsrc_n   := public._enrich_name_source_for_state(p_state);

  select * into v_dd_rec from public.run_late_stage_dedup();
  v_kills   := coalesce(v_dd_rec.kills, 0);
  v_bep_mig := coalesce(v_dd_rec.bep_rows_migrated, 0);

  select * into v_qq_rec from public.process_geom_change_queue(100);
  v_queue := coalesce(v_qq_rec.fids_processed, 0);

  return query select
    v_cluster_grp, v_extras_grp, v_promoted, v_existing,
    v_cities, v_counties, v_addr_n, v_addr_poi, v_nsrc_n,
    v_kills, v_bep_mig, v_queue, now();
end $function$;

grant execute on function public.run_pipeline_for_state(text, bigint[], boolean)
  to anon, authenticated, service_role;

commit;
