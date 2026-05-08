-- 20260508_state_launcher_helpers.sql
--
-- Two helpers for the state-launcher UI:
--   1. repair_noaa_for_state — re-runs _nearest_noaa_station for every gold
--      row in a state. Use case: after loading a state's NOAA stations,
--      pre-existing gold rows are still paired with whatever station was
--      closest at insert time (often a different state's station).
--   2. state_launcher_stats — single-call snapshot of all the counters the
--      UI wants to display (pre/post counts of arena/gold/BEP/queue/NOAA).

begin;

create or replace function public.repair_noaa_for_state(p_state text)
returns table(
  fids_repaired bigint,
  worst_km_before numeric,
  worst_km_after  numeric
)
language plpgsql
security definer
as $function$
declare
  v_repaired bigint := 0;
  v_before numeric := 0;
  v_after  numeric := 0;
begin
  -- Capture worst-case distance before
  select round(max(st_distance(g.geom::geography, n.geom::geography))::numeric / 1000, 1)
    into v_before
    from public.beaches_gold g
    join public.noaa_stations n on n.station_id = g.noaa_station_id
   where g.state = p_state and g.is_active = true;

  -- Re-pair to nearest. Only update where the nearest changed (avoids
  -- needless trigger fires).
  update public.beaches_gold g
     set noaa_station_id = public._nearest_noaa_station(g.geom::geography)
   where g.state = p_state
     and g.is_active = true
     and (g.noaa_station_id is distinct from public._nearest_noaa_station(g.geom::geography));
  get diagnostics v_repaired = row_count;

  -- Capture worst-case after
  select round(max(st_distance(g.geom::geography, n.geom::geography))::numeric / 1000, 1)
    into v_after
    from public.beaches_gold g
    join public.noaa_stations n on n.station_id = g.noaa_station_id
   where g.state = p_state and g.is_active = true;

  return query select v_repaired, v_before, v_after;
end;
$function$;

grant execute on function public.repair_noaa_for_state(text) to anon, authenticated, service_role;


create or replace function public.state_launcher_stats(p_state text)
returns jsonb
language plpgsql
security definer
as $function$
declare
  v_state_fp text := case p_state
                       when 'CA' then '06'
                       when 'OR' then '41'
                       when 'WA' then '53'
                     end;
  v_arena bigint;
  v_arena_unpromoted bigint;
  v_gold bigint;
  v_gold_scoreable bigint;
  v_bep bigint;
  v_queue_pending bigint;
  v_pad_us bigint;
  v_noaa bigint;
  v_worst_km numeric;
begin
  select count(*) into v_arena
    from public.arena a
   where a.is_active = true and a.county_fips is not null
     and exists (select 1 from public.counties c
                  where c.geoid = a.county_fips and c.state_fp = v_state_fp);

  select count(*) into v_arena_unpromoted
    from public.arena a
   where a.is_active = true and a.county_fips is not null
     and exists (select 1 from public.counties c
                  where c.geoid = a.county_fips and c.state_fp = v_state_fp)
     and not exists (select 1 from public.beaches_gold g where g.fid = a.fid);

  select count(*), count(*) filter (where is_scoreable)
    into v_gold, v_gold_scoreable
    from public.beaches_gold where state = p_state and is_active = true;

  select count(*) into v_bep
    from public.beach_enrichment_provenance bep
    join public.beaches_gold g on g.fid = bep.gold_fid
   where g.state = p_state;

  select count(*) into v_queue_pending
    from public.beach_extraction_queue q
    join public.beaches_gold g on g.fid = q.fid
   where g.state = p_state and q.completed_at is null;

  select count(*) into v_pad_us
    from public.pad_us_units where state = p_state;

  select count(*) into v_noaa
    from public.noaa_stations where state = p_state;

  select round(max(st_distance(g.geom::geography, n.geom::geography))::numeric / 1000, 1)
    into v_worst_km
    from public.beaches_gold g
    join public.noaa_stations n on n.station_id = g.noaa_station_id
   where g.state = p_state and g.is_active = true;

  return jsonb_build_object(
    'state',             p_state,
    'arena_in_state',    v_arena,
    'arena_unpromoted',  v_arena_unpromoted,
    'gold_rows',         v_gold,
    'gold_scoreable',    v_gold_scoreable,
    'bep_rows',          v_bep,
    'queue_pending',     v_queue_pending,
    'pad_us_units',      v_pad_us,
    'noaa_stations',     v_noaa,
    'worst_noaa_km',     v_worst_km,
    'as_of',             now()
  );
end;
$function$;

grant execute on function public.state_launcher_stats(text) to anon, authenticated, service_role;

commit;
