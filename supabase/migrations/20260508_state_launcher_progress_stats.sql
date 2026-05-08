-- 20260508_state_launcher_progress_stats.sql
--
-- Extend state_launcher_stats with the additional counters the launcher
-- UI needs to render per-step progress bars:
--   * gold_with_bep — distinct gold fids that have any BEP row
--   * extractions_total / extractions_completed — for the queue progress bar
--   * gold_with_governance — has a governance BEP row (PAD-US/etc.)

begin;

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
  v_gold_with_bep bigint;
  v_gold_with_governance bigint;
  v_bep bigint;
  v_queue_pending bigint;
  v_queue_completed bigint;
  v_queue_total bigint;
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
   where g.state = p_state and g.is_active = true;

  select count(distinct bep.gold_fid) into v_gold_with_bep
    from public.beach_enrichment_provenance bep
    join public.beaches_gold g on g.fid = bep.gold_fid
   where g.state = p_state and g.is_active = true;

  select count(distinct bep.gold_fid) into v_gold_with_governance
    from public.beach_enrichment_provenance bep
    join public.beaches_gold g on g.fid = bep.gold_fid
   where g.state = p_state and g.is_active = true and bep.field_group = 'governance';

  select count(*) filter (where q.completed_at is null),
         count(*) filter (where q.completed_at is not null),
         count(*)
    into v_queue_pending, v_queue_completed, v_queue_total
    from public.beach_extraction_queue q
    join public.beaches_gold g on g.fid = q.fid
   where g.state = p_state;

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
    'state',                 p_state,
    'arena_in_state',        v_arena,
    'arena_unpromoted',      v_arena_unpromoted,
    'gold_rows',             v_gold,
    'gold_scoreable',        v_gold_scoreable,
    'gold_with_bep',         v_gold_with_bep,
    'gold_with_governance',  v_gold_with_governance,
    'bep_rows',              v_bep,
    'queue_pending',         v_queue_pending,
    'queue_completed',       v_queue_completed,
    'queue_total',           v_queue_total,
    'pad_us_units',          v_pad_us,
    'noaa_stations',         v_noaa,
    'worst_noaa_km',         v_worst_km,
    'as_of',                 now()
  );
end;
$function$;

commit;
