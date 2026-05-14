-- 20260513_sample_gold_set_candidates.sql
--
-- Phase 26.5 — auto-sampler that populates gold_set_curation_queue with
-- stratified diversity per state.
--
-- Stratification:
--   1. Operator level: federal / state / county / city / unknown (5 buckets)
--   2. Within each bucket: mix dogs_allowed outcomes (yes/no/mixed/null)
--   3. Within each outcome: mix extraction confidence (high/low) to catch
--      both false-confidence and genuine-ambiguity cases
--
-- Field priority — 7 high-impact fields per beach × 25 beaches = ~175 rows
-- per state. Idempotent — re-runs don't duplicate (ON CONFLICT DO NOTHING).
--
-- See project_gold_set_curator_queue.md.

begin;

create or replace function public.sample_gold_set_candidates(
  p_state            text,
  p_target_beaches   int default 25,
  p_fields_per_beach int default 7
) returns int
language plpgsql
security definer
set search_path to 'public', 'pg_catalog'
as $function$
declare
  v_rows_enqueued int := 0;
  v_priority_fields text[] := array[
    'dogs_allowed',
    'leash_policy',
    'dogs_off_leash_area',
    'time_windows',
    'seasonal_closures',
    'lifeguard',
    'restrooms'
  ];
begin
  if p_state is null then
    raise exception 'sample_gold_set_candidates: p_state required';
  end if;

  -- ── Step 1: stratify scoreable beaches ───────────────────────────────
  --
  -- For each scoreable beach in state, derive (gov_bucket, dogs_outcome,
  -- conf_band) tuple. Then rank within tuple and pick N per stratum.
  --
  -- gov_bucket derived from canonical governance BEP row's source/polygon_kind:
  --   city: c1_city_governance, cdp_governance
  --   county: county_governance
  --   fed_state: pad_us_unit_governance (federal or state-managed)
  --   other: tribal_lands, nps_places, park_operators, tiger_places, name, manual
  --   none: no canonical governance row
  --
  with scoreable as (
    select g.fid, g.name, g.group_id,
           bdp.dogs_allowed,
           (select avg(confidence)
              from public.beach_enrichment_provenance bep
             where bep.gold_fid = g.fid and bep.field_group = 'dogs'
               and bep.confidence is not null) as avg_dogs_conf,
           (select bep.source from public.beach_enrichment_provenance bep
             where bep.gold_fid = g.fid and bep.field_group = 'governance'
               and bep.is_canonical = true
             limit 1) as gov_source
      from public.beaches_gold g
      left join public.beach_dog_policy bdp on bdp.arena_group_id = g.group_id
     where g.state = p_state
       and g.is_active
       and g.scoring_tier in ('daily','hourly')
  ),
  banded as (
    select fid, name, group_id, dogs_allowed, avg_dogs_conf, gov_source,
           case
             when gov_source in ('c1_city_governance','cdp_governance')                then 'city'
             when gov_source = 'county_governance'                                     then 'county'
             when gov_source = 'pad_us_unit_governance'                                then 'fed_state'
             when gov_source in ('tribal_lands','nps_places','park_operators',
                                 'tiger_places','name','manual','cpad_governance')    then 'other'
             else                                                                          'none'
           end as op_bucket,
           coalesce(dogs_allowed, 'unknown') as outcome_bucket,
           case
             when avg_dogs_conf is null  then 'no_data'
             when avg_dogs_conf >= 0.80  then 'high_conf'
             when avg_dogs_conf >= 0.50  then 'mid_conf'
             else                             'low_conf'
           end as conf_band
      from scoreable
  ),
  ranked as (
    select b.*,
           row_number() over (
             partition by op_bucket, outcome_bucket, conf_band
             order by md5(fid::text || p_state) -- deterministic pseudo-random
           ) as rk_within_stratum
      from banded b
  ),
  -- Target distribution per stratum — soft cap, fill from any stratum if needed
  picked as (
    select fid, name, op_bucket, outcome_bucket, conf_band,
           op_bucket || ':' || outcome_bucket || ':' || conf_band as stratum
      from ranked
     where rk_within_stratum <= 2  -- ≤2 per stratum gives ~30-40 per state w/ 5 op × 4 outcome × 4 conf = 80 strata
     limit p_target_beaches
  )
  -- ── Step 2: fan out across priority fields, look up current extraction
  insert into public.gold_set_curation_queue
    (state, gold_fid, field_name, sampled_reason,
     current_extraction_value, current_source, current_source_url, current_confidence)
  select p_state,
         p.fid,
         f.field_name,
         p.stratum,
         -- Pull current canonical extraction for the field (best-effort)
         (select bep.claimed_values
            from public.beach_enrichment_provenance bep
           where bep.gold_fid = p.fid
             and bep.is_canonical = true
             and bep.field_group = case f.field_name
               when 'dogs_allowed'        then 'dogs'
               when 'leash_policy'        then 'dogs'
               when 'dogs_off_leash_area' then 'dogs'
               when 'time_windows'        then 'dogs'
               when 'seasonal_closures'   then 'dogs'
               when 'lifeguard'           then 'practical'
               when 'restrooms'           then 'practical'
               else 'dogs'
             end
           limit 1) as current_extraction_value,
         (select bep.source from public.beach_enrichment_provenance bep
           where bep.gold_fid = p.fid and bep.is_canonical = true
             and bep.field_group = case f.field_name
               when 'lifeguard' then 'practical' when 'restrooms' then 'practical'
               else 'dogs' end
           limit 1) as current_source,
         (select bep.source_url from public.beach_enrichment_provenance bep
           where bep.gold_fid = p.fid and bep.is_canonical = true
             and bep.field_group = case f.field_name
               when 'lifeguard' then 'practical' when 'restrooms' then 'practical'
               else 'dogs' end
           limit 1) as current_source_url,
         (select bep.confidence from public.beach_enrichment_provenance bep
           where bep.gold_fid = p.fid and bep.is_canonical = true
             and bep.field_group = case f.field_name
               when 'lifeguard' then 'practical' when 'restrooms' then 'practical'
               else 'dogs' end
           limit 1) as current_confidence
    from picked p
    cross join unnest(v_priority_fields) with ordinality as f(field_name, ord)
   where f.ord <= p_fields_per_beach
  on conflict (state, gold_fid, field_name) do nothing;

  get diagnostics v_rows_enqueued = row_count;
  return v_rows_enqueued;
end $function$;

grant execute on function public.sample_gold_set_candidates(text, int, int)
  to anon, authenticated, service_role;

commit;
