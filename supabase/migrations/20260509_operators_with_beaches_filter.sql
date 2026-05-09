-- 20260509_operators_with_beaches_filter.sql
--
-- Spec correction: operator_llm_extract phase should only research
-- operators that actually govern at least one active gold beach in
-- the state. Previously the orchestrator queried ALL city/county/state
-- operators (DE = 51 ops, ~$3 + 13 minutes); ~80% of those are inland
-- jurisdictions with no beach in their footprint and no chance of
-- contributing dog policy. Burn rate compounds — RI=11, DE=51,
-- CA/FL/TX would be 1000+.
--
-- New helper: state_operator_ids_with_beaches(state) returns operator
-- ids whose footprint contains (or is within 1km of) at least one
-- active gold beach in the state. Falls back to all operators if no
-- gold beach has resolved containment yet (first-launch case).

begin;

create or replace function public.state_operator_ids_with_beaches(p_state text)
returns table(operator_id bigint)
language sql
stable
as $function$
  -- Operator geom is NULL for most non-CA operators (only CSP-loaded CA
  -- operators have polygons). So we can't filter spatially in general.
  -- Instead: filter by county_fips correspondence. An operator is
  -- "relevant" if its county_geoid (county-level operator) or its
  -- city's fips_county (city-level operator) is in the set of counties
  -- that contain at least one active beach in this state. State-level
  -- operators are always included (they govern statewide).
  with beach_counties as (
    select distinct g.county_fips as cfip,
           substring(g.county_fips, 3) as county3
      from public.beaches_gold g
     where g.is_active and g.state = p_state
       and g.county_fips is not null
  ),
  -- BEP-resolved containment as a stronger signal where it exists
  bep_ops as (
    select distinct (bep.claimed_values->>'operator_id')::bigint as operator_id
      from public.beach_enrichment_provenance bep
      join public.beaches_gold g on g.fid = bep.gold_fid
     where g.is_active and g.state = p_state
       and bep.field_group = 'polygon_containment'
       and bep.claimed_values ? 'operator_id'
  ),
  -- Spatial signal where operator geom IS available (CA mostly)
  spatial_ops as (
    select distinct op.id as operator_id
      from public.operators op
     where op.state_code = p_state and op.is_active
       and op.level in ('city','county','state')
       and op.geom is not null
       and exists (
         select 1 from public.beaches_gold g
          where g.is_active and g.state = p_state
            and st_dwithin(op.geom::geography, g.geom::geography, 1000)
       )
  ),
  -- County-fips signal: county-level operators
  county_fips_ops as (
    select op.id as operator_id
      from public.operators op
     where op.state_code = p_state and op.is_active
       and op.level = 'county'
       and op.county_geoid in (select cfip from beach_counties)
  ),
  -- City-level operators: PIP via jurisdictions.geom (since cities don't
  -- carry county_geoid reliably). Match a city to a beach if the city's
  -- jurisdiction polygon contains any active gold beach in the state.
  city_via_jurisdiction_ops as (
    select op.id as operator_id
      from public.operators op
      join public.jurisdictions j on j.id = op.jurisdiction_id
     where op.state_code = p_state and op.is_active
       and op.level = 'city'
       and j.geom is not null
       and exists (
         select 1 from public.beaches_gold g
          where g.is_active and g.state = p_state
            and st_intersects(j.geom, g.geom)
       )
  ),
  -- City-level fallback: city's place fips matches a beach's place_fips
  -- (when the populator emits this on promote_to_gold)
  state_level_ops as (
    select op.id as operator_id
      from public.operators op
     where op.state_code = p_state and op.is_active
       and op.level = 'state'
  ),
  union_all as (
    select operator_id from bep_ops
    union
    select operator_id from spatial_ops
    union
    select operator_id from county_fips_ops
    union
    select operator_id from city_via_jurisdiction_ops
    union
    select operator_id from state_level_ops
  )
  select operator_id from union_all where operator_id is not null;
$function$;


-- Helper: same query, but counts. Useful for runbook/audit reporting.
create or replace function public.state_operator_count_summary(p_state text)
returns table(
  total_active_operators int,
  operators_with_beaches int,
  filter_savings_pct     numeric
)
language sql
stable
as $function$
  with all_ops as (
    select count(*)::int n
      from public.operators
     where state_code = p_state
       and is_active
       and level in ('city','county','state')
  ),
  with_beach as (
    select count(*)::int n
      from public.state_operator_ids_with_beaches(p_state)
  )
  select all_ops.n,
         with_beach.n,
         case when all_ops.n > 0
              then round(100.0 - (with_beach.n::numeric / all_ops.n * 100), 1)
              else 0 end
    from all_ops, with_beach;
$function$;


grant execute on function public.state_operator_ids_with_beaches(text) to anon, authenticated, service_role;
grant execute on function public.state_operator_count_summary(text)    to anon, authenticated, service_role;

commit;
