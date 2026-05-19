-- 20260519_codify_cascade_phase_helpers.sql
--
-- Helpers for the codify-cascade pipeline phase (docs/codify_cascade_phase_spec.md).
--
-- Adds:
--   public.count_uncovered_scoreable_beaches(p_state)
--     → count of active+scoreable beaches in state with ZERO beach_policy_source rows.
--     Pipeline gate: phase passes when count is 0 (or under per-state floor).
--
--   public.codify_federal_coverage(p_state)
--     → per-unit coverage map (PIP × policy_source) for the state's federal units.
--     Drives the Python orchestrator's precheck step.
--
-- Both are STABLE (idempotent) — no DML; pure reads.

begin;

-- ────────────────────────────────────────────────────────────────────
-- count_uncovered_scoreable_beaches(state)
-- ────────────────────────────────────────────────────────────────────
create or replace function public.count_uncovered_scoreable_beaches(
  p_state text
) returns int
language sql stable parallel safe
set search_path = 'public'
as $$
  select coalesce(count(*)::int, 0)
    from public.beaches_gold g
   where g.state = p_state
     and g.is_active
     and g.scoring_tier in ('daily','hourly')
     and not exists (
       select 1 from public.beach_policy_source bps
        where bps.beach_fid = g.fid
     )
$$;

grant execute on function public.count_uncovered_scoreable_beaches(text)
  to anon, authenticated, service_role;

comment on function public.count_uncovered_scoreable_beaches(text) is
  'Gate metric for codify-cascade phase: scoreable beaches in state with zero '
  'beach_policy_source rows. Phase fails if count > per-state floor.';


-- ────────────────────────────────────────────────────────────────────
-- codify_federal_coverage(state)
-- ────────────────────────────────────────────────────────────────────
-- Returns one row per (federal unit) in the state with:
--   mng_name          — agency code (NPS, BLM, FWS, USFS, NOAA, ...)
--   unit_name         — PAD-US unit_name
--   des_tp            — designation (NP, NM, NWR, NF, MPA, ...)
--   beaches_in_unit   — count of active scoreable beaches spatially in this unit
--   beaches_linked    — count of those beaches with a beach_policy_source row
--                       whose policy_source.issuing_agency matches the unit's agency
--   ps_ids            — array of policy_source ids linked to beaches in this unit
--   coverage_status   — 'covered' (all beaches linked) | 'partial' | 'uncovered'

create or replace function public.codify_federal_coverage(
  p_state text
) returns table (
  mng_name        text,
  unit_name       text,
  des_tp          text,
  beaches_in_unit int,
  beaches_linked  int,
  ps_ids          int[],
  coverage_status text
)
language sql stable parallel safe
set search_path = 'public'
as $$
  with units as (
    select pad.mng_name, pad.unit_name, pad.des_tp, b.fid as beach_fid
      from public.beach_relevant_pad_us pad
      join public.beaches_gold b on st_intersects(b.geom, pad.geom)
     where b.is_active
       and b.scoring_tier in ('daily','hourly')
       and b.state = p_state
       and pad.mng_type = 'FED'
  ), linked as (
    select u.mng_name, u.unit_name, u.des_tp, u.beach_fid,
           bps.policy_source_id
      from units u
      left join public.beach_policy_source bps on bps.beach_fid = u.beach_fid
  )
  select mng_name, unit_name, des_tp,
         count(distinct beach_fid)::int                                as beaches_in_unit,
         count(distinct beach_fid) filter (where policy_source_id is not null)::int
                                                                       as beaches_linked,
         coalesce(array_agg(distinct policy_source_id)
                  filter (where policy_source_id is not null), '{}')   as ps_ids,
         case
           when count(distinct beach_fid) filter (where policy_source_id is not null)
                = count(distinct beach_fid)                            then 'covered'
           when count(distinct beach_fid) filter (where policy_source_id is not null) > 0
                                                                       then 'partial'
           else                                                             'uncovered'
         end                                                            as coverage_status
    from linked
   group by mng_name, unit_name, des_tp
   order by mng_name, unit_name
$$;

grant execute on function public.codify_federal_coverage(text)
  to anon, authenticated, service_role;

comment on function public.codify_federal_coverage(text) is
  'Per-federal-unit coverage map: which PAD-US units contain scoreable beaches '
  'in this state and how many of those beaches already have a policy_source link. '
  'Drives the codify-cascade phase precheck — units with status=uncovered get '
  'queued for agent dispatch; partial may need a follow-up sweep; covered are '
  'skipped. Does NOT verify the linked ps actually belongs to the same agency '
  '(today, ON CONFLICT semantics make ANY ps link count); refine in v2 if needed.';

commit;
