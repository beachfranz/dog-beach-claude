-- 20260513_governance_name_sim_signal.sql
--
-- Adds name similarity between the BEACH name and the OPERATOR's
-- canonical_name as a fourth signal in governance canonical selection.
--
-- Three side-effects:
--   1. Rewards operator picks that are specifically named for this beach
--      (e.g. "Cannon Beach" beach → "City of Cannon Beach" operator).
--   2. Implicitly penalizes rows with no operator_id — they get 0
--      name_sim_bonus and fall behind operator-attributed rows on close
--      scores (resolves the "13 beaches with canonical=no_operator" gap
--      observed in the prior run).
--   3. Lets resolver consume one more signal that's already available
--      via shared_name_tokens(beach.name, operator.canonical_name).
--
-- The new four-signal additive score:
--   score = confidence
--         + authority_bonus(polygon_kind, operator_level)
--         + size_bonus(area_m2)
--         + name_sim_bonus(op_name_token_overlap)
--
-- name_sim_bonus tiers:
--   2+ overlapping tokens between beach and operator → +0.05
--   1 token  → +0.02
--   0 or no operator → 0

begin;

-- ────────────────────────────────────────────────────────────────────────
-- 1. Update populate_governance_gold to compute and persist
--    op_name_token_overlap into governance claimed_values.
-- ────────────────────────────────────────────────────────────────────────

create or replace function public.populate_governance_gold(p_fid bigint)
returns integer
language plpgsql
set statement_timeout to '600s'
as $function$
declare
  v_state text;
  v_total int := 0;
begin
  select state into v_state from public.beaches_gold where fid = p_fid;
  if v_state is null then return 0; end if;

  with src as (
    select
      bep.claimed_values->>'polygon_kind' as kind,
      bep.claimed_values             as pc_claimed_values,
      bep.confidence                 as confidence
    from public.beach_enrichment_provenance bep
    where bep.gold_fid = p_fid
      and bep.field_group = 'polygon_containment'
      and bep.is_canonical = true
  ),
  with_op as (
    select s.kind, s.pc_claimed_values, s.confidence,
           public.polygon_to_operator(s.kind, s.pc_claimed_values, v_state) as operator_id
      from src s
      where s.kind is not null
  ),
  with_op_meta as (
    select w.*,
           op.canonical_name as op_name,
           op.level          as op_level,
           -- name overlap between beach's name and operator's canonical_name
           coalesce(cardinality(public.shared_name_tokens(
             (select coalesce(display_name_override, name)
                from public.beaches_gold where fid = p_fid),
             op.canonical_name
           )), 0) as op_name_token_overlap
      from with_op w
      left join public.operators op on op.id = w.operator_id
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, operator_id, updated_at)
    select p_fid, 'governance',
           kind || '_governance' as source,
           confidence,
           jsonb_build_object(
             'polygon_kind',           kind,
             'polygon_id',             pc_claimed_values->>'polygon_id',
             'polygon_name',           pc_claimed_values->>'polygon_name',
             'operator_name',          op_name,
             'area_m2',                (pc_claimed_values->>'area_m2')::numeric,
             'match_strength',         (pc_claimed_values->>'match_strength')::int,
             'mng_type',               pc_claimed_values->>'mng_type',
             'own_type',               pc_claimed_values->>'own_type',
             -- ★ NEW: signal for the resolver's name_sim_bonus
             'op_name_token_overlap',  op_name_token_overlap
           ),
           operator_id,
           now()
      from with_op_meta
    on conflict (gold_fid, field_group, source) where gold_fid is not null
    do update set confidence     = excluded.confidence,
                  claimed_values = excluded.claimed_values,
                  operator_id    = excluded.operator_id,
                  updated_at     = now(),
                  is_canonical   = false
    returning 1
  )
  select count(*) into v_total from upserted;

  return v_total;
end $function$;

-- ────────────────────────────────────────────────────────────────────────
-- 2. _governance_score: now includes name_sim_bonus from
--    op_name_token_overlap stored in claimed_values.
-- ────────────────────────────────────────────────────────────────────────

create or replace function public._governance_score(
  p_confidence       numeric,
  p_claimed_values   jsonb,
  p_source           text,
  p_operator_level   text default null
)
returns numeric
language sql immutable as $$
  select
    coalesce(p_confidence, 0)
    -- ── authority_bonus ──────────────────────────────────────────────
    + case
        when p_source = 'manual' then 0.10
        when p_claimed_values->>'polygon_kind' = 'pad_us_unit'
             and (p_claimed_values->>'mng_type' = 'FED'
                  or p_operator_level = 'federal') then 0.08
        when p_claimed_values->>'polygon_kind' = 'military_base' then 0.07
        when p_claimed_values->>'polygon_kind' = 'pad_us_unit'
             and (p_claimed_values->>'mng_type' = 'STAT'
                  or p_operator_level = 'state')   then 0.06
        when p_claimed_values->>'polygon_kind' = 'tribal_land'  then 0.06
        when p_claimed_values->>'polygon_kind' = 'cpad_unit'    then 0.06
        when p_claimed_values->>'polygon_kind' = 'c1_city'      then 0.03
        when p_claimed_values->>'polygon_kind' = 'cdp'          then 0.02
        when p_claimed_values->>'polygon_kind' = 'county'       then 0.00
        else                                                       -0.05
      end
    -- ── size_bonus (smaller polygon = more specific) ─────────────────
    + case
        when (p_claimed_values->>'area_m2')::numeric is null then 0
        when (p_claimed_values->>'area_m2')::numeric < 1e6   then 0.05
        when (p_claimed_values->>'area_m2')::numeric < 1e7   then 0.03
        when (p_claimed_values->>'area_m2')::numeric < 1e8   then 0.01
        else                                                      0.00
      end
    -- ── name_sim_bonus (beach name ↔ operator name overlap) ──────────
    -- Rows without an operator_id have op_name_token_overlap = 0 →
    -- earn no name_sim_bonus, naturally falling behind attributed rows.
    + case
        when (p_claimed_values->>'op_name_token_overlap')::int >= 2 then 0.05
        when (p_claimed_values->>'op_name_token_overlap')::int >= 1 then 0.02
        else                                                              0.00
      end
$$;

commit;
