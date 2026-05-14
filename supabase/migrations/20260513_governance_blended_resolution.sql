-- 20260513_governance_blended_resolution.sql
--
-- Replaces governance canonical selection with a three-signal additive
-- blend: match_confidence + authority_bonus + size_bonus.
--
-- BEFORE: _resolve_field_group_gold('governance', fid) → ranked by
-- source_priority desc, then confidence desc. Source priority dominated.
-- A weak federal "near" match (confidence 0.68) beat a clean city
-- "inside" match (0.95) because federal priority was higher. Crude.
--
-- AFTER: per Franz 2026-05-13:
--   score = confidence
--         + authority_bonus(polygon_kind, operator_level)
--         + size_bonus(area_m2)
--   canonical = max score per beach.
--
-- Confidence does the heavy lifting (a bad match never wins).
-- Authority is a smooth tilt toward federal/military/tribal.
-- Specificity rewards smaller, more focused polygons.

begin;

-- ────────────────────────────────────────────────────────────────────────
-- 1. Propagate area_m2 + mng_type into governance BEP claimed_values
--    so the resolver has full signal access. Updates the slim
--    populate_governance_gold.
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
    select kind, pc_claimed_values, confidence,
           public.polygon_to_operator(kind, pc_claimed_values, v_state) as operator_id
      from src
      where kind is not null
  ),
  upserted as (
    insert into public.beach_enrichment_provenance
      (gold_fid, field_group, source, confidence, claimed_values, operator_id, updated_at)
    select p_fid, 'governance',
           kind || '_governance' as source,
           confidence,
           -- Propagate fields needed by the blended resolver:
           jsonb_build_object(
             'polygon_kind',   kind,
             'polygon_id',     pc_claimed_values->>'polygon_id',
             'polygon_name',   pc_claimed_values->>'polygon_name',
             'operator_name',  (select canonical_name from public.operators
                                  where id = with_op.operator_id),
             'area_m2',        (pc_claimed_values->>'area_m2')::numeric,
             'match_strength', (pc_claimed_values->>'match_strength')::int,
             -- pad_us_unit-specific: keep mng_type for federal/state authority distinction
             'mng_type',       pc_claimed_values->>'mng_type',
             'own_type',       pc_claimed_values->>'own_type'
           ),
           operator_id,
           now()
      from with_op
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
-- 2. Helper: _governance_score
--    Computes the blended score for a single governance BEP row.
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
        when (p_claimed_values->>'area_m2')::numeric < 1e6   then 0.05  -- <1 km²
        when (p_claimed_values->>'area_m2')::numeric < 1e7   then 0.03  -- <10 km²
        when (p_claimed_values->>'area_m2')::numeric < 1e8   then 0.01  -- <100 km²
        else                                                      0.00
      end
$$;

grant execute on function public._governance_score(numeric, jsonb, text, text)
  to anon, authenticated, service_role;

-- ────────────────────────────────────────────────────────────────────────
-- 3. _resolve_governance_gold: custom resolver using the blended score.
--    Replaces the generic _resolve_field_group_gold call for governance.
--
--    Preserves the bad-name override (BLM/Coast Guard/DoD CPAD picks
--    that should defer to local jurisdictions) — those operators may
--    have correct match_confidence but the attribution is semantically
--    wrong (CPAD attributes them as "owner" when they're really a
--    different kind of stakeholder).
-- ────────────────────────────────────────────────────────────────────────

create or replace function public._resolve_governance_gold(p_fid bigint default null)
returns integer
language plpgsql
as $function$
declare
  v_picked int := 0;
  v_demoted int := 0;
begin
  -- Step 1: Clear all is_canonical flags for in-scope governance rows.
  update public.beach_enrichment_provenance
     set is_canonical = false
   where field_group = 'governance'
     and is_canonical = true
     and (p_fid is null or gold_fid = p_fid);

  -- Step 2: Pick canonical by blended score (one per beach).
  with scored as (
    select bep.id, bep.gold_fid,
           public._governance_score(
             bep.confidence,
             bep.claimed_values,
             bep.source,
             op.level
           ) as score
      from public.beach_enrichment_provenance bep
      left join public.operators op on op.id = bep.operator_id
     where bep.field_group = 'governance'
       and bep.gold_fid is not null
       and (p_fid is null or bep.gold_fid = p_fid)
  ),
  best as (
    select distinct on (gold_fid) id
      from scored
     order by gold_fid, score desc nulls last, id asc
  )
  update public.beach_enrichment_provenance bep
     set is_canonical = true,
         updated_at = now()
    from best
   where bep.id = best.id;
  get diagnostics v_picked = row_count;

  -- Step 3: Override pass — demote canonical attributions whose
  -- claimed_values name is a known mis-attribution (BLM/Coast Guard/
  -- DoD branches showing as "operators" via CPAD, plus CA-specific
  -- state-non-park entities). Promote the next-best alternative.
  with bad_canon as (
    select e.id, e.gold_fid
      from public.beach_enrichment_provenance e
     where e.field_group = 'governance'
       and e.is_canonical = true
       and (p_fid is null or e.gold_fid = p_fid)
       and (
         e.claimed_values->>'operator_name' ilike '%Bureau of Land Management%'
         or e.claimed_values->>'operator_name' ilike '%Coast Guard%'
         or e.claimed_values->>'operator_name' ilike '%Department of Defense%'
         or e.claimed_values->>'operator_name' ~* '\m(Navy|Marine Corps|Air Force|Army Corps)\M'
         or e.claimed_values->>'operator_name' = 'California State Lands Commission'
         or e.claimed_values->>'operator_name' = 'California State Coastal Conservancy'
         or e.claimed_values->>'operator_name' = 'Other State'
       )
  ),
  alternative as (
    -- Among the SAME beach's other governance candidates, pick the
    -- best non-denied one by blended score.
    select distinct on (e.gold_fid) e.id, e.gold_fid
      from public.beach_enrichment_provenance e
      left join public.operators op on op.id = e.operator_id
     where e.field_group = 'governance'
       and e.gold_fid in (select gold_fid from bad_canon)
       and not (
         e.claimed_values->>'operator_name' ilike '%Bureau of Land Management%'
         or e.claimed_values->>'operator_name' ilike '%Coast Guard%'
         or e.claimed_values->>'operator_name' ilike '%Department of Defense%'
         or e.claimed_values->>'operator_name' ~* '\m(Navy|Marine Corps|Air Force|Army Corps)\M'
         or e.claimed_values->>'operator_name' = 'California State Lands Commission'
         or e.claimed_values->>'operator_name' = 'California State Coastal Conservancy'
         or e.claimed_values->>'operator_name' = 'Other State'
       )
     order by e.gold_fid,
              public._governance_score(e.confidence, e.claimed_values, e.source, op.level) desc nulls last,
              e.id asc
  ),
  swaps as (
    select b.id as bad_id, a.id as good_id
      from bad_canon b
      join alternative a using (gold_fid)
  ),
  demoted as (
    update public.beach_enrichment_provenance e set is_canonical = false
     where e.id in (select bad_id from swaps)
    returning 1
  ),
  promoted as (
    update public.beach_enrichment_provenance e set is_canonical = true
     where e.id in (select good_id from swaps)
    returning 1
  )
  select count(*) into v_demoted from demoted;

  return v_picked + v_demoted;
end $function$;

commit;
