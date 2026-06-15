-- 20260614i_dogs_operator_consistency_tiebreak.sql
--
-- Add operator-consistency tiebreak to the BEP cascade resolver. When
-- two BEP rows tie on (priority, confidence), prefer the one whose
-- bep.operator_id matches the beach's canonical_operator_id. NULL
-- bep.operator_id stays neutral (returns 1, doesn't gain or lose vs
-- other NULL rows; loses only to a positive-match competitor).
--
-- New ORDER BY:
--   priority ASC,
--   confidence DESC NULLS LAST,
--   (operator_id IS NOT NULL AND = canonical_operator_id ? 0 : 1) ASC,
--   id ASC
--
-- Sibling change to compute_dog_park_field_consensus on the dog-park
-- side per [[paired-functions-port-fixes-both-sides]].
--
-- Refire happens AFTER COMMIT to dodge the PL/pgSQL plan caching that
-- bit 20260613l. Scoped to fids with ≥2 BEP rows at the same
-- (priority, confidence), since the tiebreak only changes outcomes
-- there.
--
-- Reversibility: ship 20260614k_revert that self-patches both
-- resolvers back to their pre-20260614i ORDER BY. The bep.operator_id
-- column stays populated. The 20260613l revert (20260613m) is the
-- structural template.

BEGIN;

-- ─── 1. Patch _resolve_field_group_gold ─────────────────────────────────
DO $patch$
DECLARE
  v_src text;
  v_from_old text := E'from public.beach_enrichment_provenance e\n    where e.field_group = p_field_group';
  v_from_new text := E'from public.beach_enrichment_provenance e\n    join public.beaches_gold b on b.fid = e.gold_fid\n    where e.field_group = p_field_group';
  v_order_old text := E'order by public._gold_field_group_source_priority(e.field_group, e.source) asc,\n                 e.confidence desc nulls last,\n                 e.id asc';
  v_order_new text := E'order by public._gold_field_group_source_priority(e.field_group, e.source) asc,\n                 e.confidence desc nulls last,\n                 (case when e.operator_id is not null\n                        and e.operator_id = b.canonical_operator_id\n                       then 0 else 1 end) asc,\n                 e.id asc';
BEGIN
  v_src := pg_get_functiondef('public._resolve_field_group_gold(text,bigint)'::regprocedure);

  IF position(v_from_old IN v_src) = 0 THEN
    RAISE EXCEPTION '_resolve_field_group_gold: FROM anchor not found';
  END IF;
  IF position(v_order_old IN v_src) = 0 THEN
    RAISE EXCEPTION '_resolve_field_group_gold: ORDER BY anchor not found';
  END IF;

  v_src := replace(v_src, v_from_old,  v_from_new);
  v_src := replace(v_src, v_order_old, v_order_new);
  EXECUTE v_src;
END
$patch$;

-- ─── 2. CREATE OR REPLACE compute_dog_park_field_consensus ──────────────
CREATE OR REPLACE FUNCTION public.compute_dog_park_field_consensus(p_park_fid bigint)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
declare
  fg text;
begin
  for fg in
    select distinct field_group from public.dog_park_enrichment_provenance
     where dog_park_fid = p_park_fid
  loop
    update public.dog_park_enrichment_provenance
       set is_canonical = false
     where dog_park_fid = p_park_fid and field_group = fg;

    update public.dog_park_enrichment_provenance bep
       set is_canonical = true
     where bep.id = (
       select dpep.id
         from public.dog_park_enrichment_provenance dpep
        where dpep.dog_park_fid = p_park_fid
          and dpep.field_group = fg
          and dpep.claimed_values is not null
          and dpep.claimed_values <> '{}'::jsonb
        order by
          coalesce(dpep.confidence, 0) desc,
          -- 20260614i: operator-consistency tiebreak (sibling to
          -- _resolve_field_group_gold). NULL operator_id is neutral.
          (case when dpep.operator_id is not null
                 and dpep.operator_id = (select dpg.canonical_operator_id
                                           from public.dog_parks_gold dpg
                                          where dpg.fid = dpep.dog_park_fid)
                then 0 else 1 end) asc,
          case dpep.extraction_method
            when 'smart_fetch'  then 1
            when 'web_search'   then 2
            when 'agent_rescue' then 3
            else 4
          end,
          dpep.updated_at desc
        limit 1
     );
  end loop;
end;
$function$;

COMMIT;

-- ─── 3. Scoped refire (post-COMMIT, separate transaction) ───────────────
-- Refires the dogs cascade chain on fids where two BEP rows tie on
-- (priority, confidence) — those are the only fids where the new
-- tiebreak can change the canonical row. 20260613m line 21-23 lesson:
-- the refire runs OUTSIDE the schema migration's BEGIN/COMMIT to avoid
-- PL/pgSQL plan caching.
DO $refire$
DECLARE
  v_fid bigint;
  v_count int := 0;
BEGIN
  FOR v_fid IN
    WITH ties AS (
      SELECT e.gold_fid,
             public._gold_field_group_source_priority('dogs', e.source) AS prio,
             coalesce(e.confidence, -1) AS conf
        FROM public.beach_enrichment_provenance e
       WHERE e.field_group = 'dogs' AND e.gold_fid IS NOT NULL
       GROUP BY e.gold_fid, prio, conf
      HAVING count(*) >= 2
    )
    SELECT DISTINCT gold_fid FROM ties
  LOOP
    PERFORM public.compute_beach_field_consensus(v_fid);
    PERFORM public._resolve_dogs_gold(v_fid);
    PERFORM public.promote_canonical_dogs_to_beach_dog_policy(v_fid, 0.5);
    v_count := v_count + 1;
  END LOOP;
  RAISE NOTICE 'beach refire: % fids touched', v_count;
END
$refire$;

-- Sibling refire for dog-park side
DO $refire_dp$
DECLARE
  v_fid bigint;
  v_count int := 0;
BEGIN
  FOR v_fid IN
    WITH ties AS (
      SELECT dpep.dog_park_fid, dpep.field_group,
             coalesce(dpep.confidence, -1) AS conf
        FROM public.dog_park_enrichment_provenance dpep
       WHERE dpep.dog_park_fid IS NOT NULL
       GROUP BY dpep.dog_park_fid, dpep.field_group, conf
      HAVING count(*) >= 2
    )
    SELECT DISTINCT dog_park_fid FROM ties
  LOOP
    PERFORM public.compute_dog_park_field_consensus(v_fid);
    v_count := v_count + 1;
  END LOOP;
  RAISE NOTICE 'dog-park refire: % fids touched', v_count;
END
$refire_dp$;

NOTIFY pgrst, 'reload schema';
