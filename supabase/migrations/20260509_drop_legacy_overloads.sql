-- 20260509_drop_legacy_overloads.sql
--
-- Drift cleanup. Tonight's drift scan found two function overloads that
-- the canonical orchestrator can no longer call unambiguously:
--
--   public.promote_to_gold(bigint[], boolean)
--   public.run_pipeline_for_state(text, bigint[])  (and a 3-arg version)
--
-- The 3-arg promote_to_gold (with p_publish DEFAULT true) is the
-- canonical version. The 2-arg form is a stale predecessor; calling
-- with 2 args is now ambiguous because both signatures match.
--
-- Same story for run_pipeline_for_state — multiple overloads grew across
-- tonight's migrations as we added precheck, operators, address_poi,
-- etc. The latest 3-arg form (state, fids, skip_precheck) is canonical.
--
-- All known callers (scripts/promote_to_gold.py, scripts/run_state_pipeline.py,
-- run_pipeline_for_state SQL function itself, tg_arena_auto_promote_to_gold
-- trigger) are updated to use the canonical signatures.

begin;

-- Drop the 2-arg legacy promote_to_gold. Callers passing 2 args will
-- naturally resolve to the 3-arg form (p_publish defaults to true).
drop function if exists public.promote_to_gold(bigint[], boolean);

-- Drop the 2-arg legacy run_pipeline_for_state. The canonical form
-- takes (state, fids, skip_precheck).
drop function if exists public.run_pipeline_for_state(text, bigint[]);

commit;
