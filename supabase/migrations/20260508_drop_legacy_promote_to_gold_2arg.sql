-- 20260508_drop_legacy_promote_to_gold_2arg.sql
--
-- The 2-arg promote_to_gold(bigint[], boolean) was superseded by the 3-arg
-- (bigint[], boolean, boolean) added in 20260508_split_init_from_publish.sql.
-- Both coexisted, which made callers like run_pipeline_for_state ambiguous
-- ("function promote_to_gold(bigint[], boolean) is not unique").
-- Drop the legacy 2-arg; the 3-arg's p_publish defaults to true so 2-arg
-- callers continue to work without modification.

begin;

drop function if exists public.promote_to_gold(bigint[], boolean);

commit;
