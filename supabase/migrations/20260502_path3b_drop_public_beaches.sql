-- Path 3b-3 (final final): drop public.beaches.
--
-- After all the path-3 migrations, every reader has moved off this
-- table. Verified pre-flight 2026-05-02 with smoke tests against all
-- 5 edge functions + the find_beaches RPC.
--
-- One soft dependency we still need to clean: the FK constraint
-- public.beaches.arena_group_id → beaches_gold.fid that we added in
-- path 3b-3.1 to enable PostgREST embedding. That just disappears
-- with the table.

begin;

drop table if exists public.beaches cascade;

commit;

-- Notify PostgREST so the schema cache reloads + the just-deployed
-- functions stop seeing 'beaches' as a queryable resource.
notify pgrst, 'reload schema';
