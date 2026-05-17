-- Switch the beach_policy_source trigger from FOR EACH ROW to
-- FOR EACH STATEMENT with transition tables. Mitigates bulk-INSERT
-- performance hit identified by 2026-05-17 discovery: each per-row
-- invocation queries the entity layer for that fid; on large Wave-style
-- batches (OR Wave-3 sweep would hit this) the per-row promote can
-- materially slow inserts. The statement-level shape deduplicates
-- beach_fids across the batch and calls the promoter once per distinct fid.

DROP TRIGGER IF EXISTS tg_after_change_beach_policy_source
  ON public.beach_policy_source;

DROP FUNCTION IF EXISTS public.tg_after_change_beach_policy_source();

-- Shared promote-loop: take a set of beach_fids and promote each once.
-- Idempotent; safe to re-call. Helper used by all three statement-level
-- triggers below.
CREATE OR REPLACE FUNCTION public._refresh_beaches_from_ps(p_fids bigint[])
RETURNS int
LANGUAGE plpgsql
AS $$
DECLARE
  v_fid bigint;
  v_count int := 0;
BEGIN
  FOREACH v_fid IN ARRAY p_fids LOOP
    IF v_fid IS NOT NULL THEN
      PERFORM public.promote_entity_dogs_to_beach_dog_policy(v_fid);
      v_count := v_count + 1;
    END IF;
  END LOOP;
  RETURN v_count;
END;
$$;

-- INSERT — only NEW TABLE available
CREATE OR REPLACE FUNCTION public.tg_stmt_ins_beach_policy_source()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM public._refresh_beaches_from_ps(
    ARRAY(SELECT DISTINCT beach_fid FROM new_rows WHERE beach_fid IS NOT NULL)
  );
  RETURN NULL;
END;
$$;

CREATE TRIGGER tg_stmt_ins_beach_policy_source
AFTER INSERT ON public.beach_policy_source
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.tg_stmt_ins_beach_policy_source();

-- UPDATE — both NEW and OLD available; union distinct fids (an UPDATE
-- changing beach_fid would otherwise miss the old beach's refresh).
CREATE OR REPLACE FUNCTION public.tg_stmt_upd_beach_policy_source()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM public._refresh_beaches_from_ps(
    ARRAY(
      SELECT DISTINCT beach_fid FROM (
        SELECT beach_fid FROM new_rows
        UNION
        SELECT beach_fid FROM old_rows
      ) t WHERE beach_fid IS NOT NULL
    )
  );
  RETURN NULL;
END;
$$;

CREATE TRIGGER tg_stmt_upd_beach_policy_source
AFTER UPDATE ON public.beach_policy_source
REFERENCING NEW TABLE AS new_rows OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.tg_stmt_upd_beach_policy_source();

-- DELETE — only OLD TABLE available
CREATE OR REPLACE FUNCTION public.tg_stmt_del_beach_policy_source()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  PERFORM public._refresh_beaches_from_ps(
    ARRAY(SELECT DISTINCT beach_fid FROM old_rows WHERE beach_fid IS NOT NULL)
  );
  RETURN NULL;
END;
$$;

CREATE TRIGGER tg_stmt_del_beach_policy_source
AFTER DELETE ON public.beach_policy_source
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION public.tg_stmt_del_beach_policy_source();

COMMENT ON FUNCTION public._refresh_beaches_from_ps(bigint[]) IS
  'Helper: promote_entity_dogs_to_beach_dog_policy() in a loop for a set of '
  'beach fids. Used by the three statement-level triggers on beach_policy_source.';

COMMENT ON TRIGGER tg_stmt_ins_beach_policy_source ON public.beach_policy_source IS
  'Statement-level promote on INSERT. Deduplicates beach_fids across the batch '
  'so a bulk INSERT calls the promoter once per distinct fid.';

COMMENT ON TRIGGER tg_stmt_upd_beach_policy_source ON public.beach_policy_source IS
  'Statement-level promote on UPDATE. Unions NEW and OLD beach_fids so an UPDATE '
  'that changes beach_fid still refreshes both the old and new beach.';

COMMENT ON TRIGGER tg_stmt_del_beach_policy_source ON public.beach_policy_source IS
  'Statement-level promote on DELETE. Refreshes the orphaned beaches so removed '
  'ps rows stop influencing their dog_policy.';
