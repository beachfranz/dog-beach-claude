-- 20260613m_revert_subtype_aware_resolver.sql
--
-- Revert 20260613l. The subtype-aware tiebreak had a real flaw:
-- it ranked broader codified subtypes (municipal_code, state_regulation,
-- federal_regulation) ABOVE non-codified per-beach extractors like
-- park_url and per_beach_offleash_v2, which are typically more
-- specific in practice.
--
-- Surfaced on Dana Point Headlands (fid 5968): the city municipal
-- code (codified ps_48) says "dogs allowed in city" generically, but
-- the beach's own page (park_url) says "no dogs on the beach itself."
-- The subtype priority promoted the broader codified over the
-- more-specific park_url, flipping the canonical from no → yes
-- incorrectly.
--
-- Revert plan:
--   1. Self-patch _resolve_field_group_gold to restore the original
--      ORDER BY (priority, confidence DESC, id ASC).
--   2. Drop _codified_subtype_priority (no longer used).
--
-- Re-resolve runs as a separate query post-COMMIT (not in this
-- migration) to dodge the PL/pgSQL plan caching that bit the apply
-- in 20260613l.
--
-- A proper specificity-aware resolver (universal across all sources,
-- not just codified subtypes) is pinned as a follow-up.

BEGIN;

-- ─── 1. Patch _resolve_field_group_gold back to original ─────────────
DO $patch$
DECLARE
  v_src text;
  v_old text := E'order by public._gold_field_group_source_priority(e.field_group, e.source) asc,\n                 e.confidence desc nulls last,\n                 public._codified_subtype_priority(e.source) asc,\n                 e.id asc';
  v_new text := E'order by public._gold_field_group_source_priority(e.field_group, e.source) asc,\n                 e.confidence desc nulls last,\n                 e.id asc';
BEGIN
  v_src := pg_get_functiondef('public._resolve_field_group_gold'::regproc);

  IF position(v_old IN v_src) = 0 THEN
    RAISE EXCEPTION '_resolve_field_group_gold: 20260613l ORDER BY anchor missing — already reverted?';
  END IF;

  v_src := replace(v_src, v_old, v_new);
  EXECUTE v_src;
END
$patch$;

-- ─── 2. Drop unused helper ───────────────────────────────────────────
DROP FUNCTION IF EXISTS public._codified_subtype_priority(text);

COMMIT;

NOTIFY pgrst, 'reload schema';
