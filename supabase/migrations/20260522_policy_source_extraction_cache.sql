-- 20260522_policy_source_extraction_cache.sql
--
-- Generic LLM-extraction cache for policy_source-driven extractors.
-- Cache key = prompt_sha + model — keyed so prompt edits auto-invalidate.
--
-- Per task #118 (canonical operating hours). Same shape will serve other
-- per-ps extractors (operator-posted-hours, sub-area-scope, etc.).
--
-- Usage from Python:
--   key = f"oh:{MODEL}:{PROMPT_SHA[:8]}"
--   cache_lookup(ps_id) → result_json or None
--   cache_write(ps_id, result_json)

BEGIN;

CREATE TABLE IF NOT EXISTS public.policy_source_extraction_cache (
  id               bigserial PRIMARY KEY,
  policy_source_id bigint    NOT NULL REFERENCES public.policy_source(id) ON DELETE CASCADE,
  cache_key        text      NOT NULL,
  result_json      jsonb     NOT NULL,
  cached_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (policy_source_id, cache_key)
);

CREATE INDEX IF NOT EXISTS policy_source_extraction_cache_key_idx
  ON public.policy_source_extraction_cache (cache_key);

GRANT SELECT, INSERT ON public.policy_source_extraction_cache
  TO anon, authenticated, service_role;
GRANT USAGE, SELECT ON SEQUENCE public.policy_source_extraction_cache_id_seq
  TO anon, authenticated, service_role;

COMMENT ON TABLE public.policy_source_extraction_cache IS
  'Cache for per-ps LLM extractions (operating hours, sub-area scope, etc.). Key = prompt_sha + model so prompt edits naturally invalidate.';

COMMIT;
