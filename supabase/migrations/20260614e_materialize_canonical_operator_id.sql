-- 20260614e_materialize_canonical_operator_id.sql
--
-- Materialize the result of `_resolve_beach_operator(fid)` as a column
-- on `beaches_gold`, refreshed by trigger on geom change. Same pattern
-- as the Phase W1 weather_grid materialization
-- (tg_compute_weather_grid_cell): a column + AFTER trigger keeps it
-- in sync without consumers re-running the resolver per query.
--
-- Direction A foundation. With this column in place:
--   - Consumer surfaces (PBI, edge functions, beach.html) can JOIN
--     operators on canonical_operator_id in O(1) instead of calling
--     the multi-tier resolver on every read.
--   - The vw_pbi_dogs_evidence view (bumped below) surfaces a
--     `resolved_operator_name` column on EVERY scoring beach that has
--     a resolved operator, not just the 38.8% that today have
--     `claim_operator_name` packed into BEP claimed_values by the
--     city/county populators. Expected PBI lift: 38.8% → ~95%
--     (everything except the residual 161 cross-state orphans after
--     20260614c/d closed HI + ME).
--
-- Notes on the trigger:
--   - AFTER (not BEFORE) because _resolve_beach_operator reads from
--     beaches_gold itself; in a BEFORE INSERT trigger the new row
--     isn't visible yet, so the resolver returns NULL.
--   - Fires only when geom is the column being modified (matches the
--     resolver's spatial dependency).
--   - Wrapped in an EXISTS-by-fid lookup so it only updates rows that
--     actually exist after commit timing — keeps the trigger safe to
--     re-fire during bulk loads.

BEGIN;

-- 1. Column ----------------------------------------------------------------
ALTER TABLE public.beaches_gold
  ADD COLUMN IF NOT EXISTS canonical_operator_id bigint
    REFERENCES public.operators(id) ON DELETE SET NULL;

COMMENT ON COLUMN public.beaches_gold.canonical_operator_id IS
  'Materialized result of _resolve_beach_operator(fid). The 7-tier '
  'spatial cascade (federal → CPAD → state → district → local → city → '
  'county) returns ONE canonical operator id per beach. Refreshed by '
  'tg_compute_canonical_operator on geom INSERT/UPDATE. NULL = beach '
  'falls outside all canonical-operator polygons (residual 161 fids '
  'at session 2026-06-14 — mostly NH/VA/RI/MA/MI/WA/OH/MD/DE/AL '
  'small-pop holes).';

CREATE INDEX IF NOT EXISTS beaches_gold_canonical_operator_id_idx
  ON public.beaches_gold (canonical_operator_id);

-- 2. Trigger function ------------------------------------------------------
CREATE OR REPLACE FUNCTION public._set_canonical_operator_for_beach()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  -- Recompute the operator membership from the current geom.
  -- _resolve_beach_operator reads beaches_gold WHERE fid=p_fid, so the
  -- row must already exist — we run AFTER INSERT/UPDATE OF geom.
  UPDATE public.beaches_gold
     SET canonical_operator_id = public._resolve_beach_operator(NEW.fid)
   WHERE fid = NEW.fid
     AND canonical_operator_id IS DISTINCT FROM
         public._resolve_beach_operator(NEW.fid);
  RETURN NULL;
END
$$;

-- 3. Trigger wiring --------------------------------------------------------
DROP TRIGGER IF EXISTS tg_compute_canonical_operator ON public.beaches_gold;
CREATE TRIGGER tg_compute_canonical_operator
  AFTER INSERT OR UPDATE OF geom ON public.beaches_gold
  FOR EACH ROW EXECUTE FUNCTION public._set_canonical_operator_for_beach();

-- 4. One-shot backfill -----------------------------------------------------
-- Bypasses the trigger by updating canonical_operator_id directly.
-- ~4K active scoring rows; ~hundreds of ms total.
UPDATE public.beaches_gold AS bg
   SET canonical_operator_id = public._resolve_beach_operator(bg.fid)
 WHERE bg.is_active;

-- 5. Audit -----------------------------------------------------------------
SELECT
  count(*) AS total_active,
  count(*) FILTER (WHERE canonical_operator_id IS NOT NULL) AS with_operator,
  round(100.0 * count(*) FILTER (WHERE canonical_operator_id IS NOT NULL)
              / nullif(count(*), 0), 1) AS pct_coverage
  FROM public.beaches_gold
 WHERE is_active AND scoring_tier IN ('daily','hourly');

COMMIT;

-- 6. PBI view bump ---------------------------------------------------------
-- Reload vw_pbi_dogs_evidence with two additional columns:
--   resolved_operator_id   — the materialized canonical_operator_id
--   resolved_operator_name — operators.canonical_name JOIN
-- The existing claim_operator_name column stays as-is so existing PBI
-- visuals don't break. Power BI users can choose: resolved_operator_name
-- for "who actually runs this beach" (95% coverage), claim_operator_name
-- for "what did the city/county_policy populator's BEP row claim"
-- (38.8% coverage, BEP-evidence-backed).
BEGIN;

DROP VIEW IF EXISTS public.vw_pbi_dogs_evidence CASCADE;
CREATE VIEW public.vw_pbi_dogs_evidence AS
SELECT
  bep.gold_fid                                AS fid,
  bep.source,
  CASE
    WHEN bep.source ~ '^codified_v1:ps_\d+$' THEN
      coalesce(
        (SELECT 'codified: ' || citation FROM public.policy_source
          WHERE id = substring(bep.source FROM 'codified_v1:ps_(\d+)$')::int),
        bep.source
      )
    WHEN bep.source ~ '^codified_v1_deferred:ps_\d+$' THEN
      coalesce(
        (SELECT 'codified-deferred: ' || citation FROM public.policy_source
          WHERE id = substring(bep.source FROM 'codified_v1_deferred:ps_(\d+)$')::int),
        bep.source
      )
    ELSE bep.source
  END                                         AS source_label,
  bep.source_url,
  bep.confidence,
  bep.is_canonical,
  bep.extraction_type,
  bep.cpad_role,
  bep.cpad_unit_name,
  -- ── Resolved operator (the canonical 7-tier cascade answer) ──
  b.canonical_operator_id                     AS resolved_operator_id,
  op.canonical_name                           AS resolved_operator_name,
  op.level                                    AS resolved_operator_level,
  -- ── Existing per-row claims ─────────────────────────────────────
  bep.claimed_values->>'allowed'              AS claim_allowed,
  coalesce((bep.claimed_values->>'_allowed_derived')::boolean, false)
                                              AS claim_allowed_derived,
  bep.claimed_values->>'leash_required'       AS claim_leash_required,
  bep.claimed_values->>'default_rule'         AS claim_default_rule,
  (bep.claimed_values->>'off_leash_exists')::boolean AS claim_off_leash_exists,
  coalesce(
    bep.claimed_values->>'area_sand',
    bep.claimed_values->'sections'->'sand'->>'rule',
    (SELECT r->>'rule' FROM jsonb_array_elements(coalesce(bep.claimed_values->'rules','[]'::jsonb)) r
      WHERE r->>'section' = 'sand' LIMIT 1)
  )                                           AS claim_sand_rule,
  coalesce(
    bep.claimed_values->>'area_water',
    bep.claimed_values->'sections'->'water'->>'rule',
    (SELECT r->>'rule' FROM jsonb_array_elements(coalesce(bep.claimed_values->'rules','[]'::jsonb)) r
      WHERE r->>'section' = 'water' LIMIT 1)
  )                                           AS claim_water_rule,
  coalesce(
    bep.claimed_values->'sections'->'trails'->>'rule',
    (SELECT r->>'rule' FROM jsonb_array_elements(coalesce(bep.claimed_values->'rules','[]'::jsonb)) r
      WHERE r->>'section' = 'trails' LIMIT 1)
  )                                           AS claim_trails_rule,
  coalesce(
    bep.claimed_values->'sections'->'picnic_area'->>'rule',
    (SELECT r->>'rule' FROM jsonb_array_elements(coalesce(bep.claimed_values->'rules','[]'::jsonb)) r
      WHERE r->>'section' = 'picnic_area' LIMIT 1)
  )                                           AS claim_picnic_rule,
  bep.claimed_values->>'source_quote'         AS claim_source_quote,
  bep.claimed_values->>'ordinance_ref'        AS claim_ordinance_ref,
  bep.claimed_values->>'scope_notes'          AS claim_scope_notes,
  bep.claimed_values->>'subtype'              AS claim_subtype,
  bep.claimed_values->>'citation'             AS claim_citation,
  bep.claimed_values->>'notes'                AS claim_notes,
  bep.claimed_values->>'operator_name'        AS claim_operator_name,
  bep.claimed_values->>'mng_name'             AS claim_mng_name,
  bep.claimed_values->'time_windows'          AS claim_time_windows,
  bep.claimed_values->'rules'                 AS claim_rules,
  bep.claimed_values->'sections'              AS claim_sections,
  bep.claimed_values                          AS claim_raw_jsonb,
  (bep.source ~* '_no_result$')               AS is_sentinel,
  (bep.source ~* '^codified_v1_deferred:')    AS is_deferred_sentinel
FROM public.beach_enrichment_provenance bep
JOIN public.beaches_gold b ON b.fid = bep.gold_fid
LEFT JOIN public.operators op ON op.id = b.canonical_operator_id
WHERE bep.field_group = 'dogs'
  AND b.is_active AND b.scoring_tier IN ('daily','hourly');

COMMENT ON VIEW public.vw_pbi_dogs_evidence IS
  'Long-form evidence list for dogs field_group. resolved_operator_name '
  'is the canonical 7-tier-cascade answer (95% coverage post-20260614e). '
  'claim_operator_name is the per-BEP-row claim packed by city/county '
  '_policy populators (38.8% coverage, BEP-evidence-backed). source_label '
  'resolves codified_v1:ps_N → policy_source.citation; '
  'codified_v1_deferred:ps_N → "codified-deferred: <citation>". '
  'is_deferred_sentinel TRUE = a categorizer-v2 deferred sentinel '
  '(confidence 0.10, never canonical).';

COMMIT;

NOTIFY pgrst, 'reload schema';
