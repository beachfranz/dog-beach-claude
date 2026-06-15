-- v3 advisory-driven scoring: schema + base-curve helper.
--
-- Phase A.1 of the v3 refactor (plan delegated-splashing-lynx.md). Today's v3
-- shadow path uses inline negatives (heat_uv_neg / harsh_neg / tide_neg /
-- crowd_neg). Phase C swaps those for an additive penalty pulled from
-- beach_advisory rows weighted by event_type via this table.
--
-- This migration ships the table + base-curve helper only. No rows seeded —
-- Phase B (20260615d) writes the seed after Franz signs off on each
-- event_type's weight one-by-one. Behavior of compute_beach_hourly_v2 is
-- unchanged by this migration.

BEGIN;

CREATE TABLE IF NOT EXISTS public.advisory_score_config (
  entity_type text        NOT NULL,        -- 'beach' | 'dog_park'
  event_type  text        NOT NULL,        -- 'rip_current_risk', 'sand_status', 'bacteria_risk', etc.
  weight      numeric     NOT NULL DEFAULT 1.0,
  category    text,                        -- 'water_hazard' | 'water_quality' | 'thermal' | 'air_safety' | 'comfort' | 'crowd' | 'wmo' | 'static'
  enabled     boolean     NOT NULL DEFAULT true,
  notes       text,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (entity_type, event_type),
  CONSTRAINT advisory_score_config_entity_type_check
    CHECK (entity_type IN ('beach','dog_park')),
  CONSTRAINT advisory_score_config_weight_nonneg
    CHECK (weight >= 0)
);

COMMENT ON TABLE public.advisory_score_config IS
  'Per-(entity_type, event_type) weight for the v3 advisory-driven score. '
  'The advisory penalty for a single (beach, hour) is '
  'SUM(_advisory_base_for_severity(sev) * weight) over active advisories '
  'in the validity window. Unknown event_types default to weight=1.0 in '
  'the helper. Tune weights any time via UPDATE — next bulk run picks up '
  'the change. Seeded by 20260615d after Franz weight review.';

COMMENT ON COLUMN public.advisory_score_config.entity_type IS
  'Which advisory feed this row applies to: beach (beach_advisory) or '
  'dog_park (dog_park_advisory).';

COMMENT ON COLUMN public.advisory_score_config.weight IS
  'Multiplier on _advisory_base_for_severity. 1.0 = nominal severity curve '
  '(severe=8, moderate=4, minor=2). 2.0 doubles the penalty for that event_type '
  '(e.g. bacteria, lightning). 0.5 halves it (e.g. tide, crowd, light rain).';

COMMENT ON COLUMN public.advisory_score_config.enabled IS
  'Soft kill switch. enabled=false zeroes the contribution without losing '
  'the historical weight value. Used for event_types we want to keep '
  'reading at the data layer but not penalize (e.g. beach_erosion_risk).';

-- Base-curve helper. Severity-only — weighting happens in the caller.
CREATE OR REPLACE FUNCTION public._advisory_base_for_severity(p_sev text)
RETURNS numeric LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE p_sev
    WHEN 'extreme'  THEN 10
    WHEN 'severe'   THEN 8
    WHEN 'moderate' THEN 4
    WHEN 'minor'    THEN 2
    ELSE                 0
  END::numeric;
$$;

COMMENT ON FUNCTION public._advisory_base_for_severity(text) IS
  'Severity → base penalty before per-event_type weight. Used by '
  '_advisory_penalty_for_hour + bulk-writer CTE. IMMUTABLE so callers can '
  'inline it in indexed expressions.';

-- Verification
SELECT
  'advisory_score_config_table' AS metric,
  CASE WHEN to_regclass('public.advisory_score_config') IS NOT NULL THEN 'yes' ELSE 'no' END AS val
UNION ALL SELECT
  '_advisory_base_for_severity_fn',
  CASE WHEN EXISTS (
    SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
    WHERE n.nspname='public' AND p.proname='_advisory_base_for_severity'
  ) THEN 'yes' ELSE 'no' END
UNION ALL SELECT
  'rows_seeded',
  (SELECT count(*)::text FROM public.advisory_score_config);

COMMIT;
