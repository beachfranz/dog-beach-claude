-- v3 advisory-driven scoring: penalty helpers.
--
-- Phase A.2 of the v3 refactor (plan delegated-splashing-lynx.md).
--
-- Provides two helpers used by the singleton compute path
-- (compute_beach_hourly_v2 / compute_dog_park_hourly_v2, called by
-- get-beach-now / get-dog-park-now). The bulk writer
-- (apply_v2_best_window_to_beach_recommendations_bulk) inlines the same
-- math as a CTE for speed — see Phase C migration.
--
-- _advisory_penalty_total(entity_type, fid, hour_ts) → numeric
--   Sum of base × weight over active advisories at hour_ts.
--   Returns 0 when no advisories match. Used directly in the v3 score
--   expression: 50 + amenities + dog_access + weather_pos
--                   - _advisory_penalty_total(...) + v_drive
--
-- _advisory_penalty_for_hour(entity_type, fid, hour_ts) → jsonb
--   Same total, plus per-event components for drill/debug. Shape:
--     { "total": 12.0,
--       "components": [
--         { "event_type": "rip_current_risk", "severity": "severe",
--           "base": 8, "weight": 1.5, "contribution": 12.0 },
--         ...
--       ] }
--   Empty components array when no advisories are active.

BEGIN;

CREATE OR REPLACE FUNCTION public._advisory_penalty_total(
  p_entity_type text,
  p_fid         bigint,
  p_hour_ts     timestamptz
) RETURNS numeric LANGUAGE sql STABLE AS $$
  -- Beach branch
  SELECT COALESCE(SUM(
    public._advisory_base_for_severity(ba.severity)
      * COALESCE(asc_row.weight, 1.0)
  ), 0)::numeric
    FROM public.beach_advisory ba
    LEFT JOIN public.advisory_score_config asc_row
      ON asc_row.entity_type = 'beach'
     AND asc_row.event_type  = ba.event_type
     AND asc_row.enabled     = true
   WHERE p_entity_type = 'beach'
     AND ba.beach_fid  = p_fid
     AND p_hour_ts BETWEEN ba.valid_from AND ba.valid_to
     AND COALESCE(asc_row.enabled, true) = true
  UNION ALL
  -- Dog-park branch
  SELECT COALESCE(SUM(
    public._advisory_base_for_severity(dpa.severity)
      * COALESCE(asc_row.weight, 1.0)
  ), 0)::numeric
    FROM public.dog_park_advisory dpa
    LEFT JOIN public.advisory_score_config asc_row
      ON asc_row.entity_type = 'dog_park'
     AND asc_row.event_type  = dpa.event_type
     AND asc_row.enabled     = true
   WHERE p_entity_type   = 'dog_park'
     AND dpa.dog_park_fid = p_fid
     AND p_hour_ts BETWEEN dpa.valid_from AND dpa.valid_to
     AND COALESCE(asc_row.enabled, true) = true
  LIMIT 1;
$$;

COMMENT ON FUNCTION public._advisory_penalty_total(text, bigint, timestamptz) IS
  'Sum of (base × weight) over advisories active at p_hour_ts for the '
  '(entity_type, fid) pair. Unknown event_types weight=1.0; disabled '
  'rows contribute 0. Used by compute_*_hourly_v2 singleton paths; bulk '
  'writer inlines the same math as a CTE.';

CREATE OR REPLACE FUNCTION public._advisory_penalty_for_hour(
  p_entity_type text,
  p_fid         bigint,
  p_hour_ts     timestamptz
) RETURNS jsonb LANGUAGE sql STABLE AS $$
  WITH active AS (
    SELECT ba.event_type, ba.severity,
           public._advisory_base_for_severity(ba.severity) AS base,
           COALESCE(asc_row.weight, 1.0)                   AS weight,
           COALESCE(asc_row.enabled, true)                 AS enabled
      FROM public.beach_advisory ba
      LEFT JOIN public.advisory_score_config asc_row
        ON asc_row.entity_type = 'beach'
       AND asc_row.event_type  = ba.event_type
     WHERE p_entity_type = 'beach'
       AND ba.beach_fid  = p_fid
       AND p_hour_ts BETWEEN ba.valid_from AND ba.valid_to
    UNION ALL
    SELECT dpa.event_type, dpa.severity,
           public._advisory_base_for_severity(dpa.severity) AS base,
           COALESCE(asc_row.weight, 1.0)                    AS weight,
           COALESCE(asc_row.enabled, true)                  AS enabled
      FROM public.dog_park_advisory dpa
      LEFT JOIN public.advisory_score_config asc_row
        ON asc_row.entity_type = 'dog_park'
       AND asc_row.event_type  = dpa.event_type
     WHERE p_entity_type   = 'dog_park'
       AND dpa.dog_park_fid = p_fid
       AND p_hour_ts BETWEEN dpa.valid_from AND dpa.valid_to
  ),
  scored AS (
    SELECT event_type, severity, base, weight,
           CASE WHEN enabled THEN base * weight ELSE 0 END AS contribution
      FROM active
  )
  SELECT jsonb_build_object(
    'total',      COALESCE(SUM(contribution), 0),
    'components', COALESCE(jsonb_agg(
      jsonb_build_object(
        'event_type',   event_type,
        'severity',     severity,
        'base',         base,
        'weight',       weight,
        'contribution', contribution
      ) ORDER BY contribution DESC, event_type
    ) FILTER (WHERE contribution > 0), '[]'::jsonb)
  )
    FROM scored;
$$;

COMMENT ON FUNCTION public._advisory_penalty_for_hour(text, bigint, timestamptz) IS
  'Drill version of _advisory_penalty_total. Returns jsonb '
  '{total, components[{event_type, severity, base, weight, contribution}]}. '
  'Used for explainability JSON in compute_*_hourly_v2 singleton paths and '
  'by future audit views. Components filtered to contribution>0 so disabled '
  'rows don''t clutter.';

-- Verification
SELECT
  '_advisory_penalty_total_fn' AS metric,
  CASE WHEN EXISTS (
    SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
    WHERE n.nspname='public' AND p.proname='_advisory_penalty_total'
  ) THEN 'yes' ELSE 'no' END AS val
UNION ALL SELECT
  '_advisory_penalty_for_hour_fn',
  CASE WHEN EXISTS (
    SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
    WHERE n.nspname='public' AND p.proname='_advisory_penalty_for_hour'
  ) THEN 'yes' ELSE 'no' END
UNION ALL SELECT
  'smoke_total_unseeded_beach',
  -- With 0 rows in advisory_score_config, weights default to 1.0 →
  -- penalty for any beach equals sum of base values of its active advisories.
  -- For a beach with no advisories, must be 0.
  public._advisory_penalty_total('beach',
    (SELECT fid FROM public.beaches_gold
       WHERE scoring_tier IN ('daily','hourly') ORDER BY fid LIMIT 1),
    now()
  )::text
UNION ALL SELECT
  'smoke_drill_beach_jsonb_shape',
  (public._advisory_penalty_for_hour('beach',
    (SELECT fid FROM public.beaches_gold
       WHERE scoring_tier IN ('daily','hourly') ORDER BY fid LIMIT 1),
    now()
  ) ? 'total')::text;

COMMIT;
