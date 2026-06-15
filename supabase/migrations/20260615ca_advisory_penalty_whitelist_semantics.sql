-- v3 advisory penalty helpers: whitelist semantics.
--
-- Tightens 20260615c. The helpers shipped earlier today used LEFT JOIN
-- + COALESCE(weight, 1.0), meaning any never-seeded event_type silently
-- contributes at base × 1.0. The DB has 7 NWS raw event_types beyond
-- the 44 weights Franz signed off on (Beach Hazards Statement, Coastal
-- Flood Advisory/Statement, Extreme Heat Warning, Heat Advisory, High
-- Surf Warning, strong_drift) plus future emitters we don't yet know
-- about. Default-1.0 mode would penalize all of those without explicit
-- approval.
--
-- Whitelist mode: INNER JOIN advisory_score_config. Only event_types
-- with a row (and enabled=true) contribute. New emitters are silent
-- until INSERTed. Audit views can still surface rows-without-config so
-- a missing weight is visible operationally.
--
-- Ships before 20260615d so the seed rows are authoritative the moment
-- they land. Behavior change is zero today (advisory_score_config is
-- still empty → no advisories contribute regardless of mode).

BEGIN;

CREATE OR REPLACE FUNCTION public._advisory_penalty_total(
  p_entity_type text,
  p_fid         bigint,
  p_hour_ts     timestamptz
) RETURNS numeric LANGUAGE sql STABLE AS $$
  SELECT COALESCE(SUM(
    public._advisory_base_for_severity(ba.severity) * asc_row.weight
  ), 0)::numeric
    FROM public.beach_advisory ba
    JOIN public.advisory_score_config asc_row
      ON asc_row.entity_type = 'beach'
     AND asc_row.event_type  = ba.event_type
     AND asc_row.enabled     = true
   WHERE p_entity_type = 'beach'
     AND ba.beach_fid  = p_fid
     AND p_hour_ts BETWEEN ba.valid_from AND ba.valid_to
  UNION ALL
  SELECT COALESCE(SUM(
    public._advisory_base_for_severity(dpa.severity) * asc_row.weight
  ), 0)::numeric
    FROM public.dog_park_advisory dpa
    JOIN public.advisory_score_config asc_row
      ON asc_row.entity_type = 'dog_park'
     AND asc_row.event_type  = dpa.event_type
     AND asc_row.enabled     = true
   WHERE p_entity_type   = 'dog_park'
     AND dpa.dog_park_fid = p_fid
     AND p_hour_ts BETWEEN dpa.valid_from AND dpa.valid_to
  LIMIT 1;
$$;

COMMENT ON FUNCTION public._advisory_penalty_total(text, bigint, timestamptz) IS
  'Whitelist mode (20260615ca): only event_types with a seeded + enabled '
  'row in advisory_score_config contribute. Unknown event_types return 0. '
  'Tune individual contributions via UPDATE advisory_score_config; '
  'INSERT a new row to start scoring a new event_type.';

CREATE OR REPLACE FUNCTION public._advisory_penalty_for_hour(
  p_entity_type text,
  p_fid         bigint,
  p_hour_ts     timestamptz
) RETURNS jsonb LANGUAGE sql STABLE AS $$
  WITH active AS (
    SELECT ba.event_type, ba.severity,
           public._advisory_base_for_severity(ba.severity) AS base,
           asc_row.weight                                  AS weight
      FROM public.beach_advisory ba
      JOIN public.advisory_score_config asc_row
        ON asc_row.entity_type = 'beach'
       AND asc_row.event_type  = ba.event_type
       AND asc_row.enabled     = true
     WHERE p_entity_type = 'beach'
       AND ba.beach_fid  = p_fid
       AND p_hour_ts BETWEEN ba.valid_from AND ba.valid_to
    UNION ALL
    SELECT dpa.event_type, dpa.severity,
           public._advisory_base_for_severity(dpa.severity) AS base,
           asc_row.weight                                   AS weight
      FROM public.dog_park_advisory dpa
      JOIN public.advisory_score_config asc_row
        ON asc_row.entity_type = 'dog_park'
       AND asc_row.event_type  = dpa.event_type
       AND asc_row.enabled     = true
     WHERE p_entity_type   = 'dog_park'
       AND dpa.dog_park_fid = p_fid
       AND p_hour_ts BETWEEN dpa.valid_from AND dpa.valid_to
  ),
  scored AS (
    SELECT event_type, severity, base, weight,
           base * weight AS contribution
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
  'Whitelist mode (20260615ca). Drill of _advisory_penalty_total.';

-- Verification: with empty config, both still return 0.
SELECT
  'whitelist_total_zero' AS metric,
  public._advisory_penalty_total('beach',
    (SELECT fid FROM public.beaches_gold
       WHERE scoring_tier IN ('daily','hourly') ORDER BY fid LIMIT 1),
    now()
  )::text AS val
UNION ALL SELECT
  'whitelist_drill_empty_components',
  ((public._advisory_penalty_for_hour('beach',
    (SELECT fid FROM public.beaches_gold
       WHERE scoring_tier IN ('daily','hourly') ORDER BY fid LIMIT 1),
    now()
  ) -> 'components') = '[]'::jsonb)::text;

COMMIT;
