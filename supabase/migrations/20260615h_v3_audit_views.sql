-- v3 audit views — verification + ongoing weight tuning.
--
-- Phase F of the v3 refactor (plan delegated-splashing-lynx.md). Read-only,
-- view-shaped audit surfaces over the new v3 path. Pre-cutover sign-off
-- runs through these; post-cutover Franz tunes weights using
-- vw_v3_advisory_contribution as the truth source.
--
-- Three views:
--   vw_v3_v2_delta             — per-hour v2 vs v3 score + status comparison
--   vw_v3_advisory_contribution — per event_type contribution drill
--   vw_v3_audit_coverage        — % of scoreable entities with active advisories
--
-- All three UNION beach + dog_park into one row stream with entity_type column.

BEGIN;

-- ─── vw_v3_v2_delta ───────────────────────────────────────────────────
-- Per (entity_type, fid, local_date, local_hour): v2 score, v3 score,
-- delta, and derived status bands for both. Use for V1/V2 plan verifications
-- ("status flip count", "score distribution histogram").
CREATE OR REPLACE VIEW public.vw_v3_v2_delta AS
SELECT
  'beach'::text                                AS entity_type,
  h.arena_group_id                             AS fid,
  h.local_date,
  h.local_hour,
  h.hour_score_v2,
  h.hour_score_v3,
  (h.hour_score_v3 - h.hour_score_v2)          AS delta,
  public._v2_status_from_score(h.hour_score_v2)         AS status_v2,
  public._v2_status_from_score(h.hour_score_v3::int)    AS status_v3
FROM public.beach_day_hourly_scores h
WHERE h.hour_score_v2 IS NOT NULL AND h.hour_score_v3 IS NOT NULL
UNION ALL
SELECT
  'dog_park'::text,
  h.dog_park_fid,
  h.local_date,
  h.local_hour,
  h.hour_score_v2,
  h.hour_score_v3,
  (h.hour_score_v3 - h.hour_score_v2),
  public._v2_status_from_score(h.hour_score_v2),
  public._v2_status_from_score(h.hour_score_v3::int)
FROM public.dog_park_day_hourly_scores h
WHERE h.hour_score_v2 IS NOT NULL AND h.hour_score_v3 IS NOT NULL;

COMMENT ON VIEW public.vw_v3_v2_delta IS
  'Per-hour v2 vs v3 comparison across beach + dog_park. Use for plan '
  'V1 (histogram) + V2 (status flip count) verification. Sign-off '
  'expects most rows on the diagonal with v3 spread for high-penalty '
  'beaches.';


-- ─── vw_v3_advisory_contribution ──────────────────────────────────────
-- Per (entity_type, event_type, severity, current activity): how many
-- rows are firing right now, how many distinct entities they cover,
-- per-event contribution (base × weight), and total contribution. Use
-- for weight tuning — if rip_current shows total_contribution
-- dominating bacteria_risk, weights are off.
--
-- Rows with weight=0 (unseeded event_types, like NWS raw alerts the
-- whitelist ignores) show as visible-but-zero — they're contributing
-- nothing, but you can see the firing volume so missing seeds are
-- obvious.
CREATE OR REPLACE VIEW public.vw_v3_advisory_contribution AS
WITH beach_active AS (
  SELECT ba.event_type, ba.severity, ba.beach_fid AS fid
    FROM public.beach_advisory ba
   WHERE now() BETWEEN ba.valid_from AND ba.valid_to
),
dog_park_active AS (
  SELECT dpa.event_type, dpa.severity, dpa.dog_park_fid AS fid
    FROM public.dog_park_advisory dpa
   WHERE now() BETWEEN dpa.valid_from AND dpa.valid_to
)
SELECT
  'beach'::text                                              AS entity_type,
  a.event_type,
  a.severity,
  count(*)                                                   AS active_rows,
  count(DISTINCT a.fid)                                      AS distinct_entities,
  public._advisory_base_for_severity(a.severity)             AS base,
  COALESCE(asc_row.weight, 0)                                AS weight,
  COALESCE(asc_row.enabled, false)                           AS enabled,
  (public._advisory_base_for_severity(a.severity)
    * COALESCE(asc_row.weight, 0))                           AS per_event_contribution,
  (count(*) * public._advisory_base_for_severity(a.severity)
    * COALESCE(asc_row.weight, 0))                           AS total_contribution
FROM beach_active a
LEFT JOIN public.advisory_score_config asc_row
  ON asc_row.entity_type = 'beach' AND asc_row.event_type = a.event_type
GROUP BY a.event_type, a.severity, asc_row.weight, asc_row.enabled
UNION ALL
SELECT
  'dog_park'::text,
  a.event_type,
  a.severity,
  count(*),
  count(DISTINCT a.fid),
  public._advisory_base_for_severity(a.severity),
  COALESCE(asc_row.weight, 0),
  COALESCE(asc_row.enabled, false),
  (public._advisory_base_for_severity(a.severity) * COALESCE(asc_row.weight, 0)),
  (count(*) * public._advisory_base_for_severity(a.severity) * COALESCE(asc_row.weight, 0))
FROM dog_park_active a
LEFT JOIN public.advisory_score_config asc_row
  ON asc_row.entity_type = 'dog_park' AND asc_row.event_type = a.event_type
GROUP BY a.event_type, a.severity, asc_row.weight, asc_row.enabled;

COMMENT ON VIEW public.vw_v3_advisory_contribution IS
  'Per (entity_type, event_type, severity) contribution audit. Tune '
  'weights against this view: high total_contribution + tiny '
  'distinct_entities = over-weighted; high distinct_entities + tiny '
  'total_contribution = under-weighted. Rows where enabled=false or '
  'weight=0 surface unseeded events the whitelist ignores.';


-- ─── vw_v3_audit_coverage ─────────────────────────────────────────────
-- Per entity_type: scoreable population, count with at least one
-- currently-active advisory, % coverage. Sanity check that the
-- advisory feed is broad enough to drive v3 differentiation.
CREATE OR REPLACE VIEW public.vw_v3_audit_coverage AS
WITH entities AS (
  SELECT 'beach'::text AS entity_type, fid
    FROM public.beaches_gold
   WHERE is_active AND scoring_tier IN ('daily','hourly')
  UNION ALL
  SELECT 'dog_park', fid
    FROM public.dog_parks_gold
   WHERE is_active AND is_scoreable = true
),
with_advisory AS (
  SELECT 'beach'::text AS entity_type, ba.beach_fid AS fid
    FROM public.beach_advisory ba
   WHERE now() BETWEEN ba.valid_from AND ba.valid_to
     AND EXISTS (SELECT 1 FROM public.advisory_score_config asc_row
                  WHERE asc_row.entity_type='beach'
                    AND asc_row.event_type=ba.event_type
                    AND asc_row.enabled = true)
  UNION ALL
  SELECT 'dog_park', dpa.dog_park_fid
    FROM public.dog_park_advisory dpa
   WHERE now() BETWEEN dpa.valid_from AND dpa.valid_to
     AND EXISTS (SELECT 1 FROM public.advisory_score_config asc_row
                  WHERE asc_row.entity_type='dog_park'
                    AND asc_row.event_type=dpa.event_type
                    AND asc_row.enabled = true)
)
SELECT
  e.entity_type,
  count(DISTINCT e.fid)                                              AS active_entities,
  count(DISTINCT w.fid)                                              AS entities_with_seeded_advisory,
  round(100.0 * count(DISTINCT w.fid)::numeric
        / NULLIF(count(DISTINCT e.fid), 0), 1)                       AS pct_coverage
FROM entities e
LEFT JOIN with_advisory w
  ON w.entity_type = e.entity_type AND w.fid = e.fid
GROUP BY e.entity_type;

COMMENT ON VIEW public.vw_v3_audit_coverage IS
  '% of scoreable entities with at least one seeded+enabled advisory '
  'active right now. If this hovers near 0%, the advisory feed isn''t '
  'driving v3 differentiation and v3 collapses toward v2.';


-- Verification
SELECT '_v3_v2_delta_rows'                AS metric, count(*)::text AS val FROM public.vw_v3_v2_delta
UNION ALL SELECT '_v3_contribution_rows', count(*)::text             FROM public.vw_v3_advisory_contribution
UNION ALL SELECT '_v3_coverage_rows',     count(*)::text             FROM public.vw_v3_audit_coverage;

COMMIT;
