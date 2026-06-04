-- Parity audit fix: respect prior curation work.
--
-- The audit shipped earlier today (20260603_orch_dogs_policy_parity_audit.sql)
-- flagged 94 CA beaches as `disagree_curated_yes_cascade_no`. Investigation
-- (see [[dogs-policy-parity-cascade-confidence-gap]]) showed:
--
--   * 64 weak — sources=['operator_default'], conf 1.0 artifact
--   * 21 moderate — cpad + operator agree on no
--   * 9 strong — per-beach exception or cpad_manual
--
-- Then Franz pointed out: all 94 have BOTH `zone_rules IS NOT NULL` AND
-- codified `beach_policy_source` rows. La Jolla Shores (fid 8347) — the
-- canonical example — has full seasonal time-of-day zone_rules from
-- §63.0102 with verbatim cite + URL, plus 9 BPS rows. Every single
-- one of the 94 is in the same shape: ALREADY CURATED, real work
-- already done, the audit just didn't know.
--
-- The bug: audit compares binary dogs_allowed='yes' (meaning "dogs are
-- permitted SOMETIMES per zone_rules") against binary cascade
-- dogs_verdict='no' (meaning "operator's default policy is no"). Both
-- sides collapse past the actual encoded answer that lives in zone_rules.
-- Net: re-derivation of solved problems → whack-a-mole.
--
-- [[never-solve-same-problem-twice]] is the parent rule firing.
-- [[regular-data-quality-audits]] — audits must respect prior work, not
-- ignore it.
--
-- Fix: exclude beaches where prior curation work exists. Two indicators:
--   * zone_rules IS NOT NULL — section/time-aware curation (the gold standard)
--   * source = 'manual_curator' — human has already decided
--
-- Beaches without either signal AND with a cascade disagreement are the
-- actual "fresh problem" set the audit was supposed to surface. First-run
-- after this fix should land at 0 disagreements for CA (all 94 are excluded).
-- Future drift (new beaches added without zone_rules curation, or a
-- manual_curator row that disagrees with itself) will surface organically.

BEGIN;

CREATE OR REPLACE FUNCTION public._orch_w_dogs_policy_parity_audit(p jsonb)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_radius_m            int := COALESCE((p->>'radius_m')::int, 500);
  v_disagree_threshold  int := COALESCE((p->>'disagree_threshold')::int, 0);
  v_disagree_n          int;
  v_curated_yes_no_n    int;
  v_curated_no_yes_n    int;
  v_findings            text;
  v_err                 text;
BEGIN
  WITH verdict_geom AS (
    SELECT 'ubp/' || ubp.fid::text AS origin_key, ubp.geom
      FROM public.us_beach_points ubp
    UNION ALL
    SELECT 'osm/' || osm.osm_type || '/' || osm.osm_id::text AS origin_key, osm.geom
      FROM public.osm_features osm
     WHERE osm.osm_type IS NOT NULL
    UNION ALL
    SELECT 'ccc/' || ccc.objectid::text AS origin_key, ccc.geom
      FROM public.ccc_access_points ccc
  ),
  fid_to_verdict AS (
    SELECT DISTINCT ON (b.fid)
           b.fid,
           bv.dogs_verdict
      FROM public.beaches_gold b
      JOIN verdict_geom vg
        ON ST_DWithin(b.geom::geography, vg.geom::geography, v_radius_m)
      JOIN public.beach_verdicts bv ON bv.origin_key = vg.origin_key
     WHERE b.is_active
       AND b.scoring_tier IN ('hourly','daily')
       AND bv.dogs_verdict IS NOT NULL
     ORDER BY b.fid,
              bv.dogs_verdict_confidence DESC NULLS LAST,
              ST_Distance(b.geom::geography, vg.geom::geography) ASC
  ),
  parity AS (
    SELECT
      b.fid,
      b.state,
      CASE
        WHEN bdp.dogs_allowed IS NULL AND f.dogs_verdict IS NULL                                THEN 'both_null'
        WHEN f.dogs_verdict IS NULL                                                             THEN 'cascade_missing'
        WHEN bdp.dogs_allowed IS NULL                                                           THEN 'curated_missing'
        WHEN bdp.dogs_allowed IN ('yes','mixed','restricted','seasonal') AND f.dogs_verdict = 'yes' THEN 'agree_yes'
        WHEN bdp.dogs_allowed = 'no'  AND f.dogs_verdict = 'no'                                 THEN 'agree_no'
        WHEN bdp.dogs_allowed = 'no'  AND f.dogs_verdict = 'yes'                                THEN 'disagree_curated_no_cascade_yes'
        WHEN bdp.dogs_allowed IN ('yes','mixed','restricted','seasonal') AND f.dogs_verdict = 'no'  THEN 'disagree_curated_yes_cascade_no'
        ELSE 'other'
      END AS parity
      FROM public.beaches_gold b
      LEFT JOIN public.beach_dog_policy bdp ON bdp.arena_group_id = b.fid
      LEFT JOIN fid_to_verdict     f   ON f.fid = b.fid
     WHERE b.is_active AND b.scoring_tier IN ('hourly','daily')
       -- Respect prior curation work: skip beaches with zone_rules or
       -- manual_curator source. Both indicate the binary dogs_allowed
       -- field is a summary of richer encoded answers, not the curation
       -- itself. Comparing binary vs binary on these beaches re-derives
       -- solved problems and produces noise.
       AND (bdp.zone_rules IS NULL AND COALESCE(bdp.source, '') <> 'manual_curator')
  ),
  per_state AS (
    SELECT state,
           count(*) FILTER (WHERE parity = 'disagree_curated_yes_cascade_no') AS curated_yes_cascade_no,
           count(*) FILTER (WHERE parity = 'disagree_curated_no_cascade_yes') AS curated_no_cascade_yes
      FROM parity
     GROUP BY state
  )
  SELECT
    (SELECT count(*) FILTER (WHERE parity LIKE 'disagree_%') FROM parity),
    (SELECT count(*) FILTER (WHERE parity = 'disagree_curated_yes_cascade_no') FROM parity),
    (SELECT count(*) FILTER (WHERE parity = 'disagree_curated_no_cascade_yes') FROM parity),
    string_agg(
      format('%s: %s curated_yes_cascade_no, %s curated_no_cascade_yes',
             state, curated_yes_cascade_no, curated_no_cascade_yes),
      '; '
      ORDER BY curated_yes_cascade_no DESC, curated_no_cascade_yes DESC)
    INTO v_disagree_n, v_curated_yes_no_n, v_curated_no_yes_n, v_findings
    FROM (
      SELECT *
        FROM per_state
       WHERE curated_yes_cascade_no > 0 OR curated_no_cascade_yes > 0
       ORDER BY curated_yes_cascade_no DESC, curated_no_cascade_yes DESC
       LIMIT 8
    ) s;

  IF v_disagree_n > v_disagree_threshold THEN
    v_err := format(
      'cascade vs curated dogs_allowed disagreement (uncurated beaches only): %s fids total (%s curated_yes_cascade_no; %s curated_no_cascade_yes). Top states: [%s]',
      v_disagree_n,
      COALESCE(v_curated_yes_no_n, 0),
      COALESCE(v_curated_no_yes_n, 0),
      COALESCE(v_findings, '—')
    );
    UPDATE public.orch_jobs
       SET last_failed_at = now(),
           last_error = v_err,
           updated_at = now()
     WHERE job_name = 'dogs_policy_parity_audit';
  ELSE
    UPDATE public.orch_jobs
       SET last_error = NULL,
           updated_at = now()
     WHERE job_name = 'dogs_policy_parity_audit'
       AND last_error LIKE 'cascade vs curated dogs_allowed disagreement%';
  END IF;
END;
$$;

-- Refresh state immediately.
SELECT public._orch_w_dogs_policy_parity_audit('{}'::jsonb);

COMMIT;
