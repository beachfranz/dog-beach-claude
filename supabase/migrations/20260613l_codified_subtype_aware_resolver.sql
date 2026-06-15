-- 20260613l_codified_subtype_aware_resolver.sql
--
-- Add subtype-aware tiebreak to the BEP resolver. Today, when multiple
-- codified_v1:ps_N rows exist for the same beach at 0.95 confidence
-- (~50% of beaches with any codified evidence), the resolver tiebreaks
-- by id ASC — arbitrary. That's why Fort Funston shows mixed and Dana
-- Point Headlands shows "yes" even with 3 sources saying no.
--
-- The fix: introduce specificity hierarchy among codified subtypes.
-- Most-specific beats general:
--
--   1  operator_posted_policy           per-beach extraction
--   2  mou                              per-beach legal instrument
--   3  agency_administrative_policy     city/operator policy page
--   4  municipal_code                   city ordinance
--   5  superintendents_compendium       federal unit-specific (NPS/USFS)
--   6  special_district_ordinance       regional district (EBRPD, etc.)
--   7  state_regulation                 OAR / WAC state-park default
--   8  state_statute                    state legislature default
--   9  federal_regulation               CFR federal default
--   50 (other)                          unknown subtype — neutral
--
-- The new tiebreak slots into _resolve_field_group_gold AFTER
-- confidence DESC and BEFORE id ASC. Net behaviour:
--
--   - Within codified-at-0.95 ties: most specific wins
--   - Non-codified rows: helper returns 99 for non-codified pattern,
--     so they tie among themselves on this dimension (id ASC still
--     breaks). No change to their relative ordering.
--   - Other field_groups (governance, park_url, etc.): all hit ELSE
--     branch (99 for non-codified or 50 for unknown subtype). Neutral.

BEGIN;

-- ─── 1. Helper: codified subtype priority (lower = more specific) ────
CREATE OR REPLACE FUNCTION public._codified_subtype_priority(p_source text)
RETURNS integer
LANGUAGE sql STABLE
AS $function$
  SELECT CASE
    WHEN p_source !~ '^codified_v1:ps_\d+$' THEN 99
    ELSE coalesce((
      SELECT CASE ps.subtype
        WHEN 'operator_posted_policy'       THEN 1
        WHEN 'mou'                          THEN 2
        WHEN 'agency_administrative_policy' THEN 3
        WHEN 'municipal_code'               THEN 4
        WHEN 'superintendents_compendium'   THEN 5
        WHEN 'special_district_ordinance'   THEN 6
        WHEN 'state_regulation'             THEN 7
        WHEN 'state_statute'                THEN 8
        WHEN 'federal_regulation'           THEN 9
        ELSE                                     50
      END
      FROM public.policy_source ps
      WHERE ps.id = substring(p_source FROM 'codified_v1:ps_(\d+)$')::int
    ), 50)
  END;
$function$;

-- ─── 2. Self-patch the resolver to include subtype tiebreak ──────────
DO $patch$
DECLARE
  v_src text;
  v_old text := E'order by public._gold_field_group_source_priority(e.field_group, e.source) asc,\n                 e.confidence desc nulls last,\n                 e.id asc';
  v_new text := E'order by public._gold_field_group_source_priority(e.field_group, e.source) asc,\n                 e.confidence desc nulls last,\n                 public._codified_subtype_priority(e.source) asc,\n                 e.id asc';
BEGIN
  v_src := pg_get_functiondef('public._resolve_field_group_gold'::regproc);

  IF position(v_old IN v_src) = 0 THEN
    RAISE EXCEPTION '_resolve_field_group_gold: ORDER BY anchor missing — body drifted';
  END IF;

  v_src := replace(v_src, v_old, v_new);
  EXECUTE v_src;
END
$patch$;

-- ─── 3. Re-resolve dogs for fids that might flip ────────────────────
-- Affected fids = those with >= 2 codified_v1:* BEP rows for dogs.
-- Other fids are unaffected (single codified row → still wins among
-- codifieds; non-codified ties → id ASC still applies).
DO $backfill$
DECLARE
  v_fid bigint;
BEGIN
  FOR v_fid IN
    SELECT gold_fid
      FROM public.beach_enrichment_provenance
     WHERE field_group = 'dogs'
       AND source LIKE 'codified_v1:%'
     GROUP BY gold_fid
    HAVING count(*) >= 2
  LOOP
    PERFORM public._resolve_dogs_gold(v_fid);
    PERFORM public.compute_beach_field_consensus(v_fid);
    PERFORM public.promote_canonical_dogs_to_beach_dog_policy(v_fid, 0.5);
  END LOOP;
END
$backfill$;

COMMIT;

NOTIFY pgrst, 'reload schema';
