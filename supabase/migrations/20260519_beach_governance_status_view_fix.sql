-- Fix beach_governance_status view: COALESCE across the multiple operator-name
-- keys that different BEP sources use.
--
-- Per Franz 2026-05-19 MVP+ punch-list investigation. The view was checking
-- only `claimed_values->>'name'` for is_attributed, but governance BEP rows
-- use different keys depending on source:
--
--   - CA (cpad, pad_us, polygon_containment_governance_v1):  `name` + `type`
--   - OR/WA (pad_us_unit_governance, county_governance,
--     c1_city_governance):                                    `operator_name` + `operator_type`
--   - park_url_buffer_attribution (some CA):                  `governing_body_name` + `governing_body_type`
--
-- Effect of the broken view:
--   OR  →   0 / 151 beaches attributed (real: 138)
--   WA  →  53 / 419 attributed         (real: ~295)
--   CA  → 530 / 557 attributed         (unchanged — CA mostly uses `name`)
--
-- Single COALESCE fix flips ~380 OR+WA beaches from "unattributed" to
-- "attributed" in count_unattributed_beaches() with no underlying data change.
-- Remaining real gaps after fix: CA 27, OR 13, WA 124 — those are real
-- governance-derivator follow-up via populate_governance_gold(fid).

BEGIN;

CREATE OR REPLACE VIEW public.beach_governance_status AS
SELECT
  g.fid,
  g.state,
  g.is_active,
  (g.scoring_tier = ANY (ARRAY['daily'::text, 'hourly'::text])) AS is_scoring_active,
  COALESCE(g.display_name_override, g.name)                     AS name,
  COALESCE(
    b.claimed_values ->> 'name',
    b.claimed_values ->> 'operator_name',
    b.claimed_values ->> 'governing_body_name'
  )                                                              AS operator_name,
  COALESCE(
    b.claimed_values ->> 'type',
    b.claimed_values ->> 'operator_type',
    b.claimed_values ->> 'governing_body_type'
  )                                                              AS operator_type,
  b.source                                                       AS governance_source,
  b.operator_id,
  (COALESCE(
    b.claimed_values ->> 'name',
    b.claimed_values ->> 'operator_name',
    b.claimed_values ->> 'governing_body_name'
  ) IS NOT NULL)                                                 AS is_attributed
FROM public.beaches_gold g
LEFT JOIN public.beach_enrichment_provenance b
  ON b.gold_fid     = g.fid
 AND b.field_group  = 'governance'
 AND b.is_canonical = true;

COMMENT ON VIEW public.beach_governance_status IS
  'Per-beach governance attribution status. is_attributed=true when canonical '
  'BEP row has any of name / operator_name / governing_body_name populated. '
  'Fixed 2026-05-19: was checking only `name` — OR/WA BEP sources use '
  '`operator_name`, park_url_buffer_attribution uses `governing_body_name`. '
  'Single COALESCE fix unlocked ~380 OR+WA beaches that were already attributed '
  'but invisible to count_unattributed_beaches().';

-- Verification
SELECT
  state,
  count(*)                                  AS scoreable,
  count(*) FILTER (WHERE is_attributed)     AS attributed,
  count(*) FILTER (WHERE NOT is_attributed) AS unattributed
FROM public.beach_governance_status
WHERE is_active = true AND is_scoring_active = true AND state IN ('CA','OR','WA')
GROUP BY state ORDER BY state;

-- Cross-check via count_unattributed_beaches (should now match above)
SELECT * FROM public.count_unattributed_beaches('OR');
SELECT * FROM public.count_unattributed_beaches('WA');
SELECT * FROM public.count_unattributed_beaches('CA');

COMMIT;
