-- Phase 14b.1 — dedup NPS canonical rows.
--
-- Spot-check on the new resolver revealed two is_canonical=true rows for NPS:
--   541  "National Park Service"               (origin_source='cpad', older)
--   1683 "United States National Park Service" (canonical, used by Phase 12 bridge)
--
-- Both have 'NPS' in pad_us_mng_name array, so the resolver's JOIN sees both
-- and the result is non-deterministic. 1683 is the established canonical
-- (130 beach attributions via entity_operator + the Phase 12 NPS-wide
-- policy_source 383). 541 has 0 entity_operator references.
--
-- Mark 541 as a duplicate of 1683 via parent_operator_id + is_canonical=false.

BEGIN;

-- ─────────────────────────────────────────────────────────────────────
-- 1. Mark op 541 as duplicate of 1683 (agency-wide NPS dedup)
-- ─────────────────────────────────────────────────────────────────────
UPDATE public.operators
   SET parent_operator_id = 1683,
       is_canonical = false,
       updated_at = now()
 WHERE id = 541
   AND (parent_operator_id IS DISTINCT FROM 1683 OR is_canonical IS NOT FALSE);

-- ─────────────────────────────────────────────────────────────────────
-- 2. Strip short codes from per-unit federal operator pad_us_mng_name arrays.
--    Per-unit operators (Point Reyes NS, Yosemite, GGNRA, etc.) shouldn't
--    appear as agency-wide matches in the resolver. Only agency-wide rows
--    (NPS=1683, FWS=1681, etc.) should carry short codes like 'NPS'/'FWS'.
--    Per-unit operators keep their specific long-form names (or empty).
-- ─────────────────────────────────────────────────────────────────────
UPDATE public.operators
   SET pad_us_mng_name = ARRAY(
         SELECT x FROM unnest(pad_us_mng_name) AS x
         WHERE x NOT IN ('NPS','FWS','USFS','BLM','USACE','DOD','BIA','NRCS','USBR','BOEM','NOAA','TVA','USCG','UNK')
       ),
       updated_at = now()
 WHERE is_canonical = true
   AND level = 'federal'
   AND id NOT IN (
     -- Preserve short codes ONLY on agency-wide canonical operators
     1676,  -- USACE
     1677,  -- BLM
     1678,  -- USBR
     1679,  -- USCG
     1680,  -- DOD
     1681,  -- FWS
     1682,  -- USFS
     1683,  -- NPS
     84038, -- BIA
     84039, -- NRCS
     84041, -- BOEM
     84042, -- NOAA
     84043  -- TVA
   )
   AND pad_us_mng_name && ARRAY['NPS','FWS','USFS','BLM','USACE','DOD','BIA','NRCS','USBR','BOEM','NOAA','TVA','USCG','UNK'];

-- Confirm no agency-wide short-code dupes remain
DO $$
DECLARE v_dupes int;
BEGIN
  SELECT count(*) INTO v_dupes
  FROM (
    SELECT mng_name, count(*) AS n
    FROM (
      SELECT unnest(pad_us_mng_name) AS mng_name
      FROM public.operators
      WHERE is_canonical = true AND level = 'federal'
        AND pad_us_mng_name IS NOT NULL
    ) x
    WHERE mng_name IN ('NPS','FWS','USFS','BLM','USACE','DOD','BIA','NRCS','USBR','BOEM','NOAA','TVA','USCG')
    GROUP BY mng_name
    HAVING count(*) > 1
  ) y;

  RAISE NOTICE 'Remaining federal-agency canonical short-code duplicates: %', v_dupes;

  IF v_dupes > 0 THEN
    RAISE EXCEPTION 'Federal short-code dedup incomplete: % duplicates remain', v_dupes;
  END IF;
END $$;

COMMIT;
