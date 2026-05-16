-- Consensus engine rewrite — Phase 7.2.2: cleanup surfaced by
-- Phase 7.2's PAD-US overlay.
--
-- Two fixes:
--
-- 1. DEDUP BLM agency rows. CPAD seeded "United States Bureau of
--    Land Management" (Phase 7); Phase 7.2 inserted "Bureau of
--    Land Management" as a separate row from a hard-coded VALUES
--    list. Two beaches (Sea View, Pit Stop) ended up with both at
--    rank=1. Canonical name keeps the "United States" prefix to
--    match CPAD vocabulary.
--
-- 2. DEMOTE State Lands Commission dog_policy → land_use. State
--    Lands has authority over tidelands (ownership) but doesn't
--    set dog rules — those flow from the adjacent upland owner
--    (DPR if state park; city if city beach; county if
--    unincorporated). 7 beaches affected; dog_policy primary
--    should fall through to city/county per the cascade.

-- ─── 1. BLM dedup ────────────────────────────────────────────────

-- Remap any beach_agency rows pointing at the short-name BLM row
-- to the long-name BLM row. ON CONFLICT to handle the case where
-- the long-name row already exists at the same (beach, domain).
DO $$
DECLARE
  short_id BIGINT;
  long_id  BIGINT;
BEGIN
  SELECT id INTO short_id FROM public.agency
   WHERE name='Bureau of Land Management' AND type='federal_department';
  SELECT id INTO long_id FROM public.agency
   WHERE name='United States Bureau of Land Management' AND type='federal_department';

  IF short_id IS NOT NULL AND long_id IS NOT NULL THEN
    -- Move rows that don't collide
    UPDATE public.beach_agency
       SET agency_id = long_id
     WHERE agency_id = short_id
       AND NOT EXISTS (
         SELECT 1 FROM public.beach_agency ba2
          WHERE ba2.beach_fid = beach_agency.beach_fid
            AND ba2.agency_id = long_id
            AND ba2.authority_domain = beach_agency.authority_domain
       );
    -- Delete rows that would have collided
    DELETE FROM public.beach_agency WHERE agency_id = short_id;
    -- Delete the duplicate agency
    DELETE FROM public.agency WHERE id = short_id;
  END IF;
END$$;

-- ─── 2. Demote State Lands dog_policy → tidelands/land_use only ──

WITH state_lands_beaches AS (
  SELECT ba.beach_fid
    FROM public.beach_agency ba
    JOIN public.agency a ON a.id = ba.agency_id
   WHERE a.name = 'California State Lands Commission'
     AND ba.authority_domain = 'dog_policy'
     AND ba.precedence_rank = 1
)
-- Drop the dog_policy rank=1 row for State Lands at these beaches
DELETE FROM public.beach_agency ba
 USING public.agency a, state_lands_beaches s
 WHERE ba.beach_fid = s.beach_fid
   AND ba.agency_id = a.id
   AND a.name = 'California State Lands Commission'
   AND ba.authority_domain = 'dog_policy';

-- For each affected beach, install city/county as dog_policy primary
-- (re-running the Phase 7.1 cascade for just these beaches).

WITH affected AS (
  SELECT g.fid, g.county_name, j.name AS city_name, j.place_type AS city_place_type
    FROM public.beaches_gold g
    LEFT JOIN public.jurisdictions j ON j.id = g.c1_jurisdiction_id
   WHERE g.fid IN (8274, 8276, 8742, 8839, 8854, 9261, 9606)
),
resolved AS (
  SELECT a.fid,
    CASE
      WHEN a.city_name IS NOT NULL
       AND a.city_place_type IN ('C1','C5','C9','M2')
        THEN (SELECT id FROM public.agency
               WHERE name = a.city_name AND type='city' LIMIT 1)
      ELSE (SELECT id FROM public.agency
             WHERE name = a.county_name || ' County' AND type='county' LIMIT 1)
    END AS primary_agency_id
  FROM affected a
)
INSERT INTO public.beach_agency (beach_fid, agency_id, authority_domain, precedence_rank)
SELECT fid, primary_agency_id, 'dog_policy', 1
  FROM resolved
 WHERE primary_agency_id IS NOT NULL
ON CONFLICT (beach_fid, agency_id, authority_domain) DO NOTHING;

-- Make sure State Lands keeps a land_use + tidelands authority row
INSERT INTO public.beach_agency (beach_fid, agency_id, authority_domain, precedence_rank)
SELECT fid,
       (SELECT id FROM public.agency
         WHERE name='California State Lands Commission' AND type='state_department'),
       d.dom,
       1
  FROM (VALUES (8274), (8276), (8742), (8839), (8854), (9261), (9606)) f(fid)
  CROSS JOIN (VALUES ('tidelands'), ('land_use')) d(dom)
ON CONFLICT (beach_fid, agency_id, authority_domain) DO NOTHING;
