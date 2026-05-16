-- Consensus engine rewrite — Phase 7.2.1: undo Phase 7.2's
-- over-application for incorporated-city beaches.
--
-- Problem: PAD-US polygons can spill from a state/federal unit
-- into the adjacent incorporated-city beach centroid. HBDB
-- (fid 6212) is the surfacing case: its centroid falls inside
-- the Bolsa Chica State Beach PAD-US polygon, so Phase 7.2
-- promoted California DPR to rank=1 dog_policy and demoted
-- Huntington Beach to rank=2. That's wrong — HBDB is city
-- land per the walkthrough.
--
-- Fix: PAD-US override should only fire when the beach has NO
-- c1_jurisdiction_id (i.e., not inside an incorporated city).
-- Fort Funston (no c1_jurisdiction) still gets NPS — correct.
-- HBDB (Huntington Beach as c1_jurisdiction) reverts to city —
-- correct.
--
-- 11 beaches were misclassified; this migration reverses them.

-- ─── Identify mis-assigned beaches ────────────────────────────────

CREATE TEMP TABLE _mis_assigned AS
SELECT DISTINCT ba.beach_fid, ba.agency_id AS pad_us_agency_id
  FROM public.beach_agency ba
  JOIN public.beaches_gold g ON g.fid = ba.beach_fid
  JOIN public.agency a ON a.id = ba.agency_id
  JOIN public.jurisdictions j ON j.id = g.c1_jurisdiction_id
 WHERE ba.authority_domain = 'dog_policy'
   AND ba.precedence_rank = 1
   AND a.type IN ('state_department','federal_department','federal_military')
   AND j.place_type IN ('C1','C5','C9','M2')
   -- Restrict to PAD-US derived rows only: skip beaches with cpad_unit_id
   -- pointing at the same agency (those are CPAD-supported, legitimate).
   AND g.cpad_unit_id IS NULL;

-- ─── 1. Delete the PAD-US-derived dog_policy + land_use rows ─────

DELETE FROM public.beach_agency ba
 USING _mis_assigned m
 WHERE ba.beach_fid = m.beach_fid
   AND ba.agency_id = m.pad_us_agency_id
   AND ba.authority_domain IN ('dog_policy','land_use');

-- ─── 2. Restore demoted city dog_policy to rank=1 ────────────────

UPDATE public.beach_agency ba
   SET precedence_rank = 1
  FROM _mis_assigned m, public.agency a
 WHERE ba.beach_fid = m.beach_fid
   AND ba.authority_domain = 'dog_policy'
   AND ba.precedence_rank = 2
   AND a.id = ba.agency_id
   AND a.type IN ('city','county','county_department');

DROP TABLE _mis_assigned;
