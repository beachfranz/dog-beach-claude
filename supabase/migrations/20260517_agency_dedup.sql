-- Agency dedup: remove 31 duplicate agency rows that were created in
-- parallel with the canonical bare-name entries. In every case the orphan
-- has ZERO policy_source and ZERO beach_agency references — only one
-- operator row needs an FK remap before delete.
--
-- Pattern: bare-name = CANONICAL (carries ps + beach_agency); the
-- "X, City of" or "United States X" form = ORPHAN.
--
-- Surfaced today after the Morro Bay agent flagged "duplicate Morro Bay,
-- City of at id=258". Systematic audit confirmed 30 city pairs + 1 federal
-- pair across the agency table.

BEGIN;

-- ─── 1. Remap the one operator FK referencing the federal orphan ───
-- operator id 147 "United States National Park Service" points at the
-- orphan agency 293; remap to canonical 307 "National Park Service".
-- Also normalize the operator's display name to match the canonical agency.

UPDATE public.operator
   SET agency_id = 307,
       name = 'National Park Service'
 WHERE id = 147 AND agency_id = 293;

-- ─── 2. Confirm no other FK references remain to orphans ──────────
-- (Sanity check; will raise an exception if any orphans still referenced.)
DO $$
DECLARE
  v_ps_refs   int;
  v_ba_refs   int;
  v_op_refs   int;
  v_par_refs  int;
  orphan_ids  int[] := ARRAY[
    242,243,244,245,246,247,248,249,250,251,252,253,254,256,257,258,259,
    260,261,262,263,264,265,266,267,269,270,271,272,273,293
  ];
BEGIN
  SELECT count(*) INTO v_ps_refs  FROM public.policy_source WHERE issuing_agency_id  = ANY(orphan_ids);
  SELECT count(*) INTO v_ba_refs  FROM public.beach_agency  WHERE agency_id          = ANY(orphan_ids);
  SELECT count(*) INTO v_op_refs  FROM public.operator      WHERE agency_id          = ANY(orphan_ids);
  SELECT count(*) INTO v_par_refs FROM public.agency        WHERE parent_agency_id   = ANY(orphan_ids);
  IF v_ps_refs > 0 OR v_ba_refs > 0 OR v_op_refs > 0 OR v_par_refs > 0 THEN
    RAISE EXCEPTION 'Aborting dedup: orphans still referenced (ps=%, ba=%, operator=%, parent=%).',
      v_ps_refs, v_ba_refs, v_op_refs, v_par_refs;
  END IF;
END $$;

-- ─── 3. Delete the 31 orphan agency rows ──────────────────────────
DELETE FROM public.agency
 WHERE id IN (
   -- 30 city orphans (each is "X, City of" duplicate of a bare-name entry)
   242, -- Alameda, City of           → 109 Alameda
   243, -- Avalon, City of            → 49  Avalon
   244, -- Capitola, City of          → 79  Capitola
   245, -- Coronado, City of          → 96  Coronado
   246, -- Dana Point, City of        → 93  Dana Point
   247, -- Del Mar, City of           → 48  Del Mar
   248, -- Fort Bragg, City of        → 106 Fort Bragg
   249, -- Goleta, City of            → 51  Goleta
   250, -- Hermosa Beach, City of     → 54  Hermosa Beach
   251, -- Huntington Beach, City of  → 92  Huntington Beach
   252, -- Laguna Beach, City of      → 94  Laguna Beach
   253, -- Lake Elsinore, City of     → 75  Lake Elsinore
   254, -- Long Beach, City of        → 76  Long Beach
   256, -- Los Angeles, City of       → 66  Los Angeles
   257, -- Monterey, City of          → 46  Monterey
   258, -- Morro Bay, City of         → 59  Morro Bay
   259, -- Newport Beach, City of     → 55  Newport Beach
   260, -- Pacific Grove, City of     → 73  Pacific Grove
   261, -- Pacifica, City of          → 71  Pacifica
   262, -- Palos Verdes Estates, City of → 63 Palos Verdes Estates
   263, -- Port Hueneme, City of      → 67  Port Hueneme
   264, -- Rancho Palos Verdes, City of → 77 Rancho Palos Verdes
   265, -- Reedley, City of           → 60  Reedley
   266, -- San Clemente, City of      → 90  San Clemente
   267, -- San Diego, City of         → 101 San Diego
   269, -- Santa Barbara, City of     → 52  Santa Barbara
   270, -- Santa Cruz, City of        → 61  Santa Cruz
   271, -- Sausalito, City of         → 102 Sausalito
   272, -- Seal Beach, City of        → 91  Seal Beach
   273, -- Solana Beach, City of      → 114 Solana Beach
   -- 1 federal orphan
   293  -- United States National Park Service → 307 National Park Service
 );

COMMIT;
