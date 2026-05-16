-- Consensus engine rewrite — Phase 7.2.3: resolve remaining
-- multi-rank-1 dog_policy conflicts.
--
-- After Phase 7.2.2, 6 beaches still have multiple agencies at
-- rank=1 on dog_policy:
--   5 Sonoma State Park beaches with both DPR + Sonoma County
--     (introduced when 7.2.2 demoted State Lands and inserted
--     county fallback, but DPR was already there from PAD-US)
--   1 Swim Beach (Big Bear) with USFS + Big Bear Valley
--     Recreation and Park District
--
-- Authority hierarchy for rank-1 conflict resolution:
--   federal_military > federal_department >
--   state_department > county_department > special_district >
--   county > city
--
-- Tied authorities (e.g., both federal_department) keep both at
-- rank=1; the consensus engine treats them as concurrent.

WITH ranked AS (
  SELECT ba.beach_fid, ba.agency_id, a.type,
         CASE a.type
           WHEN 'federal_military'   THEN 1
           WHEN 'federal_department' THEN 2
           WHEN 'state_department'   THEN 3
           WHEN 'county_department'  THEN 4
           WHEN 'special_district'   THEN 5
           WHEN 'county'             THEN 6
           WHEN 'city'               THEN 7
           ELSE 9
         END AS authority_rank
    FROM public.beach_agency ba
    JOIN public.agency a ON a.id = ba.agency_id
   WHERE ba.authority_domain='dog_policy'
     AND ba.precedence_rank=1
),
beach_min AS (
  SELECT beach_fid, MIN(authority_rank) AS keep_rank
    FROM ranked
   GROUP BY beach_fid
  HAVING COUNT(*) > 1
)
UPDATE public.beach_agency ba
   SET precedence_rank = 2
  FROM ranked r, beach_min m
 WHERE ba.beach_fid = r.beach_fid
   AND ba.agency_id = r.agency_id
   AND ba.authority_domain = 'dog_policy'
   AND ba.precedence_rank = 1
   AND r.beach_fid = m.beach_fid
   AND r.authority_rank > m.keep_rank;
