-- Wave 2 follow-up: NPS units OUTSIDE GGNRA — federal baseline backfill.
--
-- GGNRA was covered separately (Wave 2 GGNRA backfill). This migration
-- handles other NPS-administered units with beaches in CA:
--   - Point Reyes National Seashore (Marin)
--   - Phillip Burton Wilderness Area (Marin)
--   - Whiskeytown-Shasta-Trinity NRA (Shasta)
--   - Yosemite National Park (Tuolumne)
--   - Channel Islands National Park (Ventura)
--   - Aquatic Park (SF)
--
-- All NPS units share 36 CFR §2.15 as the federal pet baseline
-- (policy_source id 1, already in DB). Per-unit Compendiums typically
-- modify — Point Reyes is notably restrictive (most beaches prohibit
-- dogs for wildlife protection).
--
-- 32 beaches in scope (not already linked to the GGNRA Compendium).

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
WITH nps_id AS (
  SELECT id FROM public.agency WHERE name='National Park Service' AND type='federal_department'
), targets AS (
  SELECT DISTINCT g.fid, g.park_name
  FROM public.beaches_gold g
  LEFT JOIN public.pad_us_units p ON ST_Intersects(p.geom, ST_SetSRID(ST_MakePoint(g.lon, g.lat), 4326))
  LEFT JOIN public.beach_agency ba ON ba.beach_fid = g.fid
  WHERE g.state='CA' AND g.is_active=true
    AND ((p.own_name='NPS' AND (p.unit_name IS NULL OR p.unit_name NOT ILIKE '%Golden Gate%'))
         OR ba.agency_id = (SELECT id FROM nps_id))
    AND NOT EXISTS (
      SELECT 1 FROM public.beach_policy_source bps
      WHERE bps.beach_fid = g.fid AND bps.policy_source_id = 19
    )
)
SELECT t.fid, 1, 'sand', 'on_leash', 'operative'::operative_status,
       '36 CFR §2.15: pets must be restrained on a leash not to exceed six feet in length, crated, caged, or otherwise physically confined at all times. Exception under §2.15(b) authorizes superintendent to designate areas.',
       'https://www.ecfr.gov/current/title-36/chapter-I/part-2/section-2.15',
       CASE
         WHEN t.park_name ILIKE '%Point Reyes%' OR t.park_name ILIKE '%Phillip Burton%'
           THEN 'Point Reyes / Burton Wilderness: federal baseline. Per-unit Compendium typically restricts dogs on most beaches for snowy plover and tule elk wildlife protection. Per-beach refinement (rule=not_allowed where Compendium prohibits) deferred.'
         WHEN t.park_name ILIKE '%Channel Islands%'
           THEN 'Channel Islands NP: dogs generally prohibited except on Santa Rosa Island and certain campsites. Federal baseline §2.15 + per-unit Compendium override deferred.'
         WHEN t.park_name ILIKE '%Whiskeytown%' OR t.park_name ILIKE '%Yosemite%'
           THEN 'Inland NRA / national park: federal baseline. Per-unit Compendium variations deferred.'
         ELSE 'Federal NPS baseline. Per-unit Compendium may modify — deferred.'
       END
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
