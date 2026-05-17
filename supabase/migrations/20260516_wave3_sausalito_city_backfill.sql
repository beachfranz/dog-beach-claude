-- Wave 3: City of Sausalito — 3 beaches, default on_leash per §6.04.160
-- (citywide leash law; no beach-specific prohibition codified or posted).
--
-- Source: Sausalito Municipal Code §6.04.160 (Leash law), Title 6
-- ANIMALS. Code hosted on ecode360 (custid SA4880) at
-- https://ecode360.com/SA4880. Recently migrated from Code Publishing
-- (codepublishing.com/CA/Sausalito/) — old URL now redirects to
-- ecode360.
--
-- Operative rule (per City of Sausalito Animal Control + multiple
-- corroborating sources):
--   "Dogs must be on leash at all times per City Ordinance 6.04.160."
--   Off-leash exception: Remington Dog Park (the city's designated
--   off-leash dog park).
--   Park-specific no-dog rules: "No dogs, with the exception of dogs
--   which are 'Service Animals' as defined in the American's with
--   Disabilities Act of 1990, whether on or off leash, shall be
--   permitted in Cazneau Park, Harrison Park, the fenced in children's
--   area of Robin Sweeny Park, or the fenced in children's area of
--   South View Park."
--
-- Sausalito does NOT codify a citywide beach prohibition. Several
-- secondary sources (BringFido) describe Schoonmaker Beach as
-- "not dog-friendly" but city ordinance and the City's own facility
-- pages do not impose a beach-specific prohibition. Default citywide
-- leash law applies.
--
-- 3 Sausalito-city-polygon beaches, all unlinked:
--   - fid 8658 Schoonmaker Beach — small bay beach south of Schoonmaker
--                                  Point Marina; city-jurisdiction.
--                                  Default on_leash.
--   - fid 9301 Swede's Beach     — small bay beach at east end of Valley
--                                  Street; reported active dog use.
--                                  Default on_leash.
--   - fid 6099 Tiffany Beach     — small bay beach near Tiffany Park;
--                                  city-jurisdiction. Default on_leash.

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Sausalito Municipal Code §6.04.160 (Citywide leash law — dogs must be on leash at all times on public property)',
       (SELECT id FROM public.agency WHERE name='Sausalito' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://ecode360.com/SA4880',
       'Sausalito Municipal Code Title 6 ANIMALS, §6.04.160 (per City of Sausalito Animal Control + corroborating City facility pages, verbatim summary): "Dogs must be on leash at all times per City Ordinance 6.04.160." Companion city beach/park policy: "No dogs, with the exception of dogs which are ''Service Animals'' as defined in the American''s with Disabilities Act of 1990, whether on or off leash, shall be permitted in Cazneau Park, Harrison Park, the fenced in children''s area of Robin Sweeny Park, or the fenced in children''s area of South View Park." Off-leash exception: Remington Dog Park (the city''s designated off-leash dog park). Sausalito does NOT codify a citywide beach prohibition; default leash law applies on beaches. Several secondary sources (BringFido) describe Schoonmaker Beach as "not dog-friendly" but city ordinance and the City''s own facility pages do not impose a beach-specific prohibition — only Cazneau Park, Harrison Park, and specific fenced children''s areas are blanket no-dog. Pacific Grove Municipal Code recently migrated from Code Publishing to ecode360 (custid SA4880) at ecode360.com/SA4880. [Fetched verbatim 2026-05-16 via web search; ecode360 SPA contents corroborated.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Sausalito Municipal Code §6.04.160%'
);

-- ─── 2. Link all 3 Sausalito beaches → on_leash ─────────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Sausalito MC §6.04.160: dogs must be on leash at all times on public property (citywide). No beach-specific prohibition in city ordinance or on City facility pages.',
       ps.source_url,
       CASE g.name
         WHEN 'Schoonmaker Beach' THEN 'BringFido describes this beach as "not dog-friendly" in practice; City has not codified a beach-specific prohibition. Walkthrough recommended.'
         ELSE NULL
       END
FROM public.beaches_gold g
JOIN public.jurisdictions j ON st_intersects(j.geom, g.geom)
  AND j.state IN ('California','CA') AND j.place_type='C1' AND j.name='Sausalito'
CROSS JOIN public.policy_source ps
WHERE g.state='CA' AND g.is_active=true
  AND ps.citation LIKE 'Sausalito Municipal Code §6.04.160%'
  AND NOT EXISTS (
    SELECT 1 FROM public.beach_policy_source bps WHERE bps.beach_fid=g.fid
  )
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
