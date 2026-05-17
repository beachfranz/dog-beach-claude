-- Wave 3: City of Oceanside — blanket no-dogs-on-beach per §4.15.
--
-- Source: Oceanside Code of Ordinances Chapter 4 (Animals and Fowl),
-- Article III, Division 1, §4.15 "Dogs on public beach."
-- URLs:
--   https://library.municode.com/ca/oceanside/codes/code_of_ordinances?nodeId=CH4ANFO
--   http://oceanside-ca.elaws.us/code/coor_ch4_artiii_div1_sec4.15
--   https://www.ci.oceanside.ca.us/government/public-works/beaches-pier/beaches/beach-pier-rules-regulations
--
-- Operative rule (per city Beach & Pier Rules + Oceanside Dog Beach
-- advocacy organization summary): While leashed dogs are permitted
-- along The Strand (the paved promenade), dogs are not allowed on the
-- beach sand itself. Oceanside is the southern bookend to a chain of
-- North County SD coastal cities — Carlsbad (no dogs on ocean beach),
-- Oceanside (no dogs on beach), and Encinitas (leashed dogs allowed
-- on beach).
--
-- 3 Oceanside-city-polygon beaches:
--   - fid 8553 Buccaneer Beach          — already linked (CA DPR /
--                                         San Diego County); skipped
--   - fid 8470 Harbor Beach             — Oceanside as precedence-1;
--                                         unlinked → not_allowed
--   - fid 9047 Oceanside City Beach     — already linked; skipped
--
-- Net 1 new link (Harbor Beach).

-- ─── 1. Insert policy_source ────────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Oceanside Code of Ordinances Ch 4 / Art III / Div 1 / §4.15 (Dogs on public beach)',
       (SELECT id FROM public.agency WHERE name='Oceanside' AND type='city'),
       ARRAY['dog_policy']::text[],
       'http://oceanside-ca.elaws.us/code/coor_ch4_artiii_div1_sec4.15',
       'Oceanside Code of Ordinances Chapter 4 (Animals and Fowl), Article III (Dogs), Division 1 (Generally), §4.15 "Dogs on public beach." Operative rule per City Beach & Pier Rules & Regulations: leashed dogs are permitted along The Strand (paved promenade), but dogs are NOT allowed on the beach sand. [Source: City of Oceanside Beach & Pier Rules + Oceanside Code of Ordinances Ch 4. Verbatim §4.15 text not directly captured from Municode (chapter TOC fetched). Fetched 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Oceanside Code of Ordinances%§4.15%'
);

-- ─── 2. Link unlinked Oceanside beaches → not_allowed ──────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'Oceanside Code of Ordinances §4.15 (Dogs on public beach): dogs prohibited on city beach sand. The Strand (paved promenade) allows leashed dogs.',
       ps.source_url, NULL
FROM public.beaches_gold g
JOIN public.jurisdictions j ON st_intersects(j.geom, g.geom)
  AND j.state IN ('California','CA') AND j.place_type='C1' AND j.name='Oceanside'
CROSS JOIN public.policy_source ps
WHERE g.state='CA' AND g.is_active=true
  AND ps.citation LIKE 'Oceanside Code of Ordinances%§4.15%'
  AND NOT EXISTS (
    SELECT 1 FROM public.beach_policy_source bps WHERE bps.beach_fid=g.fid
  )
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
