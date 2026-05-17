-- Wave 3: City of Capitola — 3 beaches, blanket no-dogs per §6.14.200(A)(1).
--
-- Source: Capitola Municipal Code §6.14.200 (Dogs in public places —
-- Permitted and prohibited locations), Chapter 6.14 Dogs and Other
-- Domesticated Animals.
-- URL: https://www.codepublishing.com/CA/Capitola/html/Capitola06/Capitola0614.html
-- Code Publishing platform.
--
-- Operative rule (§6.14.200 verbatim, fetched via curl 2026-05-16):
--   A. Except as provided in Section 6.14.210, it is unlawful for any
--   person owning, having an interest in, harboring or having charge of
--   the care, custody, control or possession of any dog to cause or
--   permit such dog to be in any of the following locations, whether
--   with or without a leash:
--     1. On any public beach within the city of Capitola;
--     2. Capitola Wharf;
--     3. Any other public location in the city that is posted with
--        signage prohibiting dogs.
--   B. Dogs shall be permitted on leash in the following parks unless
--   the city council, by resolution, declares the prohibitions of
--   subsection A of this section applicable to such areas: Monterey
--   Avenue Park, Noble Gulch Park, Perry Park, Soquel Creek Park,
--   Jade Street Park (except for lawn and tennis court areas), and
--   Esplanade Park (except for lawn areas).
--
-- §6.14.210 exemptions: guide/signal/service dogs; police dogs.
-- §6.14.320 (Leashing required): public streets, alleys, unenclosed
-- lots, etc. — 6-foot max leash (general citywide leash law where
-- dogs are permitted).
--
-- Esplanade Park Beach note: §6.14.200(B) permits leashed dogs in the
-- park (except lawn areas), but the beach portion is governed by
-- §6.14.200(A)(1)'s "any public beach" blanket prohibition. The park
-- exemption is for the park grounds, not the contiguous beach.
--
-- 3 Capitola-city-polygon beaches:
--   - fid 8564 Capitola Beach       → not_allowed (public beach)
--   - fid 9013 Esplanade Park Beach → not_allowed (the beach portion;
--                                     adjoining Esplanade Park is on-leash)
--   - fid 8884 Hooper Beach         → not_allowed (public beach)
--
-- All 3 → not_allowed.

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Capitola Municipal Code §6.14.200 (Dogs in public places — public beaches and wharf prohibited; named parks permitted on-leash)',
       (SELECT id FROM public.agency WHERE name='Capitola' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://www.codepublishing.com/CA/Capitola/html/Capitola06/Capitola0614.html',
       'Capitola Municipal Code Chapter 6.14 DOGS AND OTHER DOMESTICATED ANIMALS, §6.14.200 Dogs in public places — Permitted and prohibited locations (verbatim, fetched via curl 2026-05-16): A. Except as provided in Section 6.14.210, it is unlawful for any person owning, having an interest in, harboring or having charge of the care, custody, control or possession of any dog to cause or permit such dog to be in any of the following locations, whether with or without a leash: 1. On any public beach within the city of Capitola; 2. Capitola Wharf; 3. Any other public location in the city that is posted with signage prohibiting dogs. B. Dogs shall be permitted on leash in the following parks unless the city council, by resolution, declares the prohibitions of subsection A of this section applicable to such areas: Monterey Avenue Park, Noble Gulch Park, Perry Park, Soquel Creek Park, Jade Street Park (except for lawn and tennis court areas), and Esplanade Park (except for lawn areas). §6.14.210 Exemptions: guide/signal/service dogs; police/law enforcement dogs. §6.14.215 Removal of dog droppings required. §6.14.310 Prohibition against permitting dogs at large. §6.14.320 Leashing required: 6-foot max leash on public streets, alleys, other public places (where dogs are permitted). §12.48.010 (Beach and Wharf Area Regulations) reinforces wharf prohibition. Ozzi Dog Park is the city''s off-leash facility. (Ord. 912 §2, 2006; Ord. 915 §1, 2007). [Hosted on Code Publishing at codepublishing.com/CA/Capitola/. Fetched verbatim 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Capitola Municipal Code §6.14.200%'
);

-- ─── 2. Link all 3 Capitola beaches → not_allowed ────────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'Capitola MC §6.14.200(A)(1): "it is unlawful for any person ... to cause or permit such dog to be ... 1. On any public beach within the city of Capitola ... whether with or without a leash."',
       ps.source_url,
       CASE g.name
         WHEN 'Esplanade Park Beach' THEN 'Adjoining Esplanade Park permits leashed dogs (except lawn areas) per §6.14.200(B); the beach portion remains under §6.14.200(A)(1) blanket prohibition.'
         ELSE NULL
       END
FROM public.beaches_gold g
JOIN public.jurisdictions j ON st_intersects(j.geom, g.geom)
  AND j.state IN ('California','CA') AND j.place_type='C1' AND j.name='Capitola'
CROSS JOIN public.policy_source ps
WHERE g.state='CA' AND g.is_active=true
  AND ps.citation LIKE 'Capitola Municipal Code §6.14.200%'
  AND NOT EXISTS (
    SELECT 1 FROM public.beach_policy_source bps WHERE bps.beach_fid=g.fid
  )
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
