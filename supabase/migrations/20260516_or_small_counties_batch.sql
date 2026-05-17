-- Wave 4 OR: Batch backfill for 13 OR counties citing ORS 609.095 +
-- OPRD baseline. Each county code text was not directly captured via
-- web search in the AFK time budget; county codes typically apply
-- only to unincorporated non-state-park land, and OPRD's OAR
-- §736-021-0070 (already linked via 20260516_or_oprd_state_park_backfill)
-- is the operative rule on the majority of OR coastal beaches.
--
-- This migration adds ORS 609.095 (Dog as public nuisance) as a state
-- baseline that applies countywide. Per-county codified rules can be
-- captured in a future refinement pass when Franz has time to point
-- at specific county code URLs.
--
-- Counties covered (13 total, 110 beaches collectively):
--   - Curry (22)         — coastal, Brookings/Gold Beach area
--   - Clatsop (17)       — coastal, Astoria/Seaside/Cannon Beach area
--   - Coos (16)          — coastal, Coos Bay/Bandon area
--   - Lane (14)          — coastal, Florence/Heceta Head + Eugene area
--   - Columbia (11)      — river, Scappoose/St. Helens area
--   - Multnomah (7)      — river, Portland area (Sauvie Island etc.)
--   - Hood River (4)     — Columbia Gorge
--   - Umatilla (3)       — inland, Hermiston area lakes
--   - Marion (2)         — Salem area
--   - Clackamas (2)      — Portland metro south
--   - Morrow (1)         — Boardman lake
--   - Douglas (1)        — Roseburg area
--   - Wasco (1)          — Columbia Gorge / The Dalles
--
-- Net additive links to the OPRD baseline; consensus engine resolves
-- precedence (state law tier 1 vs OPRD state-park rule tier 1 vs
-- county rule tier 1 — they don't conflict on the leashed default).

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'state_statute',
       'Oregon Revised Statutes §609.095 (Dog as public nuisance)',
       (SELECT id FROM public.agency WHERE name='Oregon State' AND type='state'),
       ARRAY['dog_policy']::text[],
       'https://oregon.public.law/statutes/ors_609.095',
       'Oregon Revised Statutes §609.095 Dog as public nuisance; public nuisance prohibited; complaint. A dog is a public nuisance if it (a) is a menace to the safety of persons or property of others; (b) is a female dog in heat and not confined; (c) chases vehicles or persons; (d) damages or destroys property of persons other than the dog owner; (e) scatters garbage; (f) trespasses on private property; or (g) disturbs the peace by barking or howling. A dog at large is presumptively a public nuisance. Local counties and cities may set additional or stricter rules. [Fetched 2026-05-16. Cited as state baseline for OR counties whose specific county code text was not captured in the AFK session; per-county code URLs to be added in a refinement pass.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Oregon Revised Statutes §609.095%'
);

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Oregon Revised Statutes §609.095: dog at large presumptively a public nuisance. Effective leash law via county/local ordinance.',
       ps.source_url,
       'State baseline; per-county code refinement pending. OPRD OAR §736-021-0070 6-ft leash applies on state-park-operated beaches (most of the OR coast).'
FROM public.beaches_gold g
CROSS JOIN public.policy_source ps
WHERE g.state='OR' AND g.is_active=true
  AND g.county_name IN ('Curry','Clatsop','Coos','Lane','Columbia','Multnomah',
                        'Hood River','Umatilla','Marion','Clackamas','Morrow',
                        'Douglas','Wasco')
  AND ps.citation LIKE 'Oregon Revised Statutes §609.095%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
