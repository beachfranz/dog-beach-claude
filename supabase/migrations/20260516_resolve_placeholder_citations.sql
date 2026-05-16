-- Resolve placeholder citations from walkthrough seeds round 2.
--
-- Three placeholder policy_source rows updated with concrete citations
-- + full_text discovered via the statute-capture tooling:
--
-- 1. Manhattan Beach MC §12.08.020 (Animals) — fetched via Municode
--    (manhattan_beach/code_of_ordinances, nodeId=TIT12BEPARE).
--    Verbatim: "No person shall bring, maintain, ride or lead any
--    cattle, horse, mule, burro, donkey, goat, sheep, swine, dog,
--    cat, or other animal of any kind onto, on, upon, or along any
--    beach or into the waters of the Pacific Ocean adjacent to
--    any beach."
--
-- 2. OC County Code §2-5-39 (Controlling domestic animals) — fetched
--    via Municode (orange_county/code_of_ordinances). Title 2 Public
--    Facilities, Division 5 Parks Beaches and Recreational Areas,
--    Article 2 Beaches and Beach Areas. NEW policy_source row +
--    linked to Salt Creek (8316).
--
-- 3. Coronado MC Chapter 32.08 — URL discovered on coronado.ca.us
--    pointing at coronado.municipal.codes. Full text NOT fetched
--    (Cloudflare Turnstile interstitial blocks Playwright). Citation
--    + URL updated; full_text deferred to user-paste or browser
--    capture.

-- ─── 1. Manhattan Beach MC §12.08.020 ───────────────────────────

UPDATE public.policy_source
   SET citation = 'Manhattan Beach Municipal Code §12.08.020 (Animals)',
       source_url = 'https://library.municode.com/ca/manhattan_beach/codes/code_of_ordinances?nodeId=TIT12BEPARE',
       full_text = 'No person shall bring, maintain, ride or lead any cattle, horse, mule, burro, donkey, goat, sheep, swine, dog, cat, or other animal of any kind onto, on, upon, or along any beach or into the waters of the Pacific Ocean adjacent to any beach. [MBMC Title 12 Beaches Parks and Recreation, Chapter 12.08 Beach Regulations Rules and Regulations, §12.08.020 Animals. Fetched via Municode 2026-05-16.]',
       last_verified = NOW()
 WHERE citation LIKE 'Manhattan Beach Municipal Code%';

-- Link Manhattan Beach sand to its city statute (tier 1 alongside the
-- existing LA County rules at tier 3).
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url)
SELECT 8265,
       (SELECT id FROM public.policy_source
         WHERE citation = 'Manhattan Beach Municipal Code §12.08.020 (Animals)'),
       'sand', 'not_allowed', 'operative'::operative_status,
       'No person shall bring, maintain, ride or lead any... dog, cat, or other animal of any kind onto, on, upon, or along any beach or into the waters of the Pacific Ocean adjacent to any beach.',
       'https://library.municode.com/ca/manhattan_beach/codes/code_of_ordinances?nodeId=TIT12BEPARE'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 2. OC County Code §2-5-39 ──────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'OC County Code §2-5-39 (Controlling domestic animals — Beaches)',
       (SELECT id FROM public.agency WHERE name='Orange County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/ca/orange_county/codes/code_of_ordinances?nodeId=TIT2PUFA_DIV5PABEREAR_ART2REARGE',
       '(a) Leash Required. No person shall allow a dog, cat or other domestic animal in any park, beach or recreational area unless the animal is restrained at all times by a substantial leash not to exceed six (6) feet in length and in the control of a person competent to restrain the animal, or unless the animal is restrained and enclosed in a cage, crate or similar enclosure. (b) Unattended Animal. No person shall leave an animal unattended and tied to an object or confined in a vehicle in any park, beach or recreational area. (c) Animal Wastes. All persons shall remove and properly dispose of animal excreta from any park, beach or recreational area. (d) Designated Areas. No person shall allow a dog, cat or other domestic animal in any park, beach or recreational area except in areas designated and under conditions established by the Director. (e) Feral Animals. [...] [OC County Code Title 2 Public Facilities, Division 5 Parks Beaches and Recreational Areas, Article 2 Beaches and Beach Areas, §2-5-39. Fetched via Municode 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source
   WHERE citation LIKE 'OC County Code §2-5-39%'
);

-- Link Salt Creek to the tier-1 OC County Code statute. Note: the
-- statute is "leash required (a)" baseline + "designated areas (d)"
-- giving the Director authority to restrict — which is how the
-- Salt Creek-specific admin policy (already linked, ps row id we'll
-- look up) imposes "Dogs not permitted on the beach."
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   status_basis_id,
   evidence_verbatim, evidence_url, status_note)
SELECT 8316,
       (SELECT id FROM public.policy_source
         WHERE citation = 'OC County Code §2-5-39 (Controlling domestic animals — Beaches)'),
       'sand', 'on_leash', 'superseded_by_lower_tier'::operative_status,
       (SELECT id FROM public.policy_source
         WHERE source_url='https://www.ocparks.com/beaches/salt-creek-beach/park-rules'),
       'OC County Code §2-5-39(a) requires leashed domestic animals as the baseline; §2-5-39(d) authorizes the Director of OC Parks to designate restricted areas. The Salt Creek-specific admin policy designates sand as no-dogs.',
       'https://library.municode.com/ca/orange_county/codes/code_of_ordinances?nodeId=TIT2PUFA_DIV5PABEREAR_ART2REARGE',
       'Tier-1 baseline (leashed) operatively superseded at Salt Creek sand by tier-3 OC Parks Director designation (no dogs).'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- Link Salt Creek paved_walkways to the tier-1 baseline (operative at
-- this section — the OC Parks page allows leashed dogs on paths).
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url)
SELECT 8316,
       (SELECT id FROM public.policy_source
         WHERE citation = 'OC County Code §2-5-39 (Controlling domestic animals — Beaches)'),
       'paved_walkways', 'on_leash', 'operative'::operative_status,
       'OC County Code §2-5-39(a) — leashed dogs allowed on paths. Salt Creek-specific admin policy confirms operative.',
       'https://library.municode.com/ca/orange_county/codes/code_of_ordinances?nodeId=TIT2PUFA_DIV5PABEREAR_ART2REARGE'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 3. Coronado MC Chapter 32.08 ───────────────────────────────

UPDATE public.policy_source
   SET citation = 'Coronado Municipal Code Chapter 32.08 (Animal Regulations)',
       source_url = 'https://coronado.municipal.codes/CMC/32.08',
       last_verified = NOW(),
       full_text = COALESCE(full_text, '[Full text fetch blocked by Cloudflare Turnstile on coronado.municipal.codes 2026-05-16. Operative rules captured at agency_administrative_policy tier via coronado.ca.us/757/Dogs.]')
 WHERE citation = 'Coronado Municipal Code Title 32 — Animal Regulations';
