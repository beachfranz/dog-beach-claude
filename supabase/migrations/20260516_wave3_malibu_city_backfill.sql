-- Wave 3: City of Malibu — blanket no-dogs-on-beach rule applied to all
-- unlinked Malibu-polygon beaches.
--
-- Source: City of Malibu FAQ category 21 question 64 — "Dogs are not
-- allowed on any beach in Malibu. If you bring your dog to the beach,
-- you will be asked to leave, and you may also be cited."
-- URL: https://www.malibucity.org/m/faq?cat=21#question-64
--
-- Codified backstop: Malibu Municipal Code §12.08.020 adopts Los
-- Angeles County Code Title 17 Chapter 17.12 (Beaches) by reference as
-- the city's beach rules and regulations. The LA County beach rule
-- bans all animals from the sand. Both the operator (LA County) and
-- the jurisdiction (Malibu) thus point to the same outcome:
-- not_allowed.
--
-- 23 Malibu-polygon beaches in beaches_gold. 6 already have a
-- policy_source link (from prior LA County / CA DPR backfills) — left
-- untouched here. 17 are unlinked and get the Malibu policy applied.
--
-- Exception noted (for context, not in scope): Leo Carrillo State
-- Beach (near the Malibu/Ventura County line) allows dogs on leash in
-- some areas — but it's a state beach, not a City of Malibu facility.
--
-- Trancas Canyon Park has a designated dog park — see
-- [[deferred-park-entities-dog-runs]] for when that work picks up.

-- ─── 1. Insert policy_source (agency_administrative_policy) ───────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'City of Malibu — Dogs on Beach FAQ (citywide blanket prohibition) + MMC §12.08.020 adoption of LA County Title 17 Ch 17.12',
       (SELECT id FROM public.agency WHERE name='Malibu' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://www.malibucity.org/m/faq?cat=21#question-64',
       'City of Malibu — Dogs on Beach policy (FAQ): "Dogs are not allowed on any beach in Malibu. If you bring your dog to the beach, you will be asked to leave, and you may also be cited." Codified backstop: Malibu Municipal Code §12.08.020 adopts Los Angeles County Code Title 17, Chapter 17.12 (Beaches) by reference as the city''s beach rules and regulations — LA County''s rule bans animals from beach sand. Designated dog park (off-leash, not a beach): Trancas Canyon Park. Off-leash exception outside city limits: Leo Carrillo State Beach (state, not city). [Fetched 2026-05-16. Codified MMC §12.08.020 reference verified via qcode search.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE source_url='https://www.malibucity.org/m/faq?cat=21#question-64'
);

-- ─── 2. Link the 17 unlinked Malibu-polygon beaches → not_allowed ──

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'City of Malibu citywide rule: "Dogs are not allowed on any beach in Malibu" (FAQ). Backed by MMC §12.08.020 adopting LA County Code Title 17 Ch 17.12 beach rules.',
       ps.source_url,
       NULL
FROM public.beaches_gold g
JOIN public.jurisdictions j ON st_intersects(j.geom, g.geom)
  AND j.state IN ('California','CA') AND j.place_type='C1' AND j.name='Malibu'
CROSS JOIN public.policy_source ps
WHERE g.state='CA' AND g.is_active=true
  AND ps.source_url='https://www.malibucity.org/m/faq?cat=21#question-64'
  AND NOT EXISTS (
    SELECT 1 FROM public.beach_policy_source bps WHERE bps.beach_fid=g.fid
  )
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
