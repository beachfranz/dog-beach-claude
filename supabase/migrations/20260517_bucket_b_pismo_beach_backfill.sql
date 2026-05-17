-- Bucket B — City of Pismo Beach: Shell Beach (fid 5001).
--
-- Per the 2026-05-17 Wave-3 follow-up backfill pass.
--
-- Platform: amlegal (codelibrary.amlegal.com/codes/pismobeach/), slug
-- pismo_ca. Discovered via web search after parent-agency probe excluded
-- Municode (pismo_beach), ecode360 (PismoBeach), and qcode (pismobeach).
--
-- Beach-specific rule: Pismo Beach Municipal Code §12.20.030 (Chapter
-- 12.20 PUBLIC BEACH AND PIER USE REGULATIONS). The chapter declares
-- the entire publicly-owned beach and ocean area within city limits a
-- "safety zone and play area" (§12.20.010), and §12.20.030 states the
-- operative animal rule: dogs are permitted on the beach subject to the
-- citywide leash law (now §6.04.080 per the 2025 Title 6 recodification
-- by Ord. O-2025-001; the §12.20.030 text still cross-references the
-- pre-recodification cite §6.12.150, but amlegal's current Title 6 has
-- the leash text at §6.04.080).
--
-- Operative rules (verbatim):
--
-- §12.20.010 Beach safety zone—Designated:
--   "The whole of the publicly owned beach and ocean area within the
--   city limits is designated as a safety zone and play area."
--
-- §12.20.030 Beach safety zone—Animals prohibited:
--   "It is unlawful for any person to ride or lead any horse, cow,
--   donkey, mule, jackass or sheep in the safety zone and play area,
--   except as otherwise provided in this chapter. Dogs are permitted on
--   the beach subject to Section 6.12.150, Leash law." (Ord. 95-05 §2
--   (part), 1995)
--
-- §6.04.080 Dog leash law (current location of the substantive rule,
-- post Ord. O-2025-001 recodification of Title 6):
--   "A. Any person owning and/or possessing a dog must keep the animal
--   on a leash not exceeding six (6) feet in length. Further, the leash
--   must be held by a person capable of controlling such dog.
--   B. Exceptions. A dog can be unleashed under any of the following
--   circumstances: 1. When the dog is on private property of the owner
--   or of another who has consented and is either: a. Contained on the
--   enclosed property of the owner or of another who has consented; or
--   b. Is under control of a person who is in the presence of the dog;
--   or 2. When the dog is assisting a peace officer who is engaged in
--   law enforcement duties; or 3. When the dog is in designated parks,
--   as specified in a resolution of the city council."
--
-- Beach mapping:
--   fid 5001 Shell Beach (Pismo Beach city, Shell Beach neighborhood
--   coves) → on_leash per §12.20.030 + §6.04.080.
--
-- Source URL is the section-level deep link to §12.20.030 (the
-- beach-specific rule), per [[page-level-over-agency-level]] and the
-- amlegal pattern documented in [[url-resolution-field-guide]].

-- ─── 1. Insert codified-ordinance policy_source ─────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Pismo Beach Municipal Code §12.20.030 (Beach safety zone—Animals prohibited; dogs permitted on the beach subject to leash law) + §6.04.080 (Dog leash law — 6-foot leash, 2025 recodification of former §6.12.150)',
       115,
       ARRAY['dog_policy']::text[],
       'https://codelibrary.amlegal.com/codes/pismobeach/latest/pismo_ca/0-0-0-4175',
       'Pismo Beach Municipal Code Chapter 12.20 PUBLIC BEACH AND PIER USE REGULATIONS. §12.20.010 Beach safety zone—Designated: "The whole of the publicly owned beach and ocean area within the city limits is designated as a safety zone and play area." §12.20.030 Beach safety zone—Animals prohibited: "It is unlawful for any person to ride or lead any horse, cow, donkey, mule, jackass or sheep in the safety zone and play area, except as otherwise provided in this chapter. Dogs are permitted on the beach subject to Section 6.12.150, Leash law." (Ord. 95-05 §2 (part), 1995). The substantive leash rule, post the 2025 Title 6 recodification by Ord. O-2025-001, now sits at §6.04.080: "A. Any person owning and/or possessing a dog must keep the animal on a leash not exceeding six (6) feet in length. Further, the leash must be held by a person capable of controlling such dog. B. Exceptions. A dog can be unleashed under any of the following circumstances: 1. When the dog is on private property of the owner or of another who has consented and is either: a. Contained on the enclosed property of the owner or of another who has consented; or b. Is under control of a person who is in the presence of the dog; or 2. When the dog is assisting a peace officer who is engaged in law enforcement duties; or 3. When the dog is in designated parks, as specified in a resolution of the city council." Dogs are also prohibited on the Pismo Pier per §6.04.100 (formerly §12.20.180), exception for ADA service dogs. [Hosted on amlegal (codelibrary.amlegal.com/codes/pismobeach/) under slug pismo_ca. §12.20.030 deep-link = 0-0-0-4175; §6.04.080 deep-link = 0-0-0-31280 region; chapter context fetched verbatim via Playwright 2026-05-17.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Pismo Beach Municipal Code §12.20.030%'
);

-- ─── 2. Link Shell Beach (fid 5001) → on_leash ───────────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 5001, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Pismo Beach Municipal Code §12.20.030: "Dogs are permitted on the beach subject to Section 6.12.150, Leash law." The substantive 6-foot leash requirement is now codified at §6.04.080 (post 2025 Ord. O-2025-001 Title 6 recodification): "Any person owning and/or possessing a dog must keep the animal on a leash not exceeding six (6) feet in length. Further, the leash must be held by a person capable of controlling such dog." Shell Beach is the Pismo Beach city neighborhood fronting publicly-owned coastal coves — squarely within the §12.20.010 "safety zone and play area" (the whole publicly-owned beach and ocean area within city limits).',
       'https://codelibrary.amlegal.com/codes/pismobeach/latest/pismo_ca/0-0-0-4175',
       'Pismo Beach city citywide rule: dogs on-leash (6 feet max) on all publicly-owned beach + ocean area within city limits. §12.20.030 still cross-references the pre-recodification leash cite §6.12.150; the live leash text is now at §6.04.080. Walkthrough recommended to verify cove-specific signage and any seasonal carve-outs.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Pismo Beach Municipal Code §12.20.030%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
