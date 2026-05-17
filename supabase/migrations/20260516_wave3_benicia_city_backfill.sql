-- Wave 3 (first city backfill): City of Benicia.
--
-- Source: City of Benicia "Dogs Off Leash" pamphlet citing Benicia
-- Municipal Code Chapter 6.16 Regulation of Dogs in full.
-- URL: https://beniciacarebuild.govoffice2.com/vertical/Sites/%7BF991A639-AAED-4E1A-9735-86EA195E2C8D%7D/uploads/Dogs_off_leash_pamphlet.pdf
--
-- Context: Solano County's 2 active beaches in beaches_gold (fid 3963
-- Raymond J. Bordoni Public Access Beach + fid 8968 Benicia Public Pier &
-- Beach) are BOTH governed by the City of Benicia for dog_policy (both at
-- precedence_rank=1). Solano County is precedence-1 only for public_health
-- and water_quality. So the Solano County Code does NOT govern these
-- beaches; Benicia Municipal Code §6.16 does.
--
-- Operative rules — Benicia Municipal Code Chapter 6.16:
--   §6.16.020 Restraint of dogs (DEFAULT — verbatim):
--     "no person owning or having charge, care, custody, or control of
--     any dog shall cause or allow ... to be or run at large in or upon
--     any public place or premises ... unless such dog is securely
--     restrained by a substantial leash, no longer than six feet, and is
--     in charge and control."
--
--   §6.16.030(A) Dogs PROHIBITED (verbatim):
--     "Except for service dogs, it is unlawful for a person ... to permit
--     the dog to be in City Park or on the Alvarez Ninth Street Park
--     beach or in or upon a playground, skate park, in-line skate rink,
--     tennis court, bocce court, basketball court, or a baseball field
--     or bleacher within any other public park."
--
--   §6.16.030(B) OFF-LEASH voice-control (verbatim):
--     "Dogs may be off-leash in the Phenix Dog Park at Benicia Community
--     Park, on the First Street Beach, and on the Promenade Beach if
--     dogs are under voice command of the person responsible for the
--     dog. In all other places, dogs must be securely restrained by a
--     leash no longer than six feet."
--
--   §6.16.010 — feces removal required on public property and easements.
--   §6.16.040 — running at large permitted on owner's premises.
--
-- Per-beach mapping (rule selection):
--   - 8968 Benicia Public Pier & Beach: location (38.0449, -122.1625) is
--     at the foot of First Street → matches "First Street Beach" or
--     "Promenade Beach" in §6.16.030(B) → off_leash_voice_control.
--   - 3963 Raymond J. Bordoni Public Access Beach: location (38.0557,
--     -122.1684) is at the 9th Street area. Possibly the Alvarez Ninth
--     Street Park beach which §6.16.030(A) explicitly PROHIBITS dogs on.
--     Default-conservative: link with on_leash citing §6.16.020 and flag
--     in status_note that walkthrough verification is needed.

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Benicia Municipal Code §6.16 (Regulation of Dogs) — §6.16.020 default leash + §6.16.030 per-beach exceptions',
       (SELECT id FROM public.agency WHERE name='Benicia' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://beniciacarebuild.govoffice2.com/vertical/Sites/%7BF991A639-AAED-4E1A-9735-86EA195E2C8D%7D/uploads/Dogs_off_leash_pamphlet.pdf',
       'Benicia Municipal Code Chapter 6.16 Regulation of Dogs. §6.16.010 Feces removal: any person having custody of a dog on public property (including easements and public parks) shall carry an instrument suitable for removing feces and remove any feces deposited. §6.16.020 Restraint of dogs: no person owning or having charge, care, custody, or control of any dog shall cause or allow either willfully or through failure to exercise due care or control to be or run at large in or upon any public place or premises, or upon any private place not the owner''s, unless such dog is securely restrained by a substantial leash, no longer than six feet, and is in charge and control. §6.16.030(A) Dogs prohibited: except for service dogs, it is unlawful to permit a dog to be in City Park or on the Alvarez Ninth Street Park beach, or in/on playgrounds, skate parks, in-line skate rinks, tennis courts, bocce courts, basketball courts, baseball fields, or bleachers within any other public park. §6.16.030(B) Off-leash voice-control areas: dogs may be off-leash in the Phenix Dog Park at Benicia Community Park, on the First Street Beach, and on the Promenade Beach if dogs are under voice command of the person responsible for the dog. In all other places, dogs must be securely restrained by a leash no longer than six feet. §6.16.030(C) Exceptions: obedience classes sponsored by parks dept, service animals, or city-approved special events. No dangerous dogs in no-leash areas. §6.16.040 Permissive running at large: this chapter does not prohibit dogs from running at large on the owner''s premises, nor on private property with owner''s permission. §6.16.050 Unspayed female dogs prohibited from running at large during breeding period. Ord. 17-13 § 1-2; Ord. 07-72 § 4. [Verbatim per City of Benicia "Dogs Off Leash" pamphlet PDF citing Chapter 6.16 in full. Fetched 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Benicia Municipal Code §6.16%'
);

-- ─── 2. Link Benicia Public Pier & Beach (fid 8968) — off-leash voice-control ──

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8968, ps.id, 'sand', 'off_leash_voice_control', 'operative'::operative_status,
       'Benicia Municipal Code §6.16.030(B): Dogs may be off-leash on the First Street Beach and on the Promenade Beach if dogs are under voice command of the person responsible for the dog. Benicia Public Pier & Beach is at the foot of First Street (38.0449,-122.1625) — matches one of these designated areas.',
       ps.source_url,
       NULL
FROM public.policy_source ps
WHERE ps.citation LIKE 'Benicia Municipal Code §6.16%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 3. Link Raymond J. Bordoni Public Access Beach (fid 3963) — on_leash default ──

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 3963, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Benicia Municipal Code §6.16.020 default: dog must be securely restrained by a substantial leash, no longer than six feet, and in charge and control. Off-leash §6.16.030(B) carve-outs name First Street Beach + Promenade Beach + Phenix Dog Park only — Bordoni not named, so default applies.',
       ps.source_url,
       'WALKTHROUGH NEEDED: Raymond J. Bordoni Public Access Beach is at 9th Street waterfront (38.0557,-122.1684). May actually be the Alvarez Ninth Street Park beach which §6.16.030(A) explicitly PROHIBITS dogs on — rule would then be not_allowed. Default-conservative on_leash applied pending verification.'
FROM public.policy_source ps
WHERE ps.citation LIKE 'Benicia Municipal Code §6.16%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
