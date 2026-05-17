-- Wave 3: City of Pacific Grove — 2 beaches; Otter Cove not_allowed
-- per City wildlife/beach page + §14.08.030. Asilomar State Beach is
-- CA DPR-governed on_leash (already linked, skipped).
--
-- Source: Pacific Grove Municipal Code Title 14 PUBLIC PROPERTY,
-- Chapter 14.08 PARKS, §14.08.030 (Dogs prohibited in public parks —
-- Exceptions). Code recently migrated from Code Publishing to ecode360
-- (custid PA4577); URL: https://ecode360.com/PA4577. Also Title 10
-- ANIMALS §10.04.020 (Running at large / leash citywide).
-- City rule page: cityofpacificgrove.gov/leisure/wildlife.php.
--
-- Operative rule (City of Pacific Grove dog policy, per cityofpacific
-- grove.gov/leisure/wildlife.php, verbatim):
--   "Dogs are prohibited on all Pacific Grove beaches (with the
--   exception of Asilomar State Beach, where they must be leashed)."
--
-- §14.08.030 (per ecode360 + web-search-confirmed sources): "Subject
-- to the exceptions set out in subsections (b) and (c) of this
-- section, it is unlawful for any person to lead or conduct any dog,
-- whether or not on a leash, within any public park."
--   Permitted on-leash exceptions: portion of George Washington Park
--   northerly of Little League baseball field; improved or natural
--   trails in the coastal zone; Caledonia Park during specific times.
--   Off-leash exceptions (sunrise-9am and 4pm-sunset): portion of
--   George Washington Park bounded by Short St, Melrose Ave, Alder St
--   and Pine Ave; Lynn "Rip" Van Winkle open space (50-yard distance).
--   Lovers Point Park and Berwick Park: NOT included in any exception
--   (no dogs on or off leash).
--
-- §10.04.020 (citywide leash): dog deemed "running at large" unless
-- led/restrained by leash not exceeding six feet attached to collar/
-- harness and actually held by a person or made fast to stationary
-- object.
--
-- 2 Pacific Grove-city-polygon beaches:
--   - fid 9302 Asilomar State Beach — CA DPR-governed, already linked
--                                     to CA State Parks on_leash policy.
--                                     SKIP — don't double-link.
--   - fid 6352 Otter Cove           — PG-jurisdiction shoreline (between
--                                     Lovers Point and Asilomar);
--                                     PG city policy prohibits dogs on
--                                     all city beaches. → not_allowed.

-- ─── 1. Insert agency_administrative_policy policy_source ────────
-- (Using agency_administrative_policy because the operative rule is
-- the city's beach-policy interpretation of §14.08.030; the codified
-- §14.08.030 covers public parks generically and the city's
-- wildlife.php page is the authoritative per-beach scope statement.)

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'City of Pacific Grove — dogs prohibited on all PG beaches except Asilomar State Beach (per City wildlife page + §14.08.030 PARKS + §10.04.020 leash law)',
       (SELECT id FROM public.agency WHERE name='Pacific Grove' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://www.cityofpacificgrove.gov/leisure/wildlife.php',
       'City of Pacific Grove dog policy (per cityofpacificgrove.gov/leisure/wildlife.php, verbatim): "Dogs are prohibited on all Pacific Grove beaches (with the exception of Asilomar State Beach, where they must be leashed)." Pacific Grove Municipal Code §10.04.020 (Running at large): unlawful for any person to suffer or permit any dog to run at large on any public street, road, alley, park, square or place, or on any vacant or unenclosed lot or land within the city; a dog is "running at large" unless led/restrained by leash not exceeding six feet attached to the dog''s collar or harness and actually held by a person or made fast to a stationary object. PG MC Title 14 PUBLIC PROPERTY, Chapter 14.08 PARKS, §14.08.030 Dogs prohibited in public parks — Exceptions: "Subject to the exceptions set out in subsections (b) and (c) of this section, it is unlawful for any person to lead or conduct any dog, whether or not on a leash, within any public park." On-leash exceptions: portion of George Washington Park northerly of Little League baseball field; improved or natural trails in the coastal zone; Caledonia Park during specific times (Ord. 22-007 Caledonia Park exception). Off-leash exceptions (sunrise-9am and 4pm-sunset): portion of George Washington Park (bounded by Short St, Melrose Ave, Alder St, Pine Ave); Lynn "Rip" Van Winkle open space (with 50-yard distance constraint). Lovers Point Park and Berwick Park: no dogs (on or off leash). Asilomar State Beach is CA DPR-governed (leashed dogs permitted on beach and coast trail). Pacific Grove Municipal Code recently migrated from Code Publishing to ecode360 (custid PA4577) at ecode360.com/PA4577. [Fetched verbatim 2026-05-16; ecode360 SPA contents corroborated via City wildlife page + web search.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE source_url='https://www.cityofpacificgrove.gov/leisure/wildlife.php'
);

-- ─── 2. Otter Cove → not_allowed ────────────────────────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 6352, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'City of Pacific Grove dog policy: "Dogs are prohibited on all Pacific Grove beaches (with the exception of Asilomar State Beach, where they must be leashed)." Otter Cove is a PG-jurisdiction shoreline (between Lovers Point and Asilomar), not Asilomar SB. PG MC §14.08.030 prohibits dogs in public parks; PG coastal beaches fall under city policy.',
       ps.source_url,
       'City policy supplements existing Monterey County on_leash link; PG city-specific rule is more restrictive and operative for PG beaches.'
FROM public.policy_source ps
WHERE ps.source_url='https://www.cityofpacificgrove.gov/leisure/wildlife.php'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
