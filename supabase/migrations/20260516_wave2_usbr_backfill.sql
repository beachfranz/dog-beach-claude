-- Wave 2 follow-up: USBR (Bureau of Reclamation) backfill via
-- 43 CFR §423.35.
--
-- Per [[comprehensive-source-accelerator]]: USBR's federal animal
-- rule at 43 CFR §423.35 is mostly silent on leash specifics and
-- (importantly) §423.35(f) DEFERS to state/local managing agencies
-- when USBR lands are operated through agreements. So §423.35 is
-- the tier-1 federal authority, but operative dog rules at USBR
-- beaches typically come from the state/local managing partner.
--
-- 6 USBR California beaches in beaches_gold:
--   6007 South Beach (Merced)
--   6117 Sneaker Beach (Riverside)
--   6216 Two Beaches (San Bernardino)
--   9565 Laurel Beach (Sacramento)
--   9566 Horse Shoe Hill Shore (Sacramento)
--   9567 Moot Rock Beach (Sacramento)
--
-- Default rule assignment: sand=on_leash with status_note flagging
-- §423.35(f) deferral — actual operative rules require per-beach
-- managing-partner identification.

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'federal_regulation',
       '43 CFR §423.35 (Bureau of Reclamation — Animals)',
       (SELECT id FROM public.agency WHERE name='United States Bureau of Reclamation' AND type='federal_department'),
       ARRAY['dog_policy']::text[],
       'https://www.ecfr.gov/current/title-43/subtitle-A/part-423/subpart-C/section-423.35',
       '(a) You must not bring pets or other animals into public buildings, public transportation vehicles, or sanitary facilities. This provision does not apply to properly trained animals assisting persons with disabilities, such as seeing-eye dogs. (b) You must not abandon any animal on Reclamation facilities, lands, or waterbodies, or harass, endanger, or attempt to collect any animal except game you are attempting to take in the course of authorized hunting, fishing, or trapping. ... (f) Where recreation facilities or other areas of Reclamation lands and waterbodies are operated through a contract or other agreement by a managing recreation partner of another Federal, State, local, or Tribal governmental entity, such as a State park, the laws, ordinances, and regulations of those partner agencies pertaining to animals and pets shall be enforced by those partner agencies. [43 CFR Part 423 Subpart C, §423.35.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE '43 CFR §423.35%'
);

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE '43 CFR §423.35%'),
       'sand', 'on_leash', 'operative'::operative_status,
       '43 CFR §423.35 — federal baseline. Per §423.35(f), state/local managing partners'' animal rules govern when USBR lands are operated under agreement. Default conservative leashed-allowed assignment pending managing-partner identification.',
       'https://www.ecfr.gov/current/title-43/subtitle-A/part-423/subpart-C/section-423.35',
       'USBR §423.35 is mostly silent on leash specifics; §423.35(f) defers to managing partner. Per-beach managing-partner identification deferred.'
  FROM (VALUES (6007), (6117), (6216), (9565), (9566), (9567)) f(fid)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
