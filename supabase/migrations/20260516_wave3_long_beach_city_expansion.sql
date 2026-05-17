-- Wave 3 expansion: City of Long Beach — link 5 more beaches to
-- existing Long Beach policy_source rows.
--
-- Existing rows (from Rosie's walkthrough, prior commits):
--   ps id 7: Long Beach Municipal Code §6.16.310 (Dog exercise area on
--            the beach) — the Rosie's Dog Beach off-leash designation
--   ps id 8: Long Beach Municipal Code §6.16.090 (Dogs prohibited on
--            beaches or schoolgrounds) + §6.16.100 (Dog leash required)
--            — the citywide default
--
-- 6 Long Beach-city-polygon beaches in beaches_gold; only Rosie's Dog
-- Beach (6411) is linked so far. The other 5 are unlinked and get the
-- §6.16.090 default → not_allowed:
--
--   - fid 8269 Alamitos Bay Beach
--   - fid 6265 Bayshore Beach
--   - fid 8726 Long Beach City Beach
--   - fid 9625 Mother's Beach   (kid-friendly enclosed swim area; no dogs)
--   - fid 9626 Peninsula Beach
--
-- All 5 → not_allowed via existing ps id 8 (§6.16.090).

-- ─── Link 5 unlinked LB beaches → not_allowed via ps id 8 ────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid, 8, 'sand', 'not_allowed', 'operative'::operative_status,
       'Long Beach Municipal Code §6.16.090: dogs prohibited on Long Beach city beaches (Rosie''s Dog Beach is the named off-leash exception, separately linked via ps id 7).',
       (SELECT source_url FROM public.policy_source WHERE id=8),
       v.note
FROM (values
  (8269, NULL::text),
  (6265, NULL::text),
  (8726, NULL::text),
  (9625, 'Mother''s Beach is a kid-friendly enclosed swim area at Alamitos Bay; no dogs.'),
  (9626, NULL::text)
) as v(fid, note)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
