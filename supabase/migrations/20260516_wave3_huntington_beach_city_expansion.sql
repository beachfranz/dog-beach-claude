-- Wave 3 expansion: City of Huntington Beach — link unlinked
-- HB-polygon beaches to existing §13.08.070 policy_source.
--
-- Existing rows (from HBDB walkthrough):
--   ps id 5: Huntington Beach Municipal Code §13.08.070 (Dogs and
--            Other Animals) — citywide default (no dogs on beach
--            sand except at the dedicated dog beach)
--   ps id 10: MOU City of HB <-> Preservation Society of Huntington
--             Dog Beach — the operator-side authorization for HBDB
--
-- 5 HB-city-polygon beaches; only Dog Beach (6212) and Sunset Beach
-- (8271) are linked. 3 unlinked beaches; link decisions:
--
--   - fid 8606 Bolsa Chica State Beach   — CA DPR state beach; HB
--     §13.08.070 doesn't govern state land. SKIPPED.
--   - fid 6172 Humboldt Beach            — within HB polygon, no
--     resolved primary authority. Link as not_allowed via §13.08.070
--     (with status_note flagging governance gap).
--   - fid 8453 Huntington State Beach    — CA DPR state beach; SKIPPED.
--
-- Net 1 new link (Humboldt Beach).

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
VALUES
  (6172, 5, 'sand', 'not_allowed', 'operative'::operative_status,
   'Huntington Beach Municipal Code §13.08.070 (Dogs and Other Animals): citywide default prohibits dogs on city beach sand. Huntington Dog Beach is the named off-leash exception (separately linked via ps 5 + ps 10).',
   (SELECT source_url FROM public.policy_source WHERE id=5),
   'Humboldt Beach is within HB city polygon but governance resolver has no precedence-1 dog_policy authority assigned; walkthrough recommended to confirm city vs state/county jurisdiction.')
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
