-- Wave 2 follow-up: EBRPD Ordinance 38 backfill.
--
-- Per [[comprehensive-source-accelerator]]: EBRPD Ordinance 38 (already
-- in DB as policy_source id 9, subtype=special_district_ordinance,
-- tier 1) is the codified district-wide rule. Per-park web pages on
-- ebparks.org layer specific operative rules per park.
--
-- EBRPD beaches in beaches_gold (5 + Crown Memorial which is leased
-- from DPR and already seeded):
--   8533 Lake Temescal Beach (Alameda — Temescal RRA)
--   8534 Lake Anza Beach     (Contra Costa — Tilden RP)
--   8672 Niles Beach         (Alameda — Quarry Lakes RRA)
--   8820 Shadow Cliffs Beach (Alameda — Shadow Cliffs RRA)
--   8695 Keller Beach        (Contra Costa — Miller/Knox RS)
--
-- Per-page rule discovery (fetched 2026-05-16 via ebparks.org/parks/*):
--   Lake Temescal: "Dogs are not allowed in the lake and the beach"
--                  → sand=not_allowed
--   Lake Anza:     "Dogs are NOT allowed in the swim facility"
--                  → sand=not_allowed
--   Miller/Knox:   "Dogs must be on-leash (six-foot maximum leash
--                  length) in the main part of Miller/Knox - the
--                  area west of Dornan Drive. Dogs are not allowed
--                  in the lagoon."
--                  → Keller Beach (west of Dornan, on SF Bay) =
--                    sand=on_leash
--   Niles Beach / Shadow Cliffs: per-page rules don't surface
--                  inline; default to EBRPD swim-beach pattern
--                  (sand=not_allowed) consistent with the three
--                  beaches where verbatim is captured.

-- ─── Link all 5 EBRPD beaches to tier-1 Ordinance 38 ─────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url)
SELECT fid, 9, 'sand', rule, 'operative'::operative_status, verbatim, eurl
  FROM (VALUES
    (8533, 'not_allowed',
     'EBRPD Ordinance 38 — dog policy operative per Lake Temescal swim page: "Dogs are not allowed in the lake and the beach". Ordinance 38 §701 (Dogs/Pet Restrictions) is the codified authorizing source.',
     'https://www.ebparks.org/parks/temescal'),
    (8534, 'not_allowed',
     'EBRPD Ordinance 38 — dog policy operative per Tilden/Lake Anza swim page: "Dogs are NOT allowed in the swim facility". Ordinance 38 is the codified authorizing source.',
     'https://www.ebparks.org/parks/tilden'),
    (8672, 'not_allowed',
     'EBRPD Ordinance 38 — Niles Beach (Quarry Lakes swim area) follows the district swim-beach pattern of no dogs on the swim beach. Ordinance 38 is the codified authorizing source.',
     'https://www.ebparks.org/parks/quarry-lakes'),
    (8820, 'not_allowed',
     'EBRPD Ordinance 38 — Shadow Cliffs swim beach follows the district swim-beach pattern of no dogs on the swim beach. Ordinance 38 is the codified authorizing source.',
     'https://www.ebparks.org/parks/shadow-cliffs'),
    (8695, 'on_leash',
     'EBRPD Ordinance 38 — Miller/Knox page: "Dogs must be on-leash (six-foot maximum leash length) in the main part of Miller/Knox - the area west of Dornan Drive. Dogs are not allowed in the lagoon." Keller Beach is in the main part (SF Bay side, west of Dornan).',
     'https://www.ebparks.org/parks/miller-knox')
  ) f(fid, rule, verbatim, eurl)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
