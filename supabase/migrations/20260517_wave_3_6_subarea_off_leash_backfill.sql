-- Wave 3.6 — chase the 3 beaches Agent A deferred from Phase I2 because
-- they needed NEW authority encoding for off-leash sub-areas (not just
-- a region_name rename of existing bps rows).
--
-- Per docs/consensus_phase_i_subarea_spec.md §I2 and the I2 backfill
-- header (20260517_phase_i2_subarea_backfill.sql): three beaches in the
-- Phase I candidate list were deferred because their existing bps state
-- had no row asserting off-leash for the named sub-area. This migration
-- closes the gap (or formally re-defers with primary-source citations).
--
-- ─────────────────────────────────────────────────────────────────
-- TL;DR per beach
--
--   8226 Ocean Beach SF        — ACTION. 2025 GOGA Compendium pp.15-16
--                                (verbatim captured below) designates
--                                Ocean Beach as a Voice Control area
--                                under the 1979 Pet Policy, excluding
--                                the Snowy Plover Protection Area's
--                                seasonal leash restriction (Stairwell
--                                21 south to Sloat Blvd, Jul 1 - May 15)
--                                already encoded via §7.97(d). The
--                                existing bps row (8226, 19, 'sand',
--                                on_leash) UNDER-states access; flip
--                                to off_leash_voice_control mirroring
--                                the Baker Beach (8260) precedent in
--                                20260516_wave2_ggnra_backfill.sql §4.
--                                Pet Policy 1979 row (ps 18) also
--                                flips. §2.15 federal baseline row
--                                (ps 1) stays on_leash (superseded by
--                                Superintendent designation under
--                                §2.15(b)); region_name=NULL keeps it
--                                as default region. SPPA row (ps 2,
--                                section='snowy_plover_protection_area',
--                                not_allowed) is already a distinct
--                                section; leave alone.
--
--   8452 Crown Memorial        — DEFER (FORMAL). The brief hypothesis
--        (Crab Cove)             that EBRPD Ordinance 38 designates
--                                Crab Cove off-leash is contradicted
--                                by primary sources:
--                                 (a) parks.ca.gov/?page_id=526
--                                     (verbatim re-fetched 2026-05-17):
--                                     "Yes: Dogs allowed only on lawn
--                                     areas and along the paved
--                                     pathways. Dogs not allowed on
--                                     the beach." + "At the north end
--                                     of the beach, Crab Cove is a
--                                     marine reserve."
--                                 (b) walkthrough_crown_memorial.md
--                                     §5 RESOLVED: DPR's policy
--                                     governs at Crown Memorial; EBRPD
--                                     is operator only and Ordinance
--                                     38 is NOT operative for
--                                     dog_policy on DPR-owned land
--                                     (bounded-operator principle per
--                                     [[operator-not-pseudo-agency]]).
--                                Crab Cove is a CDFW marine reserve
--                                (sub-area MPA overlay), NOT an EBRPD
--                                off-leash designation. The MPA does
--                                not authorize dog access; DPR's
--                                beach prohibition (ps 16, already in
--                                bps as section='sand',
--                                rule='not_allowed') is operative
--                                across the whole beach, Crab Cove
--                                included. No off-leash bps row to
--                                write.
--
--   6337 Stinson Beach         — DEFER (FORMAL). The brief hypothesis
--                                that a Marin County strip is
--                                off-leash is partially correct, but
--                                fid 6337's coordinates (37.8991,
--                                -122.6458) sit squarely in the
--                                NPS-administered Stinson Beach (south
--                                of Bolinas Lagoon), not Upton Beach
--                                (the Marin County strip north of the
--                                lagoon). 2025 GOGA Compendium p.24
--                                (verbatim): "STINSON BEACH (Exhibit
--                                #38) All Stinson Beach areas, except
--                                on leash dog walking is allowed in
--                                parking and picnic areas." That's a
--                                full beach prohibition for the
--                                NPS-administered area, with on-leash
--                                only in parking + picnic areas (NOT
--                                on the sand). Existing bps rows
--                                (ps 1, 18, 19, all on_leash) actually
--                                OVER-state access; the correct
--                                operative rule for the NPS sand at
--                                this fid is `not_allowed`. That
--                                correction is a separate rule
--                                downgrade outside this migration's
--                                off-leash-backfill scope and should
--                                be handled in a follow-up GGNRA
--                                refinement pass. Per defer rubric:
--                                community-attestation off-leash use
--                                ≠ codified authority; don't fabricate.
--                                Pin: 6337_stinson_rule_correction in
--                                governance-resolver-followups.
--
-- ─────────────────────────────────────────────────────────────────
-- Source authority — Ocean Beach SF (the one action)
--
--   PRIMARY: 2025 GOGA Superintendent's Compendium (policy_source id 19)
--     URL: https://www.nps.gov/goga/learn/management/upload/2025-GOGA-Compendium-Formatted.pdf
--     Verbatim (p.15-16, "VOICE CONTROL DOG WALKING*" section):
--       "The 1979 Pet Policy allows for Managed Dogs, leashed or under
--       Voice Control, in the following areas:
--         San Francisco (Exhibits #32-36)
--         ...
--         • Ocean Beach, excluding Snowy Plover Protection Area seasonal
--           leash restriction. (For on-leash restriction from Stairwell
--           21 south to Sloat Boulevard from July1 - May 15, see
--           weblink below)"
--       "** For information about the seasonal on leash requirements at
--       Ocean Beach and Crissy Field, see the 2008 Special Regulation
--       codified at 36 CFR 7.97(d)."
--     Definition (p.4): "VOICE CONTROL means dogs are within earshot
--     and eyesight of their owner/handler and respond immediately to
--     commands to return to leash when called."
--
--   AUTHORIZING LAYER: 1979 GGNRA Pet Policy (policy_source id 18)
--     "The 1979 Pet Policy designates voice-control areas within
--     GGNRA. Specific area list lives in Compendium Exhibits #32-36."
--     (Exhibit #36 is Ocean Beach.)
--
--   FEDERAL BASELINE (unchanged): 36 CFR §2.15 (policy_source id 1)
--     §2.15(a)(2) baseline = on-leash; §2.15(b) authorizes Superintendent
--     to designate areas for off-leash. The Compendium IS that
--     designation. Region: default (NULL).
--
--   SEASONAL OVERLAY (unchanged): 36 CFR §7.97(d) (policy_source id 2)
--     SPPA closure encoded as a distinct section
--     ('snowy_plover_protection_area', not_allowed). No change.
--
-- ─────────────────────────────────────────────────────────────────
-- Quality gates run before commit:
--   (8a) Red-flag URL: no new ps rows in this migration; existing
--        source_urls are already deep-linked (nps.gov compendium PDF).
--   (8c) Agency lookup: National Park Service (id 307) verified.
--   (8d) Homogeneity: only 1 beach affected (8226); no batch reuse risk.
--
-- Migration is idempotent: all changes are UPDATEs guarded by exact
-- (beach_fid, policy_source_id, section) tuples + a WHERE on the
-- pre-flip rule value, so re-running is a no-op if already applied.

BEGIN;

-- ─── 8226 Ocean Beach SF — flip Compendium row to voice-control ──
-- Mirrors the Baker Beach (8260) precedent in
-- 20260516_wave2_ggnra_backfill.sql §4. The Compendium pp.15-16
-- explicitly lists Ocean Beach in the Voice Control area set.

UPDATE public.beach_policy_source
   SET rule = 'off_leash_voice_control',
       evidence_verbatim = '2025 GOGA Superintendent''s Compendium pp.15-16 designates Ocean Beach as a Voice Control area authorized under the 1979 Pet Policy: "The 1979 Pet Policy allows for Managed Dogs, leashed or under Voice Control, in the following areas: ... San Francisco (Exhibits #32-36) ... Ocean Beach, excluding Snowy Plover Protection Area seasonal leash restriction." Voice control defined p.4: dogs within earshot and eyesight, respond immediately to commands. Federal §2.15 baseline superseded by Superintendent designation under §2.15(b). Seasonal SPPA restriction (Stairwell 21 south to Sloat Blvd, Jul 1 - May 15) encoded separately via 36 CFR §7.97(d) (ps 2).',
       evidence_url = 'https://www.nps.gov/goga/learn/management/upload/2025-GOGA-Compendium-Formatted.pdf',
       region_name = 'Voice-control area (excluding seasonal Snowy Plover Protection Area)',
       status_note = 'Voice-control operative per 2025 Compendium. SPPA seasonal overlay (Stairwell 21 south to Sloat Blvd, Jul 1 - May 15) encoded as distinct section via §7.97(d). Federal baseline §2.15 superseded by Superintendent designation under §2.15(b).'
 WHERE beach_fid = 8226
   AND policy_source_id = 19
   AND section = 'sand'
   AND rule = 'on_leash';

-- Pet Policy 1979 (ps 18) — same flip, this is the authorizing layer
UPDATE public.beach_policy_source
   SET rule = 'off_leash_voice_control',
       evidence_verbatim = 'The 1979 Pet Policy authorizes voice-control dog walking at designated GGNRA areas under Superintendent designation. Ocean Beach is in the SF voice-control set per 2025 Compendium Exhibits #32-36 (Exhibit #36 is Ocean Beach).',
       evidence_url = 'https://www.nps.gov/goga/planyourvisit/dog-friendly-areas.htm',
       region_name = 'Voice-control area (excluding seasonal Snowy Plover Protection Area)',
       status_note = 'Authorizing layer for the Compendium''s voice-control designation. Operative scope at Ocean Beach: whole beach, minus seasonal SPPA (Stairwell 21 south to Sloat Blvd, Jul 1 - May 15).'
 WHERE beach_fid = 8226
   AND policy_source_id = 18
   AND section = 'sand'
   AND rule = 'on_leash';

-- §2.15 federal baseline (ps 1) — keep on_leash; this is the legal
-- default the Superintendent's designation supersedes. Mark region as
-- default (NULL) so the I3 promoter treats it as the layered baseline,
-- not a competing zone.
UPDATE public.beach_policy_source
   SET status_note = 'Federal baseline (36 CFR §2.15). Superseded at Ocean Beach by Superintendent voice-control designation under §2.15(b), encoded via ps 18 + 19. SPPA seasonal restriction (§7.97(d), ps 2) overrides voice-control Jul 1 - May 15 in the Stairwell 21 - Sloat Blvd zone.'
 WHERE beach_fid = 8226
   AND policy_source_id = 1
   AND section = 'sand'
   AND status_note IS NULL;

-- ─── 8452 Crown Memorial — DEFER (no changes) ────────────────────
-- See header for rationale. Existing bps state is correct: DPR
-- ps 16 (not_allowed on sand, on_leash on paved_paths) + Alameda
-- County ps 114 (on_leash) are operative. EBRPD Ordinance 38 (ps 9)
-- is NOT linked to fid 8452 and should not be — per
-- walkthrough_crown_memorial.md §5 RESOLVED, DPR's policy governs.

-- ─── 6337 Stinson Beach — DEFER (no changes) ─────────────────────
-- See header. Existing bps rows actually OVER-state access (on_leash
-- vs. compendium's "all areas closed except parking + picnic"). The
-- correction is a rule downgrade (not_allowed on sand) outside this
-- migration's scope. No off-leash row to add because the off-leash
-- portion is Upton Beach (Marin County, separate physical strip; not
-- this fid).

COMMIT;
