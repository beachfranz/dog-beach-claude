-- Phase I2 — hand-backfill of region_name on bps for the 13 known
-- sub-area / multi-zone CA beaches.
--
-- Per docs/consensus_phase_i_subarea_spec.md §I2. Schema column added in
-- the sibling 20260517_phase_i1_region_name.sql migration.
--
-- This migration ONLY sets region_name on rows that authoritative
-- sources already establish as the off-leash carve-out (or rolls back
-- the citywide leash baseline as "default region" by leaving NULL).
--
-- Defer-rather-than-fabricate: where the existing bps state does not
-- yet contain a row that captures the sub-area split (i.e., the
-- off-leash carve-out has never been authority-cited), we DEFER with a
-- header note. Phase I2 is a hand-backfill of existing structure only;
-- adding net-new authority rows for carve-outs without a quoted source
-- is out of scope here and will happen via Sonnet extraction (I2b) or
-- per-jurisdiction walkthroughs.
--
-- Per-beach decisions (13 candidates from spec §"Known candidate
-- beaches for I2 hand-backfill"):
--
--   6202 Coronado Dog Beach   — UPDATE: ps 50, 51 (off_leash) →
--                                "Sand Pebble Off-Leash Area".
--                                Authority: Coronado MC Ch 32.08 + City
--                                of Coronado Dogs page name the
--                                designated off-leash beach (Sand
--                                Pebble area at the far-north end). SD
--                                County baseline rows (ps 116, 117)
--                                remain NULL (= default region).
--
--   6622 Carlsbad Lagoon DB   — UPDATE: ps 139 (off_leash) →
--                                "Designated Dog Beach (Agua Hedionda
--                                Lagoon)". Authority: Carlsbad MC
--                                §11.32.030(11)(b) names the area
--                                specifically designated for dog use.
--                                SD County baselines (116, 117) → NULL.
--
--   8452 Crown Memorial       — DEFER. Existing bps state has no row
--                                for Crab Cove off-leash (CDFW MPA +
--                                EBRPD operate Crab Cove as a marine
--                                reserve where dogs are largely
--                                prohibited on the beach itself). Per
--                                walkthrough #4 the Crab Cove sub-area
--                                overlay is a CDFW MPA overlay, not an
--                                EBRPD off-leash designation as the
--                                spec hypothesized. Need authority
--                                lookup (EBRPD Ordinance 38 §501 +
--                                CDFW MPA regs) before encoding. NO
--                                CHANGE this migration.
--
--   8226 Ocean Beach SF       — UPDATE: ps 19 (sand) → "South of Sloat
--                                Blvd (DPR/GGNRA on-leash baseline)"
--                                AND keep ps 2 plover row at default
--                                region (it's a year-round overlay).
--                                The "North of Sloat (GGNRA voice
--                                control)" carve-out has NO existing
--                                bps row that asserts off-leash; the
--                                three existing sand rows (1, 18, 19)
--                                are all on_leash. Per spec the 1979
--                                GGNRA Pet Policy + 2025 Compendium
--                                designate the area north of Sloat as
--                                voice-control, but the current bps
--                                rows don't reflect that. DEFER the
--                                north-of-Sloat off-leash row to the
--                                I2b extractor pass / walkthrough; it
--                                requires authority repair (changing
--                                rule from on_leash → off_leash_vc),
--                                which is beyond the region_name
--                                hand-backfill scope. Leave existing
--                                rows NULL.
--
--   6337 Stinson Beach        — DEFER. Same shape as OB SF: existing
--                                rows are all on_leash (ps 1, 18, 19).
--                                The Marin County strip (Upton Beach,
--                                north end) historically allows dogs
--                                under Marin County Code §8.04.175,
--                                but no bps row currently encodes
--                                that authority for this fid. Region
--                                split requires adding the Marin County
--                                authority row first. NO CHANGE this
--                                migration; pin in I2b queue.
--
--   6411 Rosie's Dog Beach    — UPDATE: ps 7 (off_leash, sand) + ps 22
--                                (off_leash_voice_control, sand) →
--                                "Off-leash zone (Granada to
--                                Roycroft)". Authority: LBMC §6.16.310
--                                names the bounded zone (Granada Ave
--                                eastern bound to Roycroft Ave western
--                                bound). The ps 22 parking_lot row
--                                stays NULL (default region).
--
--   8560 Del Mar Dog Beach    — UPDATE: ps 199 (off_leash_voice_control,
--                                sand) → "North Beach (north of 29th
--                                Street)". Authority: Del Mar MC
--                                §4.08.020(B)(1) explicitly bounds the
--                                off-leash zone to "beaches or waters
--                                located northerly of a line being an
--                                extension of 29th Street". No
--                                existing baseline row for "south of
--                                29th"; that on-leash main-beach
--                                baseline is implicit in the citywide
--                                §4.08.020(A) leash rule but not yet
--                                in bps. Pin baseline-row capture for
--                                follow-up.
--
--   9716 Fiesta Island        — UPDATE: ps 156 (off_leash, sand) →
--                                "Off-leash dog area". Authority: City
--                                of SD Parks & Recreation Dogs at
--                                Beaches page names Fiesta Island
--                                off-leash, daily 4am-10pm, with
--                                seasonal Apr 15-Sep 15 nesting buffer
--                                closures. The "rest of island
--                                (on-leash)" baseline isn't in bps;
--                                pin for follow-up.
--
--   8358 Dog Beach SD (OB)    — NO CHANGE. Single-zone; the entire
--                                named beach IS the off-leash carve-out
--                                per City of SD Parks page (ps 156).
--                                No sub-area split to encode.
--
--   6097 Fort Funston         — NO CHANGE. Single primary zone
--                                (voice-control sand per GOGA
--                                Compendium ps 19) with a federal
--                                overlay (bank_swallow_closure
--                                not_allowed) already captured as a
--                                distinct section value. The on_leash
--                                row (ps 1, 36 CFR §2.15 baseline)
--                                serves as the federal default; no
--                                region rename needed.
--
--   8635 East Beach SF        — NO CHANGE. Single off_leash_voice_control
--                                zone per GOGA Compendium; whole beach
--                                is the carve-out.
--
--   8968 Benicia Pier & Beach — NO CHANGE. Benicia MC §6.16.030(B)
--                                names BOTH First Street Beach and the
--                                Promenade Beach as off-leash; the
--                                single bps row covers both. No
--                                sub-area split.
--
--   6212 HBDB                 — LEAVE ALONE per spec + memory rule
--                                (manual_curator protected).
--
-- All UPDATEs are guarded with WHERE clauses that target only the
-- specific (beach_fid, policy_source_id, section) tuples named above,
-- so re-running this migration is idempotent (a no-op if region_name
-- already matches).

BEGIN;

-- ─── 6202 Coronado Dog Beach ─────────────────────────────────────
-- Authority: Coronado MC Ch 32.08 (citywide leash) + named carve-out
-- at the Sand Pebble area (north of the Hotel del Coronado, between
-- the hotel grounds and Naval Air Station North Island).
UPDATE public.beach_policy_source
   SET region_name = 'Sand Pebble Off-Leash Area'
 WHERE beach_fid = 6202
   AND policy_source_id IN (50, 51)
   AND section = 'sand'
   AND rule = 'off_leash';

-- ─── 6622 Carlsbad Lagoon Dog Beach ──────────────────────────────
-- Authority: Carlsbad MC §11.32.030(11) prohibits dogs on beaches
-- EXCEPT (b) "any area specifically designated for dog use by the
-- City Council" — Agua Hedionda Lagoon Dog Beach is that designated
-- area.
UPDATE public.beach_policy_source
   SET region_name = 'Designated Dog Beach (Agua Hedionda Lagoon)'
 WHERE beach_fid = 6622
   AND policy_source_id = 139
   AND section = 'sand'
   AND rule = 'off_leash';

-- ─── 6411 Rosie's Dog Beach ──────────────────────────────────────
-- Authority: LBMC §6.16.310 bounds the dog exercise area "between
-- Granada Avenue (eastern bound) and Roycroft Avenue (western bound)"
-- — these named cross-streets define the zone within the larger
-- Long Beach shoreline.
UPDATE public.beach_policy_source
   SET region_name = 'Off-leash zone (Granada to Roycroft)'
 WHERE beach_fid = 6411
   AND policy_source_id IN (7, 22)
   AND section = 'sand'
   AND rule IN ('off_leash', 'off_leash_voice_control');

-- ─── 8560 Del Mar Dog Beach ──────────────────────────────────────
-- Authority: Del Mar MC §4.08.020(B)(1) bounds the off-leash window
-- to "beaches or waters located northerly of a line being an
-- extension of 29th Street" (the North Beach strip), seasonally
-- (day after Labor Day - June 15) and year-round dawn-to-8am.
UPDATE public.beach_policy_source
   SET region_name = 'North Beach (north of 29th Street)'
 WHERE beach_fid = 8560
   AND policy_source_id = 199
   AND section = 'sand'
   AND rule = 'off_leash_voice_control';

-- ─── 9716 Fiesta Island ──────────────────────────────────────────
-- Authority: City of SD Parks & Recreation Dogs at Beaches page
-- names Fiesta Island as off-leash (daily 4am-10pm); the carve-out
-- excludes the Youth Aquatic Center and seasonal nesting buffer
-- zones (Apr 15 - Sep 15).
UPDATE public.beach_policy_source
   SET region_name = 'Off-leash dog area'
 WHERE beach_fid = 9716
   AND policy_source_id = 156
   AND section = 'sand'
   AND rule = 'off_leash';

COMMIT;
