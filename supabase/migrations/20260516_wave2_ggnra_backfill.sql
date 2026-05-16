-- Wave 2 follow-up: NPS GGNRA Compendium backfill.
--
-- Per [[comprehensive-source-accelerator]]: the 2025 GOGA
-- Superintendent's Compendium (policy_source id 19) is the single
-- operative source for dog rules across all GGNRA beaches/sub-areas.
-- The 1979 Pet Policy (id 18) is the authorizing layer for
-- voice-control designations. 36 CFR §2.15 (id 1) is the federal
-- baseline. 36 CFR §7.97(d) (id 2) is the seasonal closure for
-- Crissy Field WPA + Ocean Beach SPPA snowy plover areas.
--
-- GGNRA beaches in beaches_gold (16 total — Fort Funston id 6097
-- already linked):
--   8260 Baker Beach (SF)            — voice-control per walkthrough
--   8224 China Beach (SF)            — uncertain, conservative on_leash
--   9168 Golden Gate Beach (SF)
--   8451 Kirby Beach (SF)
--   6142 Labrynth (SF)
--   6368 Mile Rock Beach (SF)
--   8226 Ocean Beach (SF)            — §7.97(d) seasonal SPPA closure
--   8725 Black Sands Beach (Marin)
--   6268 Muir Beach (Marin)
--   8674 Pirates Cove (Marin)
--   8535 Rodeo Beach (Marin)
--   8692 Slide Ranch North Beach (Marin)
--   8448 South Rodeo Beach (Marin)
--   6337 Stinson Beach (Marin)
--   8536 Tennessee Beach (Marin)
--   8446 Phillip Burton Memorial Beach (San Mateo)
--
-- Honest scope: the Compendium's voice-control designations live in
-- Exhibits #32-36 which weren't fetched during the Fort Funston
-- walkthrough. Without per-exhibit review, this migration assigns the
-- CONSERVATIVE federal baseline (rule=on_leash) to every GGNRA beach
-- and flags via status_note that voice-control is authorized at
-- specific sub-areas per the Compendium. Per-beach refinement (rule =
-- off_leash_voice_control) requires exhibit review and is deferred.
--
-- Special cases applied here:
--   - Baker Beach (8260): voice-control explicitly named in walkthrough
--   - Ocean Beach (8226): §7.97(d) seasonal SPPA closure linked

-- ─── 1. Link all GGNRA beaches to §2.15 federal baseline ────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT fid, 1, 'sand', 'on_leash', 'operative'::operative_status,
       '36 CFR §2.15: pets must be restrained on a leash not to exceed six feet in length, crated, caged, or otherwise physically confined at all times. Exception under §2.15(b) authorizes superintendent to designate areas.',
       'https://www.ecfr.gov/current/title-36/chapter-I/part-2/section-2.15',
       'Federal baseline. 1979 GGNRA Pet Policy (linked separately) authorizes voice-control at specific sub-areas per Superintendent designation; the operative voice-control area list lives in Compendium Exhibits #32-36 (deferred for per-beach review).'
  FROM (VALUES
    (8260), (8224), (9168), (8451), (6142), (6368), (8226),
    (8725), (6268), (8674), (8535), (8692), (8448), (6337), (8536),
    (8446)
  ) f(fid)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 2. Link all GGNRA beaches to 1979 Pet Policy (admin tier) ──

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT fid, 18, 'sand', 'on_leash', 'operative'::operative_status,
       'The 1979 Pet Policy designates voice-control areas within GGNRA. Specific area list lives in Compendium Exhibits #32-36.',
       'https://www.nps.gov/goga/planyourvisit/dog-friendly-areas.htm',
       NULL
  FROM (VALUES
    (8260), (8224), (9168), (8451), (6142), (6368), (8226),
    (8725), (6268), (8674), (8535), (8692), (8448), (6337), (8536),
    (8446)
  ) f(fid)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 3. Link all GGNRA beaches to 2025 Compendium (operative tier-3) ─

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT fid, 19, 'sand', 'on_leash', 'operative'::operative_status,
       '2025 GOGA Superintendent''s Compendium — annual restatement of GGNRA dog access rules, including voice-control designations per Exhibits #32-36 and seasonal closures per §7.97(d).',
       'https://www.nps.gov/goga/learn/management/upload/2025-GOGA-Compendium-Formatted.pdf',
       'Conservative on_leash baseline assigned pending exhibit-by-exhibit review. Compendium designates voice-control at specific sub-areas (Fort Funston, Baker Beach, Marin sites, Lands End, Fort Miley per walkthrough).'
  FROM (VALUES
    (8260), (8224), (9168), (8451), (6142), (6368), (8226),
    (8725), (6268), (8674), (8535), (8692), (8448), (6337), (8536),
    (8446)
  ) f(fid)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 4. Baker Beach (8260): voice-control authorized ─────────────
-- Walkthrough explicitly cites Baker Beach in the SF voice-control set.
-- Promote the Compendium row for Baker to off_leash_voice_control.

UPDATE public.beach_policy_source
   SET rule = 'off_leash_voice_control',
       evidence_verbatim = '2025 GOGA Compendium designates Baker Beach as a voice-control area authorized under the 1979 Pet Policy. Walkthrough confirmed Baker Beach is in the SF voice-control set (Exhibits #32-36).',
       status_note = 'Voice-control operative per Compendium. Federal baseline §2.15 superseded by Superintendent designation under §2.15(b).'
 WHERE beach_fid = 8260 AND policy_source_id = 19 AND section = 'sand';

-- ─── 5. Ocean Beach (8226): §7.97(d) seasonal SPPA closure ───────
-- The Snowy Plover Protection Area at Ocean Beach is a seasonal
-- closure under §7.97(d). Link as a separate beach_policy_source row.

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT 8226, 2, 'snowy_plover_protection_area', 'not_allowed',
       'operative'::operative_status,
       '36 CFR §7.97(d): Snowy Plover Protection Areas at Ocean Beach require dogs to be on leash year-round and prohibit dogs entirely in the Snowy Plover Protection Areas during the protection season (July 1 - May 15).',
       'https://www.ecfr.gov/current/title-36/chapter-I/part-7/section-7.97',
       'Seasonal SPPA closure overrides the general GGNRA voice-control allowance. Boundaries published in §7.97(d) regulatory text.'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
