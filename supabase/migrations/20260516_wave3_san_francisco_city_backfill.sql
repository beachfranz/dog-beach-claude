-- Wave 3: City and County of San Francisco — link 4 unlinked beaches
-- per their jurisdictional bucket (SF city vs federal GGNRA).
--
-- Sources:
--   - SF Park Code + SF Police Code §41.12 (Duties of Owners or
--     Guardians) hosted on amlegal: https://codelibrary.amlegal.com/codes/san_francisco/latest/sf_health/0-0-0-187
--   - SF Recreation & Park Department Dog Policy (Dog Play Areas)
--   - GGNRA Pet Policy of 1979 (ps id 18) + 2025 GOGA Superintendent's
--     Compendium (ps id 19) — already in policy_source from prior NPS
--     GGNRA backfill (commit 8f8cada)
--
-- 14 SF-polygon beaches total; 10 already linked (Aquatic Park, Baker,
-- Candlestick Point, China Beach, Fort Funston, Golden Gate Beach,
-- Kirby, Labrynth, Mile Rock, Ocean Beach). 4 unlinked addressed here:
--
--   - fid 5697 Manor Beach        (37.73, -122.47) — Lake Merced area
--     in SF; SF city jurisdiction. → on_leash via SF citywide rule.
--   - fid 7252 Clipper Cove Beach (37.81, -122.37) — Treasure Island,
--     administered by Treasure Island Development Authority (TIDA)
--     under SF city. → on_leash via SF citywide rule.
--   - fid 8635 East Beach         (37.81, -122.45) — Crissy Field East
--     Beach in GGNRA federal jurisdiction. Crissy Field central/east
--     section is designated off-leash voice-control per GGNRA
--     Compendium. → off_leash_voice_control via existing ps 19.
--   - fid 8675 Marshall's Beach   (37.80, -122.48) — Between Baker
--     Beach and Fort Point, in GGNRA federal jurisdiction. South of
--     Lobos Creek (the Baker Beach off-leash boundary), so default
--     on_leash applies per GGNRA Pet Policy. → on_leash via existing
--     ps 19.

-- ─── 1. Insert SF City policy_source ────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'SF Police Code §41.12 (Duties of Owners or Guardians) + SF Park Code (Dogs and Other Animals in Parks)',
       (SELECT id FROM public.agency WHERE name='San Francisco' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://codelibrary.amlegal.com/codes/san_francisco/latest/sf_health/0-0-0-187',
       'San Francisco (consolidated city and county) dog leash policy. SF Police Code §41.12: Whenever off the owner''s personal property, a dog must be on a leash no longer than eight (8) feet in length, except in designated Dog Play Areas (DPAs). SF Recreation & Park Department Dog Policy: in DPAs, no more than six (6) dogs per person off-leash at one time; dog walkers must carry a maximum 8-foot leash for each dog; DPAs may not be adjacent to playgrounds without limited hard barriers. Beach designations vary by jurisdiction within SF — federal GGNRA beaches (Crissy Field, Baker, Fort Funston, Marshall''s, Ocean Beach, etc.) follow the GGNRA Pet Policy of 1979 + 2025 Superintendent''s Compendium (which controls dog access on those federal lands). SF city-jurisdiction beaches (Aquatic Park city portion, Lake Merced beaches, Treasure Island Clipper Cove) follow §41.12 default 8-ft leash. [Hosted on amlegal at codelibrary.amlegal.com/codes/san_francisco/. Fetched via web search 2026-05-16; SF Park Code chapter not pinpointed beyond §41.12 cite.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'SF Police Code §41.12%'
);

-- ─── 2. SF city beaches → on_leash via SF citywide rule ─────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT v.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       v.evidence, ps.source_url, v.note
FROM (values
  (5697, 'SF Police Code §41.12: dog must be on leash no longer than 8 feet when off owner''s property. Manor Beach is at Lake Merced in SF city jurisdiction.', 'Lake Merced area; SF city jurisdiction, not GGNRA. Walkthrough recommended if beach turns out to be operated by another agency.'),
  (7252, 'SF Police Code §41.12: dog must be on leash no longer than 8 feet when off owner''s property. Clipper Cove is at Treasure Island, administered by Treasure Island Development Authority (TIDA) under SF city.', 'Treasure Island TIDA jurisdiction; SF city policy applies as baseline. Federal Navy legacy use; check for any base-era restrictions.')
) as v(fid, evidence, note)
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'SF Police Code §41.12%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;

-- ─── 3. GGNRA-federal SF beaches → link to existing GGNRA ps 19 ──

-- East Beach (Crissy Field central/east) → off_leash_voice_control
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
VALUES
  (8635, 19, 'sand', 'off_leash_voice_control', 'operative'::operative_status,
   'Crissy Field East Beach is in GGNRA (federal NPS jurisdiction). The central/east section of Crissy Field is a designated off-leash voice-control area per the 2025 GOGA Superintendent''s Compendium (see [[walkthrough-fort-funston]] for the GGNRA voice-control framework).',
   (SELECT source_url FROM public.policy_source WHERE id=19),
   'Westernmost section of Crissy Field has seasonal leash restrictions Jul 1 – May 15; East/central section is the off-leash zone.'),
  (8675, 19, 'sand', 'on_leash', 'operative'::operative_status,
   'Marshall''s Beach is in GGNRA (federal NPS jurisdiction) between Baker Beach and Fort Point. South of the Baker Beach off-leash boundary (Lobos Creek); default leashed-dog rule applies per GGNRA Pet Policy.',
   (SELECT source_url FROM public.policy_source WHERE id=19),
   NULL)
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
