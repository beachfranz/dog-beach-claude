-- Wave 3: City of San Clemente — 7 beaches, citywide no-dogs default
-- with pilot dog-beach zone flagged.
--
-- Source background:
--   - OC Register 2014-03-19 (Fred Swegles): "San Clemente opens nearly
--     all parks to dogs" — confirms longstanding city policy that
--     "owners are barred from letting the animals stray onto the sand"
--   - SanClemente.com beach rules summary: dogs prohibited on city
--     beaches uniformly
--   - PicketFence Media 2025-08-28: "San Clemente Launches Dog Beach
--     Trial South of T-Street" — City Council approved 2025-08-19 a
--     six-month pilot allowing leashed dogs in a 1,500-foot stretch
--     from Tower 7 to Boca del Canon Beach (Calafia area). Pending
--     California Coastal Commission approval at time of article;
--     status as of 2026-05-16 not independently verified.
--
-- Pilot zone hours (per article): leashed dogs allowed Mon-Thu
-- 6-8 AM and after 6 PM; Fri 6-8 AM only; no dogs weekends.
--
-- Codified backstop: San Clemente Municipal Code Title 12 Chapter
-- 12.32 (Beaches and Piers) + Title 6 (Animals); exact section
-- prohibiting dogs on beach sand not yet pinpointed — Municode TOC
-- searched but the specific subsection not surfaced. Captured via
-- agency_administrative_policy subtype pending codified citation.
--
-- 7 SC-city-polygon beaches (sorted N→S):
--   - 8861 North Beach            33.430, -117.632   default not_allowed
--   - 8860 El Portal              33.427, -117.628   default not_allowed
--   - 8810 The Pier Beach         33.420, -117.621   default not_allowed
--                                                    (+ Pier Area No Dog
--                                                    Zones on July 4th &
--                                                    Ocean Festival weekend)
--   - 8809 T Street Beach         33.416, -117.617   default not_allowed
--                                                    (pilot dog zone begins
--                                                    south of T-Street)
--   - 8459 Calafia Beach          33.405, -117.607   default not_allowed
--                                                    (CONTAINS pilot dog
--                                                    zone Tower 7 → Boca
--                                                    del Canon — flagged
--                                                    for walkthrough)
--   - 8848 Playa Las Palmeras     33.392, -117.599   default not_allowed
--                                                    (south of pilot zone)
--   - 8435 San Mateo Point        33.388, -117.597   default not_allowed
--                                                    (far south)
--
-- All 7 currently unlinked. All linked here as not_allowed (the city's
-- default). Calafia Beach gets a status_note flagging the pilot zone
-- override; once a walkthrough confirms the pilot is operative, that
-- beach can be re-linked with on_leash + the seasonal/time-of-day
-- restriction.

-- ─── 1. Insert policy_source ────────────────────────────────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'agency_administrative_policy',
       'City of San Clemente — citywide dogs-on-beach prohibition (with 2025 pilot dog-beach trial flagged)',
       (SELECT id FROM public.agency WHERE name='San Clemente' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://www.ocregister.com/2014/03/19/san-clemente-opens-nearly-all-parks-to-dogs/',
       'City of San Clemente — dogs-on-beach policy. CITYWIDE DEFAULT: dogs are prohibited on city beach sand. Per OC Register 2014-03-19 (cited as part of council action expanding park dog access while preserving beach prohibition): "owners are barred from letting the animals stray onto the sand." Beach trail (paved/inland of beach): leashed dogs permitted on 6-ft max leash. PILOT DOG BEACH (approved 2025-08-19, pending California Coastal Commission approval at time of article): six-month trial allowing leashed dogs in a 1,500-foot stretch from Tower 7 to Boca del Canon Beach (Calafia area, south of T-Street). Pilot hours: Mon-Thu 6:00-8:00 AM and after 6:00 PM; Fri 6:00-8:00 AM only; no dogs weekends. Pier Area additional restrictions: No Dog Zones on July 4th and Ocean Festival Weekend (third weekend of July). Codified backstop is in San Clemente Municipal Code Title 6 (Animals) and Title 12 Chapter 12.32 (Beaches and Piers); exact section number not yet pinpointed in our records. [Sources: OC Register 2014 article (Fred Swegles); SanClemente.com beach rules; PicketFence Media 2025-08-28 pilot announcement. Fetched 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE source_url='https://www.ocregister.com/2014/03/19/san-clemente-opens-nearly-all-parks-to-dogs/'
);

-- ─── 2. Link all 7 SC-city-polygon beaches → not_allowed (default) ──

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'not_allowed', 'operative'::operative_status,
       'San Clemente citywide default: dogs prohibited on city beach sand. OC Register 2014: "owners are barred from letting the animals stray onto the sand."',
       ps.source_url,
       CASE g.fid
         WHEN 8459 THEN 'CONTAINS 2025 pilot dog-beach zone (Tower 7 to Boca del Canon, ~1500 ft). Pilot allows leashed dogs Mon-Thu 6-8am + after 6pm; Fri 6-8am only; no weekends. Walkthrough needed to confirm pilot status (CCC approval pending as of 2025-08).'
         WHEN 8810 THEN 'Pier Area: additional No Dog Zones in effect on July 4th and Ocean Festival Weekend (third weekend July).'
         ELSE NULL
       END
FROM public.beaches_gold g
JOIN public.jurisdictions j ON st_intersects(j.geom, g.geom)
  AND j.state IN ('California','CA') AND j.place_type='C1' AND j.name='San Clemente'
CROSS JOIN public.policy_source ps
WHERE g.state='CA' AND g.is_active=true
  AND ps.source_url='https://www.ocregister.com/2014/03/19/san-clemente-opens-nearly-all-parks-to-dogs/'
  AND NOT EXISTS (
    SELECT 1 FROM public.beach_policy_source bps WHERE bps.beach_fid=g.fid
  )
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
