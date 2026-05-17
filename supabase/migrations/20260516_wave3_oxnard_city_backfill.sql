-- Wave 3: City of Oxnard — 5 beaches, citywide on_leash per §5-42.
--
-- Source: Oxnard Municipal Code §5-42 (DOG LEASHING REQUIRED) hosted
-- on amlegal.
-- URL: https://codelibrary.amlegal.com/codes/oxnard/latest/oxnard_ca/0-0-0-67053
--
-- Operative rule (§5-42, per amlegal source):
--   "No person shall permit any dog owned, harbored or controlled by
--   that person to be on any public beach, street, sidewalk, alley,
--   park or place of whatever nature open to and used by the public
--   unless the dog is securely leashed and the leash is continuously
--   controlled by a responsible person capable of controlling such dog
--   or unless such dog is securely confined in a motor vehicle."
--
-- Oxnard explicitly permits dogs on public beaches if leashed — unlike
-- Malibu (prohibition) or Pacifica (leash + Esplanade exception).
--
-- 5 Oxnard-city-polygon beaches:
--   - fid 9053 Hobie Beach          (Oxnard primary; unlinked) → on_leash
--   - fid 8619 Kiddie Beach         (already linked; skipped)
--   - fid 8616 Mandalay State Beach (state, already linked; skipped)
--   - fid 8213 McGrath State Beach  (state, already linked; skipped)
--   - fid 8590 Oxnard Beach         (Oxnard primary; unlinked) → on_leash
--
-- Note: Mandalay + McGrath are CA DPR state beaches; state-park rules
-- prohibit dogs on the sand of those beaches (per parks.ca.gov/Dogs
-- catalog). Oxnard city rule layers but state-park rule controls on
-- state-owned beach sand.

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Oxnard Municipal Code §5-42 (DOG LEASHING REQUIRED)',
       (SELECT id FROM public.agency WHERE name='Oxnard' AND type='city'),
       ARRAY['dog_policy']::text[],
       'https://codelibrary.amlegal.com/codes/oxnard/latest/oxnard_ca/0-0-0-67053',
       'Oxnard Municipal Code §5-42 DOG LEASHING REQUIRED: No person shall permit any dog owned, harbored or controlled by that person to be on any public beach, street, sidewalk, alley, park or place of whatever nature open to and used by the public unless the dog is securely leashed and the leash is continuously controlled by a responsible person capable of controlling such dog or unless such dog is securely confined in a motor vehicle. Oxnard explicitly permits dogs on public beaches if leashed. [Hosted on amlegal at codelibrary.amlegal.com/codes/oxnard/. Fetched via web search 2026-05-16; amlegal SPA section anchor not directly fetched.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Oxnard Municipal Code §5-42%'
);

-- ─── 2. Link unlinked Oxnard beaches → on_leash ────────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Oxnard Municipal Code §5-42: dogs permitted on public beaches if securely leashed and the leash continuously controlled by a responsible competent person.',
       ps.source_url, NULL
FROM public.beaches_gold g
JOIN public.jurisdictions j ON st_intersects(j.geom, g.geom)
  AND j.state IN ('California','CA') AND j.place_type='C1' AND j.name='Oxnard'
CROSS JOIN public.policy_source ps
WHERE g.state='CA' AND g.is_active=true
  AND ps.citation LIKE 'Oxnard Municipal Code §5-42%'
  AND NOT EXISTS (
    SELECT 1 FROM public.beach_policy_source bps WHERE bps.beach_fid=g.fid
  )
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
