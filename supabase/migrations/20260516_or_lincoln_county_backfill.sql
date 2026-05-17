-- Wave 4 OR: Lincoln County — 30 beaches, on_leash per LCC §9.030.
--
-- Source: Lincoln County Code Chapter 9 (Animals).
-- URL: https://www.co.lincoln.or.us/DocumentCenter/View/814/Lincoln-County-Code-LCC-Chapter-9-PDF
--
-- Operative rule (LCC §9.030, per Lincoln County code):
--   "No person shall allow a dog or other pet to run at large, and
--   all pets shall be confined or leashed with a leash of not more
--   than ten (10) feet."
--
-- Note: Lincoln County's max leash length (10 ft) is more permissive
-- than OPRD's state-park 6-ft requirement. On state-park-operated
-- beaches (much of the Lincoln County coast — Beverly Beach, South
-- Beach State Park, Newport-area beaches, etc.), OPRD's 6-ft rule
-- controls. County rule applies on unincorporated non-state-park land.
--
-- 30 Lincoln County active beaches in beaches_gold (Newport, Lincoln
-- City, Depoe Bay, Waldport area).

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Lincoln County Code §9.030 (Animals — leash law)',
       (SELECT id FROM public.agency WHERE name='Lincoln County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://www.co.lincoln.or.us/DocumentCenter/View/814/Lincoln-County-Code-LCC-Chapter-9-PDF',
       'Lincoln County Code Chapter 9 (Animals). §9.030: no person shall allow a dog or other pet to run at large, and all pets shall be confined or leashed with a leash of not more than ten (10) feet. Operative for unincorporated Lincoln County land. On state-park-operated beaches (the majority of the Lincoln County coast), OPRD OAR §736-021-0070 6-ft leash rule applies and is more restrictive. [Hosted as PDF on Lincoln County document center. Fetched via web search 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Lincoln County Code §9.030%'
);

-- ─── 2. Link all Lincoln County beaches → on_leash ──────────────

INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT g.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Lincoln County Code §9.030: pets must be confined or leashed with a leash of not more than 10 feet; running at large prohibited.',
       ps.source_url,
       'County rule allows up to 10-ft leash. OPRD state-park rule (6 ft, per OAR 736-021-0070) is more restrictive and controls on state-park-operated beaches.'
FROM public.beaches_gold g
CROSS JOIN public.policy_source ps
WHERE g.state='OR' AND g.is_active=true AND g.county_name='Lincoln'
  AND ps.citation LIKE 'Lincoln County Code §9.030%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
