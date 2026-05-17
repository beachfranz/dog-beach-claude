-- Phase B: Tuolumne County code backfill.
--
-- Discovered 2026-05-16 via Franz's pointer (county-hosted PDF):
--   https://www.tuolumnecounty.ca.gov/DocumentCenter/View/312/Chapter-604---Animal-Control
--
-- The URL slug `604` flattens the chapter number `6.04`. Phase A discovery
-- missed Tuolumne because the county hosts its code as PDF chapters on its
-- own DocumentCenter — not on Municode/amlegal/qcode/ecode360/county.codes.
--
-- Code structure (Title 6, Animals / Chapter 6.04 Animal Control):
--   § 6.04.020(E) Definitions — "At large"
--   § 6.04.020(T) Definitions — "Leash" (6-ft max)
--   § 6.04.280 Control of dogs (operative leash mandate)
--
-- Operative rule (§6.04.280(A), verbatim):
--   "Every owner of a dog shall keep it exclusively upon his/her own
--   premises; provided, however, that such dog may be off such premises
--   if it is under the control of a person capable and actually restraining
--   the animal by a leash. The following purposes are permitted and
--   excepted therefrom, provided the dog does not present a hazard to the
--   public safety and welfare, does not trespass upon private property,
--   cause a nuisance or violate any other provision of this Chapter or
--   state law: 1. Lawful hunting; 2. Livestock herding and control on
--   public lands; 3. An animal being used by peace officers in the pursuit
--   of their duties."
--
-- §6.04.020(E) "At large" = a dog off the premises of the owner and not
-- under restraint by leash and physical control of the owner.
-- §6.04.020(T) "Leash" = any substantial rope, leather strap, chain, or
-- other material not exceeding six (6) feet in length being held in the
-- hand of a person capable and actually controlling the dog.
--
-- 5 Tuolumne County beaches in beaches_gold (inland reservoir/lake — Don
-- Pedro and Tulloch reservoirs primarily).

-- ─── 1. Insert tier-1 codified-ordinance policy_source ───────────

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Tuolumne County Code §6.04.280 (Control of dogs) + §6.04.020(E,T) (Definitions) — Title 6 / Chapter 6.04',
       (SELECT id FROM public.agency WHERE name='Tuolumne County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://www.tuolumnecounty.ca.gov/DocumentCenter/View/312/Chapter-604---Animal-Control',
       'Tuolumne County Code Title 6 (Animals), Chapter 6.04 Animal Control. §6.04.280 Control of dogs. (A) Every owner of a dog shall keep it exclusively upon his/her own premises; provided, however, that such dog may be off such premises if it is under the control of a person capable and actually restraining the animal by a leash. Excepted purposes (provided no hazard/trespass/nuisance): (1) lawful hunting; (2) livestock herding and control on public lands; (3) animal being used by peace officers. §6.04.020(E) "At large" = a dog off the premises of the owner and not under restraint by leash and physical control of the owner. §6.04.020(T) "Leash" = any substantial rope, leather strap, chain, or other material not exceeding six (6) feet in length being held in the hand of a person capable and actually controlling the dog to which it is attached. Ord. 3311 § 1 (part), 2017; Ord. 2190 § 2 (part), 1997. [Hosted on county DocumentCenter (PDF). Fetched verbatim via curl + pypdf 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Tuolumne County Code §6.04.280%'
);

-- ─── 2. Link Tuolumne County beaches ────────────────────────────

WITH tuo_id AS (
  SELECT id FROM public.agency WHERE name='Tuolumne County' AND type='county'
), targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  LEFT JOIN public.beach_agency ba_dp
    ON ba_dp.beach_fid = g.fid AND ba_dp.authority_domain = 'dog_policy' AND ba_dp.precedence_rank = 1
  WHERE g.state='CA' AND g.is_active=true AND g.county_name='Tuolumne'
    AND (ba_dp.agency_id = (SELECT id FROM tuo_id) OR ba_dp.agency_id IS NULL)
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid, ps.id, 'sand', 'on_leash', 'operative'::operative_status,
       'Tuolumne County Code §6.04.280(A): dog must be on owner''s premises or under control of a person capable and actually restraining by a leash. §6.04.020(T) caps leash at six feet, held in hand.',
       ps.source_url,
       NULL
FROM targets t
CROSS JOIN public.policy_source ps
WHERE ps.citation LIKE 'Tuolumne County Code §6.04.280%'
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
