-- Phase B: Monterey County code backfill.
--
-- Monterey County Code Title 8 Chapter 8.20 "Dogs Running At Large"
-- defines the leash baseline. The "at-large" definition specifies a
-- 6-foot leash; running "at-large" in unincorporated territory is an
-- infraction.

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Monterey County Code Chapter 8.20 (Dogs Running At Large)',
       (SELECT id FROM public.agency WHERE name='Monterey County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/ca/monterey_county/codes/code_of_ordinances?nodeId=TIT8ANCO',
       '"At-large" means off the premises of the person owning, or having possession, charge, custody, or control of the animal. All dogs shall be deemed to be running "at large," unless: A. Restrained by a chain, strap or cord attached to their collars or harness, of no more than six feet in length, actually held by some person capable of exercising physical restraint, or made fast to some stationary object, or confined within a cage or other dog tight enclosure such as an electric or electronic fence; or B. Accompanied by a person, the dog being sufficiently trained to be reliably responsive to the recall command and control of such person... It is unlawful for any person owning or having charge, care or control of any dog, whether licensed or not, to allow or permit any such dog to be at-large within the unincorporated territory of the County of Monterey. A violation of this Subsection shall be an infraction. [Monterey County Code Title 8 Ch 8.20. Fetched via Municode 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Monterey County Code Chapter 8.20%'
);

WITH mc_id AS (
  SELECT id FROM public.agency WHERE name='Monterey County' AND type='county'
), targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  LEFT JOIN public.beach_agency ba_dp
    ON ba_dp.beach_fid = g.fid
   AND ba_dp.authority_domain = 'dog_policy'
   AND ba_dp.precedence_rank = 1
  WHERE g.state='CA' AND g.is_active=true AND g.county_name='Monterey'
    AND (ba_dp.agency_id = (SELECT id FROM mc_id) OR ba_dp.agency_id IS NULL)
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Monterey County Code Chapter 8.20%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'Monterey County Code §8.20: dogs at-large prohibited in unincorporated territory; "at-large" definition requires 6-foot leash or voice-trained recall under person''s control. Applies to county beaches/parks operated within unincorporated Monterey County.',
       'https://library.municode.com/ca/monterey_county/codes/code_of_ordinances?nodeId=TIT8ANCO',
       'Default on-leash. Note Monterey§8.20 explicitly allows voice-control under §B if "reliably responsive to recall" — could be off_leash_voice_control at off-leash designated areas, deferred per-beach refinement.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
