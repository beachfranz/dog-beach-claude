-- Phase B: Santa Barbara County code backfill.
--
-- Per [[comprehensive-source-accelerator]] + [[state-expansion-playbook]]:
-- Santa Barbara County Code Ch26 §26-49 (Dogs required on leash) +
-- §26-49.1 (Off-leash dog areas) is the codified dog ordinance for
-- all county-operated parks and recreation areas including beaches.
--
-- 42 SB County beaches in beaches_gold; will link county-primary and
-- no-primary beaches.

INSERT INTO public.policy_source (subtype, citation, issuing_agency_id, scope, source_url, full_text)
SELECT 'municipal_code',
       'Santa Barbara County Code §26-49 + §26-49.1 (Dogs on leash; Off-leash areas)',
       (SELECT id FROM public.agency WHERE name='Santa Barbara County' AND type='county'),
       ARRAY['dog_policy']::text[],
       'https://library.municode.com/ca/santa_barbara_county/codes/code_of_ordinances?nodeId=CH26PARE_ARTIIIANRE',
       'Sec. 26-49. Dogs required on leash. (a) Unless otherwise approved by community services director or their designee, no person shall bring any dog into any county recreation area that is marked or designated as "NO DOGS ALLOWED." (b) No person shall bring a dog into or permit a dog to enter or remain within, or maintain a dog within any county recreation area unless the dog is on a leash not more than six feet in length and under the immediate control of a capable and responsible person, or properly confined. It is unlawful to permit any dog to run at large within any county recreation area or to be within any county recreation area without a physical restraint. Sec. 26-49.1. Off-leash dog areas in designated county recreation areas. Notwithstanding anything in chapter 26, the community services director, or his designee, may designate and un-designate, off-leash sites, off-leash areas, and off-leash dog hours in parks and open spaces under his jurisdiction... [Santa Barbara County Code Chapter 26 - PARKS AND RECREATION, Article III - Animal Regulations. Fetched via Municode 2026-05-16.]'
WHERE NOT EXISTS (
  SELECT 1 FROM public.policy_source WHERE citation LIKE 'Santa Barbara County Code §26-49%'
);

WITH sb_id AS (
  SELECT id FROM public.agency WHERE name='Santa Barbara County' AND type='county'
), targets AS (
  SELECT DISTINCT g.fid
  FROM public.beaches_gold g
  LEFT JOIN public.beach_agency ba_dp
    ON ba_dp.beach_fid = g.fid
   AND ba_dp.authority_domain = 'dog_policy'
   AND ba_dp.precedence_rank = 1
  WHERE g.state='CA' AND g.is_active=true AND g.county_name='Santa Barbara'
    AND (ba_dp.agency_id = (SELECT id FROM sb_id) OR ba_dp.agency_id IS NULL)
)
INSERT INTO public.beach_policy_source
  (beach_fid, policy_source_id, section, rule, operative_status,
   evidence_verbatim, evidence_url, status_note)
SELECT t.fid,
       (SELECT id FROM public.policy_source WHERE citation LIKE 'Santa Barbara County Code §26-49%'),
       'sand', 'on_leash', 'operative'::operative_status,
       'SB County Code §26-49(b): dog on a leash not more than six feet in length and under the immediate control of a capable and responsible person. §26-49(a) authorizes the community services director to designate "NO DOGS ALLOWED" zones. §26-49.1 authorizes off-leash designations.',
       'https://library.municode.com/ca/santa_barbara_county/codes/code_of_ordinances?nodeId=CH26PARE_ARTIIIANRE',
       'Sand rule defaults to on-leash. Per-beach designations (no-dogs or off-leash) under §26-49(a)/§26-49.1 require Director action — deferred per-beach refinement.'
FROM targets t
ON CONFLICT (beach_fid, policy_source_id, section) DO NOTHING;
