-- Add La Jolla Cove as an explicit 'no' exception in City of San Diego's
-- operator_dogs_policy.exceptions[].
--
-- Surfaced during truth-set comparison: La Jolla Cove fell into the
-- 'mixed' bucket because BringFido lists it as dog-friendly. Franz
-- confirmed the canonical answer is hard NO — sandiego.gov/lifeguards/
-- beaches/cove shows a "no dogs" image (subtle but present).
--
-- Our verdict was already 'no' via SD's operator_default, but pinning
-- it via an explicit exception with citation makes it defensible
-- against future external-source disagreements and survives
-- extraction re-runs.

update public.operator_dogs_policy
   set exceptions = exceptions || jsonb_build_array(jsonb_build_object(
         'rule', 'no',
         'beach_name', 'La Jolla Cove',
         'source_quote', 'No dogs allowed (per City of San Diego Lifeguards page; signage on-site).',
         'source_url', 'https://www.sandiego.gov/lifeguards/beaches/cove'
       ))
 where operator_id = 455
   and not exists (
     select 1 from jsonb_array_elements(exceptions) e
      where e->>'beach_name' = 'La Jolla Cove'
   );

select public.compute_dogs_verdict_by_origin('ccc/1496');
