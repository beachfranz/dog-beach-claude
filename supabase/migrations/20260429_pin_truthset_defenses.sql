-- Pin five LIKELY_OUR_ERROR_no beaches as explicit 'no' exceptions
-- where our verdict is correct but externals (BringFido / DogTrekker /
-- websearch) disagree. Same pattern as the La Jolla Cove pin.
--
-- These are NOT our errors — each has authoritative source data
-- (CDPR's own park page or City of San Diego's lifeguard page) saying
-- no dogs. Externals are wrong. Adding the explicit pin:
--   - Promotes the source from operator_default (weight 0.4) to
--     operator_exception (weight 1.0) — verdict math is stronger
--   - Captures the canonical citation in the exception data
--   - Survives future operator-policy re-extraction (the manual
--     entry stays unless explicitly removed)
--
-- Verdict outcomes unchanged: still 'no' for all five.
--
--   CDPR (551):
--     Natural Bridges State Beach — parks.ca.gov says "Are dogs Allowed? No"
--     Monterey State Beach        — parks.ca.gov says "Are dogs Allowed? No"
--     Moss Landing State Beach    — parks.ca.gov says "Are dogs Allowed? No"
--
--   City of San Diego (455):
--     La Jolla Shores — sandiego.gov: dogs prohibited; on-page signage
--     Pacific Beach   — sandiego.gov: SD city beaches default-no, only
--                       Dog Beach (Ocean Beach) and Fiesta Island
--                       allow dogs. Externals confuse with Ocean Beach
--                       Dog Beach.

begin;

-- CDPR exceptions
update public.operator_dogs_policy
   set exceptions = exceptions || jsonb_build_array(
     jsonb_build_object(
       'rule', 'no',
       'beach_name', 'Natural Bridges State Beach',
       'source_quote', 'parks.ca.gov park page (page_id=541): "Are dogs Allowed? No". Monarch Butterfly Natural Preserve and tide-pool Marine Reserve protections.',
       'source_url', 'https://www.parks.ca.gov/?page_id=541'
     ),
     jsonb_build_object(
       'rule', 'no',
       'beach_name', 'Monterey State Beach',
       'source_quote', 'parks.ca.gov park page: "Are dogs Allowed? No".',
       'source_url', 'https://www.parks.ca.gov/?page_id=571'
     ),
     jsonb_build_object(
       'rule', 'no',
       'beach_name', 'Moss Landing State Beach',
       'source_quote', 'parks.ca.gov park page: "Are dogs Allowed? No". Snowy plover protection area.',
       'source_url', 'https://www.parks.ca.gov/?page_id=575'
     )
   )
 where operator_id = 551
   and not exists (
     select 1 from jsonb_array_elements(exceptions) e
      where e->>'beach_name' in ('Natural Bridges State Beach',
                                  'Monterey State Beach',
                                  'Moss Landing State Beach')
   );

-- City of San Diego exceptions
update public.operator_dogs_policy
   set exceptions = exceptions || jsonb_build_array(
     jsonb_build_object(
       'rule', 'no',
       'beach_name', 'La Jolla Shores',
       'source_quote', 'No dogs allowed at La Jolla Shores per City of San Diego beach regulations; signage on-site. SD default-no rule applies; no exception for La Jolla Shores.',
       'source_url', 'https://www.sandiego.gov/lifeguards/beaches/ljshores'
     ),
     jsonb_build_object(
       'rule', 'no',
       'beach_name', 'Pacific Beach',
       'source_quote', 'No dogs allowed at Pacific Beach per City of San Diego beach regulations. SD default-no rule applies; only Dog Beach (Ocean Beach) and Fiesta Island are dog-friendly. External sources occasionally confuse Pacific Beach with Ocean Beach Dog Beach.',
       'source_url', 'https://www.sandiego.gov/lifeguards/beaches/pb'
     )
   )
 where operator_id = 455
   and not exists (
     select 1 from jsonb_array_elements(exceptions) e
      where e->>'beach_name' in ('La Jolla Shores','Pacific Beach')
   );

-- Recompute the 5 affected beaches
select public.compute_dogs_verdict_by_origin('ubp/1093168');     -- Natural Bridges
select public.compute_dogs_verdict_by_origin('ubp/8940399');     -- Monterey State Beach
select public.compute_dogs_verdict_by_origin('ccc/625');         -- Moss Landing State Beach
select public.compute_dogs_verdict_by_origin('ubp/7847271');     -- La Jolla Shores
select public.compute_dogs_verdict_by_origin('ubp/7051072');     -- Pacific Beach

commit;
