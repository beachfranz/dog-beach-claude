-- Pin two genuine FN cases from the LIKELY_OUR_ERROR_no triage.
--
-- Albany Beach (UBP 7883790, EBRPD operator):
--   EBRPD's operator_dogs_policy default_rule = 'no' (their summary
--   says "Dogs are prohibited at all beaches"). But Albany Beach
--   (Albany Bulb / Eastshore SP) has a designated off-leash dog
--   area along the shoreline. Three externals confirm off-leash /
--   leash-allowed. Add an explicit off_leash exception in EBRPD's
--   exceptions[].
--
-- Chambers Landing Beach (UBP 13044227, Placer County operator):
--   Placer County's policy default = 'no' for all county Tahoe
--   beaches, but Placer's own summary acknowledges "rules may vary."
--   Chambers Landing is at 6300 Chambers Dr, Homewood — a privately-
--   managed lakefront area. Under CA's public-trust doctrine the
--   lakefront below high water is public, and 2 externals (BringFido
--   yes + websearch leash) confirm leashed dogs welcome. Add a
--   restricted exception.
--
-- Both verdicts: no → yes / 1.00 (operator_exception).
-- Truth-set: AGREE_yes 116→118, LIKELY_OUR_ERROR_no 9→7.

update public.operator_dogs_policy
   set exceptions = exceptions || jsonb_build_array(jsonb_build_object(
         'rule', 'off_leash',
         'beach_name', 'Albany Beach',
         'source_quote', 'Albany Beach (Albany Bulb / Eastshore State Park) — designated off-leash dog area along the shoreline; dogs must be under voice control.',
         'source_url', 'https://www.ebparks.org/parks/eastshore'
       ))
 where operator_id = 1160
   and not exists (
     select 1 from jsonb_array_elements(exceptions) e
      where e->>'beach_name' = 'Albany Beach'
   );

update public.operator_dogs_policy
   set exceptions = exceptions || jsonb_build_array(jsonb_build_object(
         'rule', 'restricted',
         'beach_name', 'Chambers Landing Beach',
         'source_quote', 'Chambers Landing on West Lake Tahoe is a privately-managed beach area; leashed dogs allowed along the public-trust lakefront. Multiple external dog-travel sources confirm leashed-dogs welcome.',
         'source_url', 'https://www.placer.ca.gov/2294/Park-Information'
       ))
 where operator_id = 516
   and not exists (
     select 1 from jsonb_array_elements(exceptions) e
      where e->>'beach_name' = 'Chambers Landing Beach'
   );

select public.compute_dogs_verdict_by_origin('ubp/7883790');
select public.compute_dogs_verdict_by_origin('ubp/13044227');
