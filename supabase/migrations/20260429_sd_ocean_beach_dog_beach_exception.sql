-- Pin "Ocean Beach Dog Beach" as an explicit off-leash exception in
-- City of San Diego's operator_dogs_policy.exceptions[].
--
-- Pass 10 (single-word cleaned exceptions require exact match)
-- accidentally killed the legitimate match between SD's existing
-- "Dog Beach" exception (cleans to single-word "dog") and the beach
-- named "Ocean Beach Dog Beach" (cleans to multi-word "ocean dog").
-- Pre-Pass-10 the substring path fired; post-Pass-10 it doesn't.
--
-- Coronado Dog Beach got the right answer through a different path
-- (cpad_unit_default 'restricted' beating operator_default 'no'),
-- but Ocean Beach Dog Beach has no CPAD unit policy backing — its
-- containing CPAD unit is Mission Bay Park (or similar) which has
-- no per-unit policy, so the cascade fell back to SD's default-no.
--
-- Tier 1 credibility-killer: Ocean Beach Dog Beach is THE iconic
-- San Diego off-leash dog beach, well-known to the dog-owner
-- community. Saying "no dogs" there destroys app credibility.
--
-- Fix: add an explicit multi-word exception that matches the actual
-- beach name. Retains the original "Dog Beach" exception (it's still
-- a valid name reference even if Pass 10 no longer matches it
-- generically against multi-word beach names).
--
-- Source: sandiego.gov/parks-and-recreation/parks/regional/dogbeach.
-- Address: 200 Voltaire Street, San Diego.
--
-- Verdict after this migration: yes / 1.00 (sources: operator_exception).

update public.operator_dogs_policy
   set exceptions = exceptions || jsonb_build_array(jsonb_build_object(
         'rule', 'off_leash',
         'beach_name', 'Ocean Beach Dog Beach',
         'source_quote', 'Dog Beach in Ocean Beach is a leash-free area where dogs are always permitted off-leash (24/7); 200 Voltaire Street, San Diego.',
         'source_url', 'https://www.sandiego.gov/parks-and-recreation/parks/regional/dogbeach'
       ))
 where operator_id = 455
   and not exists (
     select 1 from jsonb_array_elements(exceptions) e
      where e->>'beach_name' = 'Ocean Beach Dog Beach'
   );

select public.compute_dogs_verdict_by_origin('ubp/12605142');
