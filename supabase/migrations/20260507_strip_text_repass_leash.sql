-- 20260507_strip_text_repass_leash.sql
--
-- Fix C1 — Stop text_repass_v1 from voting binary leash_required.
--
-- text_repass_v1 produces zone_rules JSONB (rich, section-aware,
-- mixed-capable). The same data is voted on by zone_rules_derived_v1
-- which has mixed in its vocab.
--
-- text_repass_v1 ALSO writes a flat `leash_required` claim derived
-- internally as a binary (on_leash | off_leash) — that's a vocabulary
-- regression vs. its own zone_rules. With 571 votes at conf 0.70, that
-- binary signal was drowning out mixed-capable voters.
--
-- Fix: strip `leash_required` from existing text_repass_v1 BEP rows.
-- Future runs of repass_zone_rules.py won't add it back (script updated
-- 2026-05-07 to skip the field).

begin;

update public.beach_enrichment_provenance
   set claimed_values = claimed_values - 'leash_required',
       updated_at = now()
 where field_group = 'dogs'
   and source = 'text_repass_v1'
   and claimed_values ? 'leash_required';

-- Recompute consensus + repromote for affected fids.
do $do$
declare fid_iter bigint;
begin
  for fid_iter in
    select distinct gold_fid from public.beach_enrichment_provenance
     where field_group='dogs' and source='text_repass_v1' and gold_fid is not null
  loop
    perform public.compute_beach_field_consensus(fid_iter);
    perform public.promote_canonical_dogs_to_beach_dog_policy(fid_iter, 0.5);
  end loop;
end $do$;

commit;
