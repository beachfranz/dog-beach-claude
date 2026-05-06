-- 20260507_retune_county_policy_confidence.sql
--
-- Per Franz 2026-05-07 + 10-beach calibration spot-check:
-- county_policy at avg conf 0.75 was empirically too high. Pattern in
-- the disagreements showed county_policy systematically over-broad —
-- single county-level extraction (e.g., "San Mateo County Ordinance
-- 4778 prohibits dogs at Tunitas Creek Beach") got stamped onto every
-- beach in the county. Per-beach extractions correctly scoped per beach.
--
-- Calibration: 8/10 disagreements went to text_repass. county_policy
-- should rank below per-unit/per-operator emitters, slightly above
-- the agency-class default floor.
--
-- Retune: cap county_policy confidence at 0.60 (drops below
-- cpad_unit/operator/pad_us-per-unit at 0.69-0.70). city_policy
-- already at 0.59 — no change.

begin;

update public.beach_enrichment_provenance
   set confidence = least(confidence, 0.60),
       updated_at = now()
 where field_group = 'dogs'
   and source = 'county_policy'
   and confidence > 0.60;

-- Recompute consensus for affected fids — confidence change can shift winners.
do $do$
declare fid_iter bigint;
begin
  for fid_iter in
    select distinct gold_fid from public.beach_enrichment_provenance
     where field_group = 'dogs' and source = 'county_policy' and gold_fid is not null
  loop
    perform public.compute_beach_field_consensus(fid_iter);
    perform public.promote_canonical_dogs_to_beach_dog_policy(fid_iter, 0.5);
  end loop;
end $do$;

commit;
