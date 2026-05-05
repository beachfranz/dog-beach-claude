-- 20260505_calibration_weight_default_fix.sql
--
-- Fix: _calibration_weight() returned NULL for any source without a row in
-- field_source_calibration, instead of falling through to the 0.5 default.
-- The coalesce() was inside the subquery, so when zero rows matched, the
-- subquery itself returned NULL — the coalesce only fired for matching rows.
--
-- Caught 2026-05-05 while diagnosing Carlsbad Lagoon Dog Beach. text_repass_v1
-- has no calibration row (we never spot-checked it), so its votes were
-- silently weighted to NULL, suppressing 440 beaches' worth of zone-rules-
-- derived consensus contribution.
--
-- Fix: move the coalesce OUTSIDE the subquery so missing rows → 0.5.

begin;

create or replace function public._calibration_weight(p_field text, p_source text)
returns numeric language sql stable as $$
  select case
    when p_source = 'manual' or p_source = 'manual_curator' then 1.0::numeric
    else coalesce(
      (select public._wilson_lower_bound(c.raw_accuracy, c.sample_size)
         from public.field_source_calibration c
        where c.field_name = p_field and c.source = p_source
        limit 1),
      0.5::numeric
    )
  end;
$$;

-- Recompute consensus + auto-promoter for every beach that has text_repass_v1
-- evidence (they were the cohort the bug affected).
do $do$
declare fid_iter bigint;
begin
  for fid_iter in
    select distinct gold_fid from public.beach_enrichment_provenance
     where source = 'text_repass_v1' and gold_fid is not null
  loop
    perform public.compute_beach_field_consensus(fid_iter);
    perform public.promote_canonical_to_consumer_tables(fid_iter);
  end loop;
end $do$;

commit;
