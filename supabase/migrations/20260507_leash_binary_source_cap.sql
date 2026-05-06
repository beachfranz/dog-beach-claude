-- 20260507_leash_binary_source_cap.sql
--
-- C2: confidence cap on binary-only sources for leash_required.
--
-- Four sources emit only binary leash values (cannot speak `mixed`):
--   park_url       (230 votes, conf 0.85) — vocab: 'required'
--   city_policy    (169 votes, conf 0.61) — vocab: 'on_leash','off_leash'
--   county_policy  (126 votes, conf 0.59) — vocab: 'on_leash','off_leash'
--   old_school_llm (648 votes, conf 0.74) — vocab: 'off_leash_ok','required'
--
-- Combined ~1,170 votes that cannot vote `mixed` regardless of beach reality.
-- This caps their effective weight on this specific (field, source) pair so
-- mixed-aware voters (zone_rules_derived_v1, research, unified_v1) can win
-- when present.
--
-- Designed as a data-driven table rather than hardcoded: lets future
-- calibration adjustments be SQL-only, no function changes. Pattern is
-- reusable for other vocabulary-incomplete (field, source) pairs we
-- encounter.

begin;

-- ─── Table: per-(field, source) weight ceiling ────────────────────────────
create table if not exists public.field_source_weight_cap (
  field_name  text not null,
  source      text not null,
  weight_cap  numeric not null check (weight_cap > 0 and weight_cap <= 1.0),
  reason      text,
  added_at    timestamptz default now(),
  primary key (field_name, source)
);

comment on table public.field_source_weight_cap is
  'Per-(field, source) calibration weight ceiling. Used by
   _calibration_weight() to cap sources whose vocabulary is known
   incomplete for the field. Sources are not silenced; their votes
   still count, just at reduced weight.';

-- ─── Seed: cap binary-only sources on leash_policy at 0.5 ────────────────
insert into public.field_source_weight_cap (field_name, source, weight_cap, reason)
values
  ('leash_policy', 'park_url',       0.5, 'Vocab "required" only — cannot vote mixed (audited 2026-05-07)'),
  ('leash_policy', 'city_policy',    0.5, 'Vocab on_leash/off_leash binary — cannot vote mixed (audited 2026-05-07)'),
  ('leash_policy', 'county_policy',  0.5, 'Vocab on_leash/off_leash binary — cannot vote mixed (audited 2026-05-07)'),
  ('leash_policy', 'old_school_llm', 0.5, 'Vocab off_leash_ok/required binary — cannot vote mixed (audited 2026-05-07)')
on conflict (field_name, source) do update
  set weight_cap = excluded.weight_cap,
      reason = excluded.reason,
      added_at = now();

-- ─── Update _calibration_weight to apply the cap ─────────────────────────
create or replace function public._calibration_weight(p_field text, p_source text)
returns numeric
language sql
stable
as $function$
  with raw as (
    select case
      when p_source = 'manual' or p_source = 'manual_curator' then 1.0::numeric
      else coalesce(
        (select public._wilson_lower_bound(c.raw_accuracy, c.sample_size)
           from public.field_source_calibration c
          where c.field_name = p_field and c.source = p_source
          limit 1),
        0.5::numeric
      )
    end as weight,
    (select weight_cap from public.field_source_weight_cap
      where field_name = p_field and source = p_source) as cap
  )
  select least(weight, coalesce(cap, weight)) from raw;
$function$;

-- ─── Recompute consensus + repromote — leash will shift on affected fids ─
do $do$
declare fid_iter bigint;
begin
  for fid_iter in
    select distinct gold_fid from public.beach_enrichment_provenance
     where field_group='dogs' and gold_fid is not null
  loop
    perform public.compute_beach_field_consensus(fid_iter);
    perform public.promote_canonical_dogs_to_beach_dog_policy(fid_iter, 0.5);
  end loop;
end $do$;

commit;
