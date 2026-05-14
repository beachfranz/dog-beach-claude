-- 20260513_operators_origin_source_widen.sql
--
-- The 11-pass populate_operators_for_state v2 introduces new
-- origin_source values to preserve per-pass audit. Widen the
-- operators.origin_source CHECK constraint to allow them.

begin;

alter table public.operators
  drop constraint if exists operators_origin_source_check;

alter table public.operators
  add constraint operators_origin_source_check
  check (origin_source = any (array[
    -- Legacy values (preserved)
    'cpad',
    'tiger_places',
    'tiger_counties',
    'seed_federal',
    'seed_tribal',
    'manual',
    -- v2 (2026-05-13) pass-specific labels
    'seed_federal_widened',
    'seed_state_pad_us',
    'seed_district_pad_us',
    'seed_ngo_pad_us',
    'seed_pvt_pad_us',
    'seed_tribal_pad_us',
    'seed_joint_pad_us',
    'seed_tribal_bia',
    'seed_osm_operator',
    'seed_bep_operator_name'
  ]));

commit;
