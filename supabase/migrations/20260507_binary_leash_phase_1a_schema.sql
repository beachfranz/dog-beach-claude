-- 20260507_binary_leash_phase_1a_schema.sql
--
-- Binary leash schema migration — Phase 1A (additive).
--
-- Replaces the categorical leash_policy enum (on_leash/off_leash/mixed/null)
-- with two presence booleans (has_on_leash, has_off_leash) that can vote
-- INDEPENDENTLY. This dissolves the carve-out scoping problem at the schema
-- level: a beach with both on-leash zones (paths) and off-leash zones
-- (sand) ends up (T, T) rather than the overloaded "mixed".
--
-- Phase 1A: add columns + backfill from existing leash_policy. Three-state
-- (T/F/null) so absence-of-evidence stays distinct from confirmed-no.
--
-- Phase 1B (next migration): derivation functions + consensus extension so
-- new emitter writes populate the booleans automatically.
-- Phase 1C: promoter writes the booleans alongside legacy leash_policy.
-- Phase 2: consumer migration (detail.html, beach-chat, admin tooling).
-- Phase 3: drop leash_policy + 'mixed' from dogs_allowed enum.

begin;

alter table public.beach_dog_policy
  add column if not exists has_on_leash  boolean,
  add column if not exists has_off_leash boolean;

comment on column public.beach_dog_policy.has_on_leash is
  'Binary presence flag: does this beach have any zone where leashed dogs are allowed? Independent of has_off_leash. Truth about WHERE/WHEN lives in zone_rules.regions. NULL = no extracted evidence yet.';
comment on column public.beach_dog_policy.has_off_leash is
  'Binary presence flag: does this beach have any zone where off-leash dogs are allowed? See has_on_leash for shape.';

-- Backfill from existing leash_policy + dogs_allowed + off_leash_flag.
-- Idempotent: only sets where currently NULL. The mapping is:
--   dogs_allowed='no'             -> (F, F)
--   leash_policy='on_leash'       -> (T, F)
--   leash_policy='off_leash'      -> (F, T)
--   leash_policy='mixed'          -> (T, T)
--   off_leash_flag=true           -> has_off_leash=T (additive)
--   dogs_allowed='yes' (no leash) -> has_on_leash=T (conservative); off=null
--
-- Phase 1B's consensus run will refine these via independent voting on each
-- binary, capturing carve-out signals the categorical leash_policy collapsed.
update public.beach_dog_policy
   set has_on_leash = case
         when dogs_allowed = 'no'        then false
         when leash_policy = 'on_leash'  then true
         when leash_policy = 'off_leash' then false
         when leash_policy = 'mixed'     then true
         when dogs_allowed = 'yes'       then true   -- conservative default
         else null
       end
 where has_on_leash is null;

update public.beach_dog_policy
   set has_off_leash = case
         when dogs_allowed = 'no'        then false
         when leash_policy = 'off_leash' then true
         when leash_policy = 'on_leash'  then false
         when leash_policy = 'mixed'     then true
         when off_leash_flag is true     then true
         when dogs_allowed = 'yes'       then null   -- abstain — yes alone doesn't say off-leash exists
         else null
       end
 where has_off_leash is null;

commit;
