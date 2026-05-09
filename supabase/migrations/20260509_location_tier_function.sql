-- 20260509_location_tier_function.sql
--
-- Renames "policy tiers" → "Location Tiers" with a new vocabulary set by
-- Franz on 2026-05-09:
--
--   1_off-leash     (collapses old 1_marquee + 1b_offleash_caveat)
--   2_on-leash      (was 1c_onleash)
--   3_limited_access (was 4_limited_access — number changed)
--   4_no_dogs       (was 3_no_dogs — number changed)
--
-- Centralises the inline CASE that used to live in 6+ scripts into a
-- single immutable SQL function. Callers should use this rather than
-- duplicating the CASE.
--
-- Note the tier-3/tier-4 number swap: anything ordering by tier number
-- needs revisiting.

begin;

create or replace function public.beach_location_tier(
  p_dogs_allowed          text,
  p_has_off_leash         boolean,
  p_has_on_leash          boolean,
  p_dogs_prohibited_start text default null   -- kept for back-compat / future use
)
returns text
language sql
immutable
as $$
  select case
    when p_dogs_allowed = 'yes' and p_has_off_leash                                  then '1_off-leash'
    when p_dogs_allowed = 'yes'                                                      then '2_on-leash'
    when p_dogs_allowed = 'mixed'
         and (p_has_off_leash is null or not p_has_off_leash)
         and p_has_on_leash                                                          then '3_limited_access'
    when p_dogs_allowed = 'no'                                                       then '4_no_dogs'
    else 'unknown'
  end
$$;

comment on function public.beach_location_tier(text, boolean, boolean, text) is
  'Canonical Location Tier classifier as of 2026-05-09. Pass beach_dog_policy fields. Returns one of: 1_off-leash, 2_on-leash, 3_limited_access, 4_no_dogs, unknown.';

grant execute on function public.beach_location_tier(text, boolean, boolean, text)
  to anon, authenticated, service_role;

commit;
