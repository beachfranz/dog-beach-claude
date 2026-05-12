-- 20260512_beach_location_tier_mixed_offleash.sql
--
-- The previous logic dropped any beach with dogs_allowed='mixed' AND
-- has_off_leash=true into 'unknown', which incorrectly excluded the
-- GGNRA-style "varies by zone, but the dog-allowed zone is off-leash"
-- pattern. Examples surfaced by beaches_pending_policy_review:
-- Ocean Beach, Baker, Rodeo, Kirby, Mile Rock — all SF/GGNRA off-leash
-- icons that were classified as unknown.
--
-- New rule: has_off_leash=true is the primary tier-1 signal regardless
-- of whether dogs_allowed is 'yes' or 'mixed'. The presence of an
-- off-leash zone makes the beach an off-leash destination — that's how
-- users find and use it.

begin;

create or replace function public.beach_location_tier(
  p_dogs_allowed text,
  p_has_off_leash boolean,
  p_has_on_leash boolean,
  p_dogs_prohibited_start text default null
)
returns text
language sql
immutable
as $function$
  select case
    when p_has_off_leash and p_dogs_allowed in ('yes','mixed')   then '1_off-leash'
    when p_dogs_allowed = 'yes'                                  then '2_on-leash'
    when p_dogs_allowed = 'mixed' and p_has_on_leash             then '3_limited_access'
    when p_dogs_allowed = 'no'                                   then '4_no_dogs'
    else 'unknown'
  end
$function$;

commit;
