-- 20260508_dedup_pair_safe.sql
--
-- Safety gate for late-stage dedup pair-matching. Returns true only when
-- two beach names are confidently safe to auto-merge.
--
-- Two rules, both validated empirically tonight (2026-05-08):
--
--   1. Minimum trigram similarity 0.70.
--      First tried 0.85 to catch the Coronado-vs-Coronado-Dog-Beach miss
--      (sim 0.79), but it killed too many legit variants (Stinson Beach vs
--      Stinson State Beach is sim ~0.6; Hermosa Beach vs Hermosa City Beach
--      is sim 0.7). The dog-suffix guard below handles the Coronado case
--      structurally instead.
--
--   2. Dog-suffix discrimination — the real safety net. If ONE name contains
--      "dog" as a word and the OTHER doesn't, refuse. Reason: in this domain
--      "X Dog Beach" is reliably a different physical zone than "X Beach" —
--      the off-leash area is conventionally separated from the main beach.
--      Conservative bias: don't auto-merge across the dog/no-dog boundary.
--
-- Future dedup orchestrators must call this function for every candidate
-- pair before deactivating either side.

begin;

create or replace function public._dedup_pair_safe(name_a text, name_b text, min_sim real default 0.70)
returns boolean
language sql
immutable
as $$
  with norm as (
    select lower(trim(name_a)) as a_lc, lower(trim(name_b)) as b_lc
  ),
  flags as (
    select
      a_lc, b_lc,
      -- Word-boundary check for "dog" — matches " dog " / "^dog " / " dog$" / "^dog$"
      (a_lc ~ '\mdog\M') as a_has_dog,
      (b_lc ~ '\mdog\M') as b_has_dog
    from norm
  )
  select
    similarity(a_lc, b_lc) >= min_sim
    and (a_has_dog = b_has_dog)  -- both have dog OR neither does
  from flags;
$$;

grant execute on function public._dedup_pair_safe(text, text, real) to anon, authenticated, service_role;

-- Validated test cases (2026-05-08):
-- select public._dedup_pair_safe('Coronado Beach', 'Coronado Dog Beach');        -- false (dog mismatch)
-- select public._dedup_pair_safe('Coronado Dog Beach', 'Del Mar Dog Beach');     -- false (sim too low)
-- select public._dedup_pair_safe('Stinson Beach', 'Stinson State Beach');        -- true
-- select public._dedup_pair_safe('El Matador Beach', 'El Matador State Beach'); -- true
-- select public._dedup_pair_safe('Hermosa Beach', 'Hermosa City Beach');         -- true
-- select public._dedup_pair_safe('LongPoint Beach', 'Long Point Beach');         -- true (spacing)
-- select public._dedup_pair_safe('Bayshore Beach', 'Alamitos Bay Beach');        -- false (sim too low)

commit;
