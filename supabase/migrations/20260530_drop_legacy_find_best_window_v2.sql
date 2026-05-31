-- Drop the legacy 4-arg _find_best_window_v2 signature so callers don't
-- hit "function is not unique" ambiguity.
--
-- 20260530_find_best_window_excludes_is_closed.sql replaced the helper
-- via CREATE OR REPLACE without changing the signature. Then
-- 20260530_find_best_window_from_hour.sql added p_from_hour as a new
-- optional param — but PG treats that as a NEW overload (different
-- arg count) and KEEPS the old 4-arg version around. Existing callers
-- like compute_dog_park_hourly_v2 + compute_beach_hourly_v2 call with
-- 4 positional args (p_hours, 0, 2, 5) which now matches BOTH overloads
-- ambiguously → 400 from PostgREST.
--
-- Resolution: drop the old 4-arg signature. The 5-arg signature is
-- backwards-compatible because p_from_hour defaults to NULL (= full-day
-- behavior, same as the old 4-arg).

begin;

DROP FUNCTION IF EXISTS public._find_best_window_v2(jsonb, integer, integer, integer);

commit;
