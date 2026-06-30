-- 20260630a_rescope_deactivate_non_ca_or_wa.sql
--
-- RESCOPE: narrow the active scoring project to CA, OR, WA only.
-- Deactivates beaches + dog parks in the other 11 states (MA, MI, HI, ME, NH,
-- MD, VA, OH, DE, RI, AL) plus any non-US / null-state rows. Mechanism is
-- REVERSIBLE: only is_active is touched; scoring_tier / is_scoreable are left
-- intact so a future re-expansion is a single is_active flip with the
-- daily/hourly distinction preserved.
--
-- All 21 scoring/grid/search consumers gate on is_active, so this alone stops
-- scoring + grid work + hides the rows from find.html (no consumer code change).
--
-- COALESCE handles the 155 active, scoreable dog parks with NULL state — these
-- are cross-border OSM junk (metro Vancouver, BC), confirmed outside all 56 US
-- state/territory polygons. They are definitionally out-of-scope.
--
-- REVERSAL (re-expand a state): key on explicit state codes so the non-US
-- null-state junk stays deactivated, e.g.
--   UPDATE public.beaches_gold   SET is_active=true WHERE state IN ('MA','MI',...);
--   UPDATE public.dog_parks_gold SET is_active=true WHERE state IN ('MA','MI',...);

UPDATE public.beaches_gold
   SET is_active = false
 WHERE is_active
   AND COALESCE(state, '') NOT IN ('CA','OR','WA');

UPDATE public.dog_parks_gold
   SET is_active = false
 WHERE is_active
   AND COALESCE(state, '') NOT IN ('CA','OR','WA');
