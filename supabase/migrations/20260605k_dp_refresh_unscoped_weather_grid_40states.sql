-- Part (a): drop scope_states for DP refresh jobs (informational only —
-- edge fn already processes all states post commit 57865e4).
--
-- Part (b): expand weather_grid_* scope_states from 13 → 40 states
-- (every state with ≥10 scoreable entities, beach OR dog park).
--
-- Why (a): dog_park_refresh_chunked + hourly_dog_park_now_refresh both
-- have scope_states gates that don't actually filter — the orch
-- dispatcher passes the cron command unchanged; the edge function's
-- own state filter is now empty (=all states). Keeping the 13-state
-- list as scope_states is misleading metadata. NULL = "no filter".
--
-- Why (b): weather_grid loader (T1/T2/T3 tiers + inventory) does
-- actively scope to listed states. Expanding to 40 states means
-- weather grid coverage for the inland/no-beach DP-rich states
-- (TX 229 DPs, NY 141, FL 121, PA 108, WI 99, NC 74, MN 63, GA 63,
-- NJ 59, etc.) that were previously falling back to direct
-- Open-Meteo per-park during refresh. Net cost similar (batched
-- grid load ≈ per-park fallback at scale) but cleaner pattern.
--
-- Per [[paired-functions-port-fixes-both-sides]]: today's edge-fn
-- scope lift (57865e4) + MVP+ expansion (6e7012a) + this completes
-- the dog-park national-coverage trifecta.

-- ── Part (a): unscope DP refresh jobs ────────────────────────────────
UPDATE public.orch_jobs
   SET scope_states = NULL,
       updated_at = now()
 WHERE job_name IN (
   'dog_park_refresh_chunked',
   'hourly_dog_park_now_refresh'
 );

-- ── Part (b): expand weather_grid_* to 40-state list ─────────────────
UPDATE public.orch_jobs
   SET scope_states = ARRAY[
         'AL','AR','AZ','CA','CT','DC','DE','FL','GA','ID',
         'IL','KS','MA','MD','MI','MN','MO','MT','NC','NE',
         'NH','NJ','NM','NV','NY','OH','OK','OR','PA','RI',
         'SC','SD','TN','TX','UT','VA','VT','WA','WI','WY'
       ],
       updated_at = now()
 WHERE job_name IN (
   'weather_grid_t1',
   'weather_grid_t2',
   'weather_grid_t3',
   'weather_grid_inventory',
   'weather_grid_hourly'  -- legacy disabled job; bump for consistency
 );
