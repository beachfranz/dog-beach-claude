-- Expand MVP+ scope from 5 → 13 states.
--
-- Before: scope_states = ['CA','OR','WA','MD','UT']
-- After:  scope_states = ['CA','OR','WA','MD','UT','MA','MI','VA','NH','OH','DE','AL','RI']
--
-- Why: 2026-06-05 LATE — today's dog-park refresh scope lift (commit
-- 57865e4) made the EDGE FUNCTION process all states, but ORCH-LEVEL
-- scope_states was untouched. orch dispatcher uses scope_states for
-- multi-state jobs that aren't refresh-shaped:
--   - weather_grid_t1/t2/t3: load weather grid cells per state
--   - monthly_dog_park_coverage: trigger dog-park-state-launch pipeline
--   - weekly_codify_gap_clone: LLM codify gap fill
--   - verdict_cascade_nightly: cascade refresh
--   - rec_freshness_audit_per_state: per-state freshness watchdog
--   - process_geom_change_queue: drift catching
--   - precipitation_history_rollup: rollup for bacteria-risk
--   - noaa_station_health_audit: tide-station audit
--   - weekly_dog_park_data_quality_audit + weekly_pipeline_health
--   - weekly_tide_refresh
--   - weather_grid_inventory + weather_grid_hourly (legacy)
--
-- The 8 new states chosen by criterion (a) — "beach pipeline already
-- runs there." Beach inventory is scoreable in MA/MI/VA/NH/OH/DE/AL/RI;
-- expanding the orch scope brings dog-park + weather-grid + codify
-- operational infrastructure to parity with the beach side.
--
-- Expected effects: weather grid cells fill in (auto-registered by
-- entity-table triggers but never loaded); cascade + audits start
-- watching the new states; monthly DP coverage pipeline runs for them
-- on next monthly tick.
--
-- Cost implications:
--   - Weather grid ~3× cell count growth; proportional Open-Meteo calls
--   - monthly_dog_park_coverage ~$40-120 one-time across 8 states
--   - Codify gap fill LLM cost for new states
--
-- Per [[paired-functions-port-fixes-both-sides]]: this aligns dog-park
-- pipeline operational scope with beach-pipeline scope (which was
-- never state-gated).

UPDATE public.orch_jobs
   SET scope_states = ARRAY[
         'CA','OR','WA','MD','UT',
         'MA','MI','VA','NH','OH','DE','AL','RI'
       ],
       updated_at = now()
 WHERE scope_states @> ARRAY['CA','OR','WA','MD','UT']
   AND array_length(scope_states, 1) = 5;
