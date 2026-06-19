-- v3 weight tune (Franz 2026-06-18): uv_status was the broadest systematic
-- drag on score_v3 — it fires on most sunny daytime hours, so at weight 1.0 it
-- shaved otherwise-clean beaches everywhere. Halve it to 0.5 so a sunny day
-- (which is GOOD for a beach trip) stops being penalized as much. The uv≥11
-- absolute gate (score→0) and sky_pos are unchanged; this only softens the
-- advisory-penalty contribution. Applied to beach + dog_park for parity.
--
-- Takes effect live in compute_*_hourly_v2 (chart/badge); stored
-- composite_score_v3 (find ranking) refreshes on the next nightly bulk run.

UPDATE public.advisory_score_config
   SET weight = 0.5
 WHERE event_type = 'uv_status';
