-- v3 weight tune (Franz 2026-06-18): car_heat_status was the single biggest
-- systematic drag on score_v3 on warm days (~7,500 pts off 700-800 beaches) —
-- but "don't leave your dog in a hot car" is a PARKING-LOT warning, not a
-- beach-quality signal. Make it show-but-don't-score: enabled=false removes
-- the advisory-penalty contribution while the advisory STILL DISPLAYS (find.html
-- chips + beach.html Parking tile / Cautions card read beach_advisory directly,
-- not advisory_score_config). Same for the symmetric car_cold_status. Both
-- entity types.
--
-- Live in compute_*_hourly_v2; stored composite_score_v3 (find ranking) recovers
-- the points on the next nightly bulk run.

UPDATE public.advisory_score_config
   SET enabled = false
 WHERE event_type IN ('car_heat_status', 'car_cold_status');
