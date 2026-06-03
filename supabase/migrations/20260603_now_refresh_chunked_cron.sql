-- Switch hourly NOW refresh jobs from hourly-at to chunked every-5-min,
-- adding per-fire `limit: 100` to keep edge fn under WORKER_RESOURCE_LIMIT.
--
-- Before:
--   hourly_beach_now_refresh: hourly_at :05, no limit → tried to
--     refresh all 711 hourly-tier beaches in one Promise.all → HTTP 546.
--   hourly_dog_park_now_refresh: hourly_at :10, no limit → same shape
--     for 782 MVP+ scoreable parks.
--
-- After: every 5 min with limit=100 + skip_recent_hours=0.9.
--   Beach drain math: 711 ÷ 100 × 5 min = 36 min < 54 min skip window ✓
--   DP    drain math: 782 ÷ 100 × 5 min = 39 min < 54 min skip window ✓
--
-- Same chunked-drain pattern that recovered daily-beach-refresh from
-- the same failure mode on 2026-05-30. Combined with the now-deployed
-- limit + oldest-first sort inside get-beach-now / get-dog-park-now,
-- each fire processes 100 stalest beaches and exits well under the
-- 150s edge-fn budget.

BEGIN;

UPDATE public.orch_jobs
   SET cadence_kind   = 'every_n_minutes',
       cadence_param  = '5',
       worker_payload = jsonb_build_object(
         'skip_recent_hours', 0.9,
         'limit', 100
       ),
       description = 'Chunked NOW refresh — every 5 min, limit=100 stalest beaches per fire, skip beaches whose is_now row is <54min old.',
       updated_at = now()
 WHERE job_name = 'hourly_beach_now_refresh';

UPDATE public.orch_jobs
   SET cadence_kind   = 'every_n_minutes',
       cadence_param  = '5',
       worker_payload = jsonb_build_object(
         'skip_recent_hours', 0.9,
         'limit', 100
       ),
       description = 'Chunked NOW refresh — every 5 min, limit=100 stalest parks per fire (MVP+ scope), skip parks whose is_now row is <54min old.',
       updated_at = now()
 WHERE job_name = 'hourly_dog_park_now_refresh';

COMMIT;
