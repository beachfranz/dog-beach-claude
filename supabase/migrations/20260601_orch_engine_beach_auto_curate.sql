-- 20260601_orch_engine_beach_auto_curate.sql
--
-- Slot beach auto-curate into the orch pipeline. Cadence: weekly Wed
-- 11:00 UTC, off-hours, after the weekly_codify_gap_clone (Mon 13:00)
-- has finished and well clear of weekly_pipeline_health (Sun 15:00) and
-- weekly_tide_refresh (Sun 08:00).
--
-- Worker is GH Actions (dispatch-github-workflow). See
-- .github/workflows/beach_auto_curate.yml. SQL-function port would
-- require fixing get_beach_photos_diverse's per-call temp-table lock
-- leak first ([[get-beach-photos-diverse-temp-table-lock-leak]]); for
-- now Python+GH Actions matches the auto_curate.py chunk-commit pattern
-- the script already implements.
--
-- Shadow mode TRUE initially per safety convention — Franz flips off
-- after a successful manual fire.

BEGIN;

INSERT INTO public.orch_jobs (
  job_name,
  description,
  cadence_kind,
  cadence_param,
  worker_kind,
  worker_target,
  worker_payload,
  scope_states,
  enabled,
  shadow_mode,
  skip_if_succeeded_within
) VALUES (
  'weekly_beach_auto_curate',
  'Run auto_curate.py over all active gold beaches; pick top 6 best+diverse photos and stamp curated_by=Curated:AI. Skips human-curated and fresh-within-7d beaches. Wraps the canonical script in GH Actions because get_beach_photos_diverse leaks pg_temp locks if looped in a single SQL transaction.',
  'weekly_on_at',
  'wed:11:00',
  'edge_function',
  'dispatch-github-workflow',
  jsonb_build_object(
    'workflow', 'beach_auto_curate.yml',
    'ref',      'main',
    'inputs',   jsonb_build_object()  -- defaults: n=6, all states, skip-fresh=7
  ),
  NULL,  -- not state-scoped (single workflow handles full catalog)
  TRUE,
  TRUE,  -- shadow until Franz validates a manual fire
  interval '6 days'  -- protect against weekly-fire reruns within a week
)
ON CONFLICT (job_name) DO UPDATE
   SET description     = EXCLUDED.description,
       cadence_kind    = EXCLUDED.cadence_kind,
       cadence_param   = EXCLUDED.cadence_param,
       worker_kind     = EXCLUDED.worker_kind,
       worker_target   = EXCLUDED.worker_target,
       worker_payload  = EXCLUDED.worker_payload,
       updated_at      = now();

COMMIT;
