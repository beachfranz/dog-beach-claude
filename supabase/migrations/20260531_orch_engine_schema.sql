-- 20260531_orch_engine_schema.sql
--
-- Orchestration engine for cron jobs. Replaces the laptop-bound Dagster
-- daemon as the production scheduler. Lives entirely in Supabase Postgres
-- so there is no external infrastructure to maintain.
--
-- Two tables:
--   orch_jobs — catalog of all jobs (cadence, deps, cost gate, worker target)
--   orch_runs — audit log (per-minute decisions; fired / skipped / failed / shadowed)
--
-- The orchestrator function (orch_tick) runs once a minute via pg_cron and
-- decides what fires this minute based on:
--   1. Cron window match (cadence_kind enum or custom cron expression)
--   2. Dependency freshness (depends_on must have succeeded within depends_max_age)
--   3. Cost gate (optional cost_check_fn returning bool)
--   4. Concurrency (is_running flag prevents double-fire)
--   5. Skip-if-succeeded-within (idempotent recent runs short-circuit)
--   6. Shadow mode (log-only for safe parallel running with legacy schedules)
--
-- Workers are either edge functions (via net.http_post) or SQL functions
-- (via dynamic EXECUTE). The existing _fire_daily_beach_refresh() etc.
-- functions can be reused directly as worker_target.

BEGIN;

-- ─── Catalog ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.orch_jobs (
  job_name                  TEXT PRIMARY KEY,
  description               TEXT,

  -- Cadence (hybrid: enum for common patterns + cron for custom)
  cadence_kind              TEXT NOT NULL
                            CHECK (cadence_kind IN
                              ('every_n_minutes','hourly_at','daily_at',
                               'weekly_on_at','monthly_on_at','custom_cron')),
  cadence_param             TEXT,
    -- every_n_minutes  : 'N'                e.g. '2'  (fires every 2 min)
    -- hourly_at        : 'M'                e.g. '5'  (fires at HH:05)
    -- daily_at         : 'HH:MM'            e.g. '04:00'
    -- weekly_on_at     : 'dow:HH:MM'        e.g. 'sun:08:00'
    -- monthly_on_at    : 'DOM:HH:MM'        e.g. '1:08:00'
  cron_expr                 TEXT,
    -- custom_cron      : standard 5-field cron '* * * * *'

  -- Dependencies — depends_on jobs must have succeeded within depends_max_age
  depends_on                TEXT[] DEFAULT ARRAY[]::TEXT[],
  depends_max_age           INTERVAL DEFAULT '36 hours',

  -- Cost gate (optional SQL fn returning BOOLEAN; true = work pending)
  -- Signature: <fn>(p_job public.orch_jobs) RETURNS BOOLEAN
  cost_check_fn             TEXT,

  -- Skip-if-succeeded-within — short-circuit if last_succeeded_at is recent
  skip_if_succeeded_within  INTERVAL,

  -- Worker
  worker_kind               TEXT NOT NULL
                            CHECK (worker_kind IN ('edge_function','sql_function','noop')),
  worker_target             TEXT NOT NULL,
    -- edge_function    : edge fn name (e.g. 'daily-beach-refresh')
    -- sql_function     : qualified function name (e.g. 'public._fire_daily_beach_refresh')
    --                    must take a jsonb arg (pass {} if no payload needed)
  worker_payload            JSONB DEFAULT '{}'::jsonb,

  -- MVP+ scope tag (informational; consumed by worker if needed)
  scope_states              TEXT[] DEFAULT ARRAY['CA','OR','WA','MD','UT']::TEXT[],

  -- Toggles
  enabled                   BOOLEAN NOT NULL DEFAULT TRUE,
  shadow_mode               BOOLEAN NOT NULL DEFAULT FALSE,

  -- Runtime state
  is_running                BOOLEAN NOT NULL DEFAULT FALSE,
  running_since             TIMESTAMPTZ,
  running_request_id        BIGINT,
  last_attempted_at         TIMESTAMPTZ,
  last_succeeded_at         TIMESTAMPTZ,
  last_failed_at            TIMESTAMPTZ,
  last_skip_reason          TEXT,
  last_error                TEXT,

  created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE public.orch_jobs IS
  'Orchestration catalog. One row per job. Edited manually; orch_tick reads.';

CREATE INDEX IF NOT EXISTS orch_jobs_enabled_idx ON public.orch_jobs (enabled);
CREATE INDEX IF NOT EXISTS orch_jobs_running_idx ON public.orch_jobs (is_running) WHERE is_running;

-- ─── Audit log ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.orch_runs (
  id                  BIGSERIAL PRIMARY KEY,
  job_name            TEXT NOT NULL REFERENCES public.orch_jobs(job_name) ON DELETE CASCADE,
  intended_fire_at    TIMESTAMPTZ NOT NULL,  -- minute boundary at decision time
  started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at            TIMESTAMPTZ,
  status              TEXT NOT NULL
                      CHECK (status IN
                        ('skipped_cron_window','skipped_dependency','skipped_cost',
                         'skipped_already_running','skipped_disabled',
                         'shadow','fired','succeeded','failed','timeout')),
  skip_reason         TEXT,
  worker_request_id   BIGINT,
  response_body       JSONB,
  duration_ms         BIGINT,
  error_msg           TEXT
);

COMMENT ON TABLE public.orch_runs IS
  'Per-tick decision log. One row per job per tick. Status indicates fire vs skip.';

CREATE INDEX IF NOT EXISTS orch_runs_job_started_idx
  ON public.orch_runs (job_name, started_at DESC);
CREATE INDEX IF NOT EXISTS orch_runs_status_started_idx
  ON public.orch_runs (status, started_at DESC) WHERE status IN ('fired','failed','timeout');

-- Prevent double-fire for the same job-minute (if orch_tick ever runs twice
-- in the same minute, the second insert is a noop for fired/succeeded rows).
CREATE UNIQUE INDEX IF NOT EXISTS orch_runs_no_dup_fire_uidx
  ON public.orch_runs (job_name, intended_fire_at)
  WHERE status IN ('fired','succeeded','failed','timeout');

COMMIT;
