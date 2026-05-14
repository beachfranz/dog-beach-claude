"""Time-based schedules — cron-style triggers.

ALL schedules default STOPPED. Toggle ON in the UI when ready to switch
from pg_cron-based triggers (daily_beach_refresh_nightly, hourly-beach-
now-refresh) to Dagster-managed.

Schedules:
  daily_beach_refresh_schedule   09:00 UTC daily — fires daily_refresh_fire for MVP+ states
  hourly_now_refresh_schedule    every hour at :00 — fires Phase 32 for MVP+ states
  weekly_pipeline_health_schedule Sun 06:00 UTC — fires field_population_check for MVP+ states

When toggling ON, disable the corresponding pg_cron job:
  SELECT cron.unschedule('daily_beach_refresh_nightly');
  SELECT cron.unschedule('hourly-beach-now-refresh');
"""

from dagster import (
    schedule,
    ScheduleEvaluationContext,
    RunRequest,
    DefaultScheduleStatus,
)

from ..jobs import daily_refresh_job, pipeline_health_audit_job


@schedule(
    cron_schedule="0 9 * * *",  # 09:00 UTC daily
    job=daily_refresh_job,
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Replaces pg_cron 'daily_beach_refresh_nightly'. Fires "
        "daily_refresh_fire for each MVP+ state. Toggle ON only after "
        "disabling the pg_cron equivalent to avoid duplicate runs."
    ),
)
def daily_beach_refresh_schedule(context: ScheduleEvaluationContext):
    for state in ("CA", "OR", "WA"):
        yield RunRequest(
            partition_key=state,
            run_key=f"daily-{state}-{context.scheduled_execution_time.date().isoformat()}",
        )


@schedule(
    cron_schedule="0 * * * *",  # every hour
    job=daily_refresh_job,  # same job; consumer interprets hourly cadence via downstream get-beach-now logic
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Replaces pg_cron 'hourly-beach-now-refresh'. Hourly NOW refresh "
        "for MVP+ states. Toggle ON only after disabling pg_cron equivalent."
    ),
)
def hourly_now_refresh_schedule(context: ScheduleEvaluationContext):
    for state in ("CA", "OR", "WA"):
        yield RunRequest(
            partition_key=state,
            run_key=f"hourly-{state}-{context.scheduled_execution_time.isoformat()}",
        )


@schedule(
    cron_schedule="0 6 * * 0",  # Sun 06:00 UTC
    job=pipeline_health_audit_job,
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Weekly pipeline-health audit — runs field_population_check for each "
        "MVP+ state. Logs threshold violations to Dagster run history; the "
        "asset itself raises on hard failures."
    ),
)
def weekly_pipeline_health_schedule(context: ScheduleEvaluationContext):
    for state in ("CA", "OR", "WA"):
        yield RunRequest(
            partition_key=state,
            run_key=f"health-{state}-{context.scheduled_execution_time.date().isoformat()}",
        )
