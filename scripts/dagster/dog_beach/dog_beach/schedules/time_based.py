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

from ..jobs import (
    daily_refresh_job,
    pipeline_health_audit_job,
    weather_grid_refresh_job,
    weather_grid_inventory_job,
    dog_park_coverage_job,
)


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


# ── Weather grid (W1.7 + W1.8) ────────────────────────────────────────

@schedule(
    cron_schedule="0 * * * *",  # every hour at :00
    job=weather_grid_refresh_job,
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "W1.7 — hourly weather_grid refresh. Per-cell tier-based stale "
        "thresholds in the asset (hot 1h, warm 6h, cold 24h) naturally "
        "differentiate cadence: most fires refresh just the hot cells, "
        "warm+cold cells refresh less often. See [[weather-grid-reference-layer]]."
    ),
)
def hourly_weather_grid_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(
        run_key=f"weather-grid-{context.scheduled_execution_time.isoformat()}",
    )


@schedule(
    cron_schedule="0 4 * * *",  # daily at 04:00 UTC
    job=weather_grid_inventory_job,
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "W1.8 — daily backstop. Rebuilds weather_grid from active entity "
        "tables in case triggers drift. Cheap insurance."
    ),
)
def daily_weather_grid_inventory_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(
        run_key=f"weather-grid-inventory-{context.scheduled_execution_time.date().isoformat()}",
    )


# ── Dog-park coverage (monthly per state) ──────────────────────────────

@schedule(
    cron_schedule="0 7 1 * *",  # 1st of month, 07:00 UTC
    job=dog_park_coverage_job,
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Monthly re-run of dog-park coverage pipeline per state. Default "
        "STOPPED — toggle ON when ready. Re-walks operator catalogs "
        "(catches new parks added since last run), re-ingests, re-extracts. "
        "Per pin [[dog-park-coverage-playbook]] — ~$5-15 per state per run."
    ),
)
def monthly_dog_park_coverage_schedule(context: ScheduleEvaluationContext):
    from ..assets.dog_park_coverage import DOG_PARK_STATES
    for state in DOG_PARK_STATES.get_partition_keys():
        yield RunRequest(
            partition_key=state,
            run_key=f"dpcov-{state}-{context.scheduled_execution_time.date().isoformat()}",
        )
