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
    weather_advisories_job,
    codify_coverage_audit_job,
    codify_gap_clone_job,
    dog_park_data_quality_audit_job,
    weather_grid_refresh_job,
    weather_grid_inventory_job,
    dog_park_coverage_job,
    beach_discovery_job,
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


# ── Weather advisories refresh (Phase 32.5) ──────────────────────────

@schedule(
    cron_schedule="0 12 * * *",  # 12:00 UTC daily — 5am Pacific, 8am Eastern
    job=weather_advisories_job,
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Daily — derives beach_advisory rows for every scored beach. Runs "
        "compute_weather_advisories.py --all-scored. Fires at 12:00 UTC, "
        "after daily-beach-refresh has populated beach_day_recommendations "
        "and the hourly_status v2 substrate. Powers the Cautions card on "
        "beach.html via loadWaterConditionAdvisories()."
    ),
)
def daily_weather_advisories_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(
        run_key=f"advisories-{context.scheduled_execution_time.date().isoformat()}",
    )


# ── Codify coverage audit + gap clone (Phases 32.6 + 32.7) ───────────

@schedule(
    cron_schedule="30 12 * * *",  # 12:30 UTC daily — after daily refresh + advisories
    job=codify_coverage_audit_job,
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Daily — audits structured-codify coverage by state. Raises on "
        "regression below threshold so a state launch that adds beaches "
        "faster than the codify pipeline catches up surfaces quickly."
    ),
)
def daily_codify_coverage_audit_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(
        run_key=f"codify-audit-{context.scheduled_execution_time.date().isoformat()}",
    )


@schedule(
    cron_schedule="30 13 * * 1",  # 13:30 UTC Mondays — after codify gap clone
    job=dog_park_data_quality_audit_job,
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Weekly — surfaces dog-park amenity inconsistencies (lighting "
        "+ dawn-dusk hours). Raises on any detection so new inconsistencies "
        "from amenity re-extractions surface immediately."
    ),
)
def weekly_dog_park_data_quality_audit_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(
        run_key=f"dp-dq-{context.scheduled_execution_time.date().isoformat()}",
    )


@schedule(
    cron_schedule="0 13 * * 1",  # 13:00 UTC Mondays — after daily audit
    job=codify_gap_clone_job,
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Weekly — runs codify_clone_gap_beaches.py across all MVP+ states. "
        "Picks up newly-added beaches that fall into already-codified "
        "cities (clone from sibling). Reports unhandled list of "
        "research-needed jurisdictions for follow-up."
    ),
)
def weekly_codify_gap_clone_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(
        run_key=f"codify-clone-{context.scheduled_execution_time.date().isoformat()}",
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


# ── Beach discovery (annual per Franz 2026-05-27) ──────────────────────
# Beach discovery primarily runs when a state comes on-line via
# state_launch_job. This schedule re-walks each state once per year to
# pick up new beaches added to state-park sites or Wikipedia since the
# initial launch. State-park inventories change slowly so annual is the
# right cadence.

@schedule(
    cron_schedule="0 9 15 1 *",  # Jan 15, 09:00 UTC — annual
    job=beach_discovery_job,
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "ANNUAL re-walk of beach-discovery walkers per state (state-park "
        "+ Wikipedia + ingest). Default STOPPED — toggle ON when ready. "
        "Cron: Jan 15 09:00 UTC. Cost ~$12.50/year for all 50 states "
        "(~$0.25/state). Per Franz 2026-05-27 — state-park sites + "
        "Wikipedia articles change slowly enough that annual is sufficient. "
        "First-time state coverage happens in state_launch_job."
    ),
)
def annual_beach_discovery_schedule(context: ScheduleEvaluationContext):
    from ..partitions import state_partitions
    for state in state_partitions.get_partition_keys():
        yield RunRequest(
            partition_key=state,
            run_key=f"beach-disc-{state}-{context.scheduled_execution_time.date().isoformat()}",
        )
