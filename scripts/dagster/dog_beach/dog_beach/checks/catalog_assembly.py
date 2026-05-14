"""AssetChecks for catalog assembly phases.

Each check mirrors the corresponding `criterion` SQL from
scripts/run_state_pipeline.py PHASES list. Severity=WARN by default —
surfaces in UI but doesn't block downstream materialization (matches
the soft-logging behavior of pipeline_phase_status).

Where the underlying asset already RAISES on criterion fail
(field_population_check), we add an additional AssetCheck for dashboard
visibility — the raise stops the run but the check makes the failure
queryable post-hoc.
"""

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
    AssetCheckExecutionContext,
    MetadataValue,
)

from ..assets.catalog_assembly import (
    promote_to_gold,
    name_source,
    catchment_refresh,
)
from ..assets.scoring_and_audit import daily_refresh_fire
from ..assets.per_fid_enrichment import descriptions
from ..resources import PostgresPoolerResource


def _query_one(postgres, sql: str, params: tuple = ()):
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    finally:
        conn.close()


# ── Phase 14 promote ──────────────────────────────────────────────────

@asset_check(
    asset=promote_to_gold,
    description="Every active beach in state has county_fips and state set (#2/#18 guard).",
)
def promote_to_gold_complete(
    context: AssetCheckExecutionContext,
    postgres: PostgresPoolerResource,
) -> AssetCheckResult:
    state = context.partition_key
    passed = bool(_query_one(postgres,
        "SELECT public.assert_promote_complete_for_state(%s)",
        (state,))[0])
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={"state": MetadataValue.text(state)},
    )


# ── Phase 17 name_source ───────────────────────────────────────────────

@asset_check(
    asset=name_source,
    description="Every active beach in state has name_source set.",
)
def name_source_complete(
    context: AssetCheckExecutionContext,
    postgres: PostgresPoolerResource,
) -> AssetCheckResult:
    state = context.partition_key
    row = _query_one(postgres,
        "SELECT count(*) FILTER (WHERE name_source IS NULL), count(*) "
        "  FROM public.beaches_gold WHERE state=%s AND is_active",
        (state,))
    missing, total = int(row[0]), int(row[1])
    return AssetCheckResult(
        passed=missing == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "state": MetadataValue.text(state),
            "missing": MetadataValue.int(missing),
            "total": MetadataValue.int(total),
        },
    )


# ── Phase 20 catchment_refresh ─────────────────────────────────────────
# (Phase 19 align_scoreable retired 2026-05-13 with is_scoreable column.)

@asset_check(
    asset=catchment_refresh,
    description="Every active beach in state has scoring_tier set.",
)
def catchment_refresh_complete(
    context: AssetCheckExecutionContext,
    postgres: PostgresPoolerResource,
) -> AssetCheckResult:
    state = context.partition_key
    row = _query_one(postgres,
        "SELECT count(*) FILTER (WHERE scoring_tier IS NULL), count(*) "
        "  FROM public.beaches_gold WHERE state=%s AND is_active",
        (state,))
    missing, total = int(row[0]), int(row[1])
    return AssetCheckResult(
        passed=missing == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "state": MetadataValue.text(state),
            "missing": MetadataValue.int(missing),
            "total": MetadataValue.int(total),
        },
    )


# ── Phase 30 descriptions ──────────────────────────────────────────────

@asset_check(
    asset=descriptions,
    description="At least 50% of state's tier-1+2 beaches have a description row.",
)
def descriptions_coverage(
    context: AssetCheckExecutionContext,
    postgres: PostgresPoolerResource,
) -> AssetCheckResult:
    state = context.partition_key
    row = _query_one(postgres,
        "SELECT "
        " count(*) FILTER (WHERE bd.arena_group_id IS NOT NULL), "
        " count(*) "
        "FROM public.beaches_gold g "
        "JOIN public.beach_dog_policy bdp ON bdp.arena_group_id=g.fid "
        "LEFT JOIN public.beach_descriptions bd ON bd.arena_group_id=g.fid "
        "WHERE g.state=%s AND g.is_active "
        "  AND public.beach_location_tier(bdp.dogs_allowed, bdp.has_off_leash, "
        "          bdp.has_on_leash, bdp.dogs_prohibited_start::text) "
        "      IN ('1_off-leash','2_on-leash')",
        (state,))
    have, total = int(row[0]), int(row[1])
    coverage = (have / total) if total else 1.0
    return AssetCheckResult(
        passed=coverage >= 0.5,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "state": MetadataValue.text(state),
            "have_description": MetadataValue.int(have),
            "tier_1_2_total": MetadataValue.int(total),
            "coverage_pct": MetadataValue.float(round(coverage * 100, 1)),
        },
    )


# ── Phase 32 daily_refresh_fire ────────────────────────────────────────

@asset_check(
    asset=daily_refresh_fire,
    description="Today's recommendation exists for >= 95% of scoreable beaches.",
)
def daily_refresh_coverage(
    context: AssetCheckExecutionContext,
    postgres: PostgresPoolerResource,
) -> AssetCheckResult:
    state = context.partition_key
    row = _query_one(postgres,
        "WITH sc AS (SELECT count(*) c FROM public.beaches_gold "
        "             WHERE state=%s AND is_active AND scoring_tier IN ('daily','hourly')), "
        "     rec AS (SELECT count(DISTINCT r.location_id) c "
        "               FROM public.beach_day_recommendations r "
        "               JOIN public.beaches_gold g ON g.location_id=r.location_id "
        "              WHERE g.state=%s AND r.local_date=current_date) "
        "SELECT sc.c, rec.c FROM sc, rec",
        (state, state))
    scoreable, with_rec = int(row[0]), int(row[1])
    coverage = (with_rec / scoreable) if scoreable else 1.0
    return AssetCheckResult(
        passed=coverage >= 0.95,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "state": MetadataValue.text(state),
            "scoreable": MetadataValue.int(scoreable),
            "with_today_rec": MetadataValue.int(with_rec),
            "coverage_pct": MetadataValue.float(round(coverage * 100, 1)),
        },
    )
