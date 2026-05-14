"""AssetCheck for Phase 10 — operators_for_state.

Replaces the run_state_pipeline.py PHASES criterion `select (count(*) > 0)
from operators where state_code=$STATE and is_active`. That's too lax —
it passes as long as Pass 1 (TIGER cities) wrote anything. After 2026-05-13
the expectation is broader operator coverage.

This check requires:
  - >= 3 distinct levels of operators for the state (city + county +
    federal minimum)
  - For non-CA states: >= 5 operators at level in (state, special-district,
    tribal) — verifies that v2 Passes 4-7 actually populated, not just
    that Pass 1/2/3 ran. CA passes this trivially via legacy CPAD seeding.
"""

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
    AssetCheckExecutionContext,
    MetadataValue,
)

from ..assets.operator_seeding import operators_for_state
from ..resources import PostgresPoolerResource


@asset_check(
    asset=operators_for_state,
    description=(
        "Phase 10 breadth check: >=3 distinct levels overall and "
        ">=5 non-basic-level operators for non-CA states."
    ),
)
def operators_for_state_has_breadth(
    context: AssetCheckExecutionContext,
    postgres: PostgresPoolerResource,
) -> AssetCheckResult:
    state = context.partition_key

    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(DISTINCT level) AS distinct_levels,
                    COUNT(*) FILTER (
                      WHERE level IN ('state','special-district','tribal')
                    ) AS non_basic_count,
                    COUNT(*) AS total_active
                FROM public.operators
                WHERE state_code = %s
                  AND is_active
                  AND parent_operator_id IS NULL
                """,
                (state,),
            )
            distinct_levels, non_basic_count, total_active = cur.fetchone()
    finally:
        conn.close()

    is_ca = state == "CA"
    passed = bool(
        distinct_levels >= 3
        and (is_ca or non_basic_count >= 5)
        and total_active > 0
    )

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "state": MetadataValue.text(state),
            "distinct_levels": MetadataValue.int(distinct_levels),
            "non_basic_count": MetadataValue.int(non_basic_count),
            "total_active": MetadataValue.int(total_active),
            "is_ca": MetadataValue.bool(is_ca),
        },
        description=(
            f"state={state} distinct_levels={distinct_levels} "
            f"non_basic={non_basic_count} total={total_active}"
        ),
    )
