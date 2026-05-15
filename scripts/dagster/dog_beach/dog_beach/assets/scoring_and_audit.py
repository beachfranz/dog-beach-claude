"""Phases 32-33 — daily scoring + final population audit.

Phase 32 (daily_refresh_fire): POSTs to supabase/functions/v1/daily-beach-refresh
  with the state's scoreable location_ids. The function fans out per beach
  to fetch weather + tides + crowds and write beach_day_recommendations +
  beach_day_hourly_scores. Per-batch BATCH=25 from run_state_pipeline.py.

Phase 33 (field_population_check): Runs scripts/audit/state_population_audit.py
  --check, which exits non-zero if any hard threshold fails (county_fips=100%,
  name_source=100%, today_rec>=95%, BEP has state_dogs_policy_v1,
  tier-1+2 > 0).
"""

import os
import re

from dagster import (
    asset,
    AssetExecutionContext,
    Config,
    MaterializeResult,
    MetadataValue,
)


class DailyRefreshConfig(Config):
    """Run-time overrides for daily_refresh_fire.

    skip_recent_hours: forwarded to daily-beach-refresh; default 24 mirrors
      run_state_pipeline.py. Pass 0 to bypass the skip-recent gate
      (ad-hoc re-scores after upstream dogs-policy fixes).

    force_location_ids: if set, use this list instead of querying
      beaches_gold by partition state. Partition is still required (for
      run record-keeping) but the actual work is scoped to this list.
      Use for narrow ad-hoc rescores; leave null for the normal per-state
      fan-out.
    """
    skip_recent_hours: int = 24
    force_location_ids: list[str] | None = None

from ..partitions import state_partitions
from ..resources import (
    PostgresPoolerResource,
    SupabaseEdgeFunctionResource,
    SubprocessResource,
)
from .per_fid_enrichment import photos_wikimedia


# ════════════════════════════════════════════════════════════════════════
#  Phase 32 — daily_refresh_fire (per-state)
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[photos_wikimedia],
    group_name="phase_29_to_33_per_fid",
    description=(
        "Fires daily-beach-refresh edge function for state's scoreable beaches. "
        "Batches of 25 location_ids per POST (mirrors run_state_pipeline.py "
        "BATCH=25 to avoid timeout). Function returns per-beach scoring result."
    ),
)
def daily_refresh_fire(
    context: AssetExecutionContext,
    config: DailyRefreshConfig,
    postgres: PostgresPoolerResource,
    supabase_edge: SupabaseEdgeFunctionResource,
) -> MaterializeResult:
    state = context.partition_key

    if config.force_location_ids:
        ids = list(config.force_location_ids)
        context.log.info(f"force_location_ids set ({len(ids)} ids); skipping state query")
    else:
        conn = postgres.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT location_id FROM public.beaches_gold "
                    " WHERE state=%s AND is_active AND scoring_tier IN ('daily','hourly')",
                    (state,),
                )
                ids = [r[0] for r in cur.fetchall()]
        finally:
            conn.close()

    if not ids:
        return MaterializeResult(metadata={"state": state, "beaches": 0, "skipped": True})

    # BATCH=12 to stay under the edge function's 150s idle timeout —
    # 25/batch hit 504s before the skip_recent_hours feature shipped.
    BATCH = 12
    ok = 0
    failed = 0
    total_skipped_recent = 0
    for i in range(0, len(ids), BATCH):
        batch = ids[i:i + BATCH]
        try:
            result = supabase_edge.call(
                "daily-beach-refresh",
                body={
                    "location_ids": batch,
                    "tide_window_days": 7,
                    "skip_recent_hours": config.skip_recent_hours,
                },
                timeout=600,
            )
            # daily-beach-refresh returns skipped_recent: N in the body
            skipped = result.get("skipped_recent", 0) if isinstance(result, dict) else 0
            total_skipped_recent += skipped
            ok += len(batch) - skipped
        except Exception as e:
            failed += len(batch)
            context.log.warning(f"Batch {i//BATCH + 1} failed: {e!s}"[:200])

    return MaterializeResult(
        metadata={
            "state": MetadataValue.text(state),
            "beaches_total": MetadataValue.int(len(ids)),
            "beaches_ok": MetadataValue.int(ok),
            "beaches_failed": MetadataValue.int(failed),
            "beaches_skipped_recent": MetadataValue.int(total_skipped_recent),
        }
    )


# ════════════════════════════════════════════════════════════════════════
#  Phase 33 — field_population_check (per-state, end-of-pipeline audit)
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[daily_refresh_fire],
    group_name="phase_29_to_33_per_fid",
    description=(
        "End-of-pipeline drift/coverage audit. Runs state_population_audit.py "
        "--check, exits non-zero on threshold failures (county_fips=100%, "
        "name_source=100%, today_rec>=95%, BEP has state_dogs_policy_v1, "
        "tier-1+2 > 0). Prints the full report regardless."
    ),
)
def field_population_check(
    context: AssetExecutionContext,
    subproc: SubprocessResource,
) -> MaterializeResult:
    state = context.partition_key
    result = subproc.run(
        "scripts/audit/state_population_audit.py",
        args=["--state", state, "--check"],
        timeout=300,
    )
    # Always log the report
    context.log.info(result.stdout)

    if result.returncode != 0:
        raise RuntimeError(
            f"field_population_check FAIL for {state} (exit {result.returncode})"
        )
    return MaterializeResult(
        metadata={
            "state": MetadataValue.text(state),
            "audit_passed": True,
            "log_tail": MetadataValue.text(result.stdout[-3000:]),
        }
    )
