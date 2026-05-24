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
#  Hourly status refresh + codify coverage check
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    group_name="phase_29_to_33_per_fid",
    description=(
        "Per-state idempotent refresh of beach_section_hour_status. Joins "
        "beach_day_hourly_scores × zone_rules sections to produce a "
        "(beach_fid, valid_date, section, hour) slice. Runs AFTER "
        "zone_rules_v2_refresh + operating_hours_refresh."
    ),
)
def hourly_status_refresh(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
    subproc: SubprocessResource,
) -> MaterializeResult:
    state = context.partition_key
    # Pull tier-1+2 fids first (the script chunks by --fids).
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT g.fid
                  FROM public.beaches_gold g
                  JOIN public.beach_dog_policy bdp ON bdp.arena_group_id = g.fid
                 WHERE g.state = %s AND g.is_active
                   AND public.beach_location_tier(
                         bdp.dogs_allowed, bdp.has_off_leash, bdp.has_on_leash,
                         bdp.dogs_prohibited_start::text
                       ) IN ('1_off-leash', '2_on-leash')
                 ORDER BY g.fid
                """,
                (state,),
            )
            fids = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    if not fids:
        return MaterializeResult(metadata={"state": state, "fids": 0, "skipped": True})

    # Chunked subprocess to refresh_beach_section_hour_status.py
    chunk_size = 100
    done = 0
    failed = 0
    for i in range(0, len(fids), chunk_size):
        chunk = fids[i:i + chunk_size]
        result = subproc.run(
            "scripts/refresh_beach_section_hour_status.py",
            args=["--fids", ",".join(str(x) for x in chunk)],
            timeout=600,
        )
        if result.returncode != 0:
            failed += 1
            context.log.warning(
                f"chunk {i//chunk_size + 1} failed (fids {chunk[0]}..{chunk[-1]}): "
                f"{(result.stderr or '')[-300:]}"
            )
            continue
        done += 1
    return MaterializeResult(
        metadata={
            "state": MetadataValue.text(state),
            "fids_total": MetadataValue.int(len(fids)),
            "chunks_done": MetadataValue.int(done),
            "chunks_failed": MetadataValue.int(failed),
        }
    )


@asset(
    partitions_def=state_partitions,
    group_name="phase_29_to_33_per_fid",
    description=(
        "Per-state codify coverage check. Counts active scoreable beaches "
        "without a policy_source attribution (uncovered_scoreable_beaches). "
        "Surfaces the structural gap that codify_cascade is supposed to close."
    ),
)
def codify_coverage_check(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    state = context.partition_key
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT public.count_uncovered_scoreable_beaches(%s)", (state,)
            )
            n_uncovered = cur.fetchone()[0]
            cur.execute(
                "SELECT COUNT(*) FROM public.beaches_gold "
                " WHERE state=%s AND is_active "
                "   AND scoring_tier IN ('daily','hourly')",
                (state,),
            )
            n_scoreable = cur.fetchone()[0]
    finally:
        conn.close()
    pct_covered = 100.0 * (1 - (n_uncovered / max(n_scoreable, 1)))
    return MaterializeResult(
        metadata={
            "state": MetadataValue.text(state),
            "scoreable": MetadataValue.int(n_scoreable),
            "uncovered": MetadataValue.int(n_uncovered),
            "pct_covered": MetadataValue.float(round(pct_covered, 1)),
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
