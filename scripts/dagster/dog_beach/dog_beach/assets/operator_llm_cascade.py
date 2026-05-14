"""Phases 26-28 — operator LLM extraction + merge + BEP cascade.

Phase 26 (operator_llm_extract): per-operator, dynamically partitioned.
Each operator gets its own partition + run history. Wraps
scripts/extract_operator_dogs_policy.py via SubprocessResource.

Phase 27 (operator_merge): GLOBAL — merge_operator_dogs_policy.py is
not state-scoped. Modeled as an unpartitioned asset that runs once and
serves as a downstream gate for all states' bep_refire phases. Wraps
scripts/one_off/merge_operator_dogs_policy.py.

Phase 28 (bep_refire): per-state. Calls public.refire_bep_cascade(fids)
with the state's tier-1+2 fids. Re-emits BEP for 5 regen field_groups
and propagates to consumer tables.

Cost model (Phase 26):
  ~$0.30 per operator (Tavily + 3-pass Sonnet)
  ~5 ops × 3min/chunk by default → 1 op = 600s budget
  Per-operator partition retry via Dagster's RetryPolicy
"""

import re
import sys

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    Backoff,
)

from ..partitions import state_partitions, operator_partitions
from ..resources import PostgresPoolerResource, SubprocessResource


# ════════════════════════════════════════════════════════════════════════
#  Phase 26 — operator_policy_extraction (per-operator dynamic partition)
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=operator_partitions,
    group_name="phase_26_to_28_operator_llm",
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=30,
        backoff=Backoff.EXPONENTIAL,
    ),
    description=(
        "Per-operator LLM extraction (Tavily site/web search + Sonnet 3-pass). "
        "One Dagster partition per operator. Each call writes one or more "
        "rows to operator_policy_extractions. Cost ~$0.30/operator. "
        "Per-partition retry handles transient Tavily/Anthropic failures. "
        "Skipped if extraction <24h old (mirrors --skip-recent semantics). "
        ""
        "WARNING: bypassing --skip-recent (e.g. re-running an operator that "
        "was extracted hours ago) will incur the full LLM cost again."
    ),
)
def operator_policy_extraction(
    context: AssetExecutionContext,
    subproc: SubprocessResource,
) -> MaterializeResult:
    operator_id = int(context.partition_key)
    context.log.info(f"Extracting operator policy for operator_id={operator_id}")

    result = subproc.run(
        "scripts/extract_operator_dogs_policy.py",
        args=["--ids", str(operator_id), "--skip-recent", "24"],
        timeout=900,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"extract_operator_dogs_policy.py exit {result.returncode}: "
            f"{result.stderr[-500:]}"
        )

    # Parse summary line from stdout (mirrors _parse_op_extract).
    rows_written = 0
    policy_found = None
    m = re.search(r"upserted\s+(\d+)", result.stdout)
    if m:
        rows_written = int(m.group(1))
    m = re.search(r"policy_found=(true|false)", result.stdout, re.IGNORECASE)
    if m:
        policy_found = m.group(1).lower() == "true"

    return MaterializeResult(
        metadata={
            "operator_id": MetadataValue.int(operator_id),
            "rows_written": MetadataValue.int(rows_written),
            "policy_found": MetadataValue.bool(bool(policy_found)) if policy_found is not None else MetadataValue.text("unknown"),
            "log_tail": MetadataValue.text(result.stdout[-2000:]),
        }
    )


# ════════════════════════════════════════════════════════════════════════
#  Phase 26 (state fanout) — operator_llm_extract_for_state
# ════════════════════════════════════════════════════════════════════════
#
# Per-state convenience asset that fans out across all operators with
# beaches in the state. Useful when launching a state for the first time.
# Wraps run_state_pipeline.py's action_operator_llm_extract pattern
# (chunked subprocess) — preserved as fallback for cases where the
# per-operator DynamicPartitions model isn't yet populated.

@asset(
    partitions_def=state_partitions,
    group_name="phase_26_to_28_operator_llm",
    description=(
        "State fanout for Phase 26. Calls extract_operator_dogs_policy.py "
        "with all operator IDs returned by state_operator_ids_with_beaches(state), "
        "chunked at 5 ops/subprocess per the chunked-subprocess design rule. "
        "Use this for first-time state launches. For ongoing per-operator "
        "extraction (e.g. when a new operator is added), prefer the per-"
        "operator partitioned asset operator_policy_extraction + sensor."
    ),
)
def operator_llm_extract_for_state(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
    subproc: SubprocessResource,
) -> MaterializeResult:
    state = context.partition_key

    # 2026-05-13 cost gate: only operators "definitively assigned" to
    # a scoreable (daily/hourly tier) beach via BEP attribution. Prevents
    # extracting policy for the 3000+ raw OR operators when only ~60
    # actually touch a scoreable beach. See:
    #   - migration 20260513_operator_ids_for_scoreable_beaches.sql
    #   - migration 20260513_city_county_governance_populator.sql
    #     (added city/county FK-based attribution; before this only
    #      PAD-US federal/state ops were "definitive")
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT array_agg(distinct operator_id) "
                "  FROM public.state_operator_ids_for_scoreable_beaches(%s)",
                (state,),
            )
            ids = cur.fetchone()[0] or []
    finally:
        conn.close()

    if not ids:
        return MaterializeResult(
            metadata={"state": state, "operators": 0, "skipped": True}
        )

    context.log.info(f"Extracting for {len(ids)} operators in {state}")

    # Chunk into groups of 5 (mirrors run_state_pipeline.py).
    chunk_size = 5
    total_rows = 0
    chunks_done = 0
    chunks_failed = 0
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i:i + chunk_size]
        result = subproc.run(
            "scripts/extract_operator_dogs_policy.py",
            args=["--ids", ",".join(str(x) for x in chunk), "--skip-recent", "24"],
            timeout=600,
        )
        if result.returncode != 0:
            chunks_failed += 1
            context.log.warning(
                f"Chunk {chunks_done+1} failed (operators {chunk[0]}..{chunk[-1]}): "
                f"{result.stderr[-300:]}"
            )
            continue
        m = re.search(r"upserted\s+(\d+)", result.stdout)
        if m:
            total_rows += int(m.group(1))
        chunks_done += 1

    return MaterializeResult(
        metadata={
            "state": MetadataValue.text(state),
            "operators_total": MetadataValue.int(len(ids)),
            "chunks_done": MetadataValue.int(chunks_done),
            "chunks_failed": MetadataValue.int(chunks_failed),
            "rows_written": MetadataValue.int(total_rows),
        }
    )


# ════════════════════════════════════════════════════════════════════════
#  Phase 27 — operator_merge (GLOBAL — not state-scoped)
# ════════════════════════════════════════════════════════════════════════

@asset(
    group_name="phase_26_to_28_operator_llm",
    description=(
        "GLOBAL operator_merge: scripts/one_off/merge_operator_dogs_policy.py "
        "consumes operator_policy_extractions and upserts canonical rows into "
        "operator_dogs_policy. Not state-scoped — merges across all states' "
        "extractions. Idempotent."
    ),
)
def operator_merge(
    context: AssetExecutionContext,
    subproc: SubprocessResource,
) -> MaterializeResult:
    result = subproc.run(
        "scripts/one_off/merge_operator_dogs_policy.py",
        timeout=600,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"merge_operator_dogs_policy.py exit {result.returncode}: "
            f"{result.stderr[-500:]}"
        )
    rows = 0
    m = re.search(r"upserted\s+(\d+)/", result.stdout)
    if m:
        rows = int(m.group(1))
    return MaterializeResult(
        metadata={
            "rows_upserted": MetadataValue.int(rows),
            "log_tail": MetadataValue.text(result.stdout[-2000:]),
        }
    )


# ════════════════════════════════════════════════════════════════════════
#  Phase 28 — bep_refire (per-state)
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[operator_merge, operator_llm_extract_for_state],
    group_name="phase_26_to_28_operator_llm",
    description=(
        "Refire BEP cascade for state's tier-1+2 fids. Calls "
        "public.refire_bep_cascade(fids) which deletes 5 regen field_groups "
        "from BEP for each fid and re-emits via the populator chain. "
        "Resolves canonical evidence and promotes to consumer tables "
        "(beach_dog_policy, beach_amenities)."
    ),
)
def bep_refire(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    state = context.partition_key
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            # Get tier-1+2 fids for state (mirrors _state_tier12_fids).
            cur.execute(
                "SELECT array_agg(distinct g.fid) "
                "  FROM public.beaches_gold g "
                "  LEFT JOIN public.beach_dog_policy bdp ON bdp.arena_group_id=g.fid "
                " WHERE g.state=%s AND g.is_active AND g.scoring_tier IN ('daily','hourly')",
                (state,),
            )
            fids = cur.fetchone()[0] or []

            if not fids:
                return MaterializeResult(
                    metadata={"state": state, "fids": 0, "skipped": True}
                )

            cur.execute("SET statement_timeout = '900s'")
            cur.execute(
                "SELECT * FROM public.refire_bep_cascade(%s)", (fids,)
            )
            result = cur.fetchone()
            rows = int(result[0] if result else 0)
        conn.commit()
    finally:
        conn.close()
    return MaterializeResult(
        metadata={
            "state": MetadataValue.text(state),
            "fids_processed": MetadataValue.int(len(fids)),
            "rows_affected": MetadataValue.int(rows),
        }
    )
