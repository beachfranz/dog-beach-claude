"""Phases 26-28 — operator LLM extraction + merge + beach evidence rebuild.

Phase 26 (operator_llm_extract): per-operator, dynamically partitioned.
Each operator gets its own partition + run history. Wraps
scripts/extract_operator_dogs_policy.py via SubprocessResource.

Phase 27 (operator_merge): GLOBAL — merge_operator_dogs_policy.py is
not state-scoped. Modeled as an unpartitioned asset that runs once and
serves as a downstream gate for all states' rebuild_beach_evidence phases.
Wraps scripts/merge_operator_dogs_policy.py.

Phase 28 (rebuild_beach_evidence): per-state. Loops calling
public.build_beach_evidence(fid) for the state's tier-1+2 fids. Each
call clears regenerable BEP rows + re-runs the full populator/resolver
chain + promotes canonical to consumer tables. Per-fid autocommit
bounds memory (no monolithic refire_bep_cascade transaction).

Cost model (Phase 26):
  ~$0.30 per operator (Tavily + 3-pass Sonnet)
  ~5 ops × 3min/chunk by default → 1 op = 600s budget
  Per-operator partition retry via Dagster's RetryPolicy
"""

import ast
import re
import sys
import time as _time

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    Backoff,
)

from ..partitions import state_partitions, operator_partitions
from ..resources import PostgresPoolerResource, PostgresSessionResource, SubprocessResource
from .catalog_assembly import promote_to_gold, catchment_refresh
from .upstream_loaders import env_preflight


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
    # 2026-05-14: dep on catchment_refresh — Phase 26 must run AFTER
    # scoring_tier is set, otherwise the cost gate
    # (state_operator_ids_for_scoreable_beaches) returns empty on cold-launch
    # states where no prior extractions exist. See bootstrap chain:
    #   build_beach_evidence (first pass)
    #     → state_default emits dogs row → beach_dog_policy populated
    #     → catchment_refresh sets scoring_tier
    #     → Phase 26 gated set now contains real operator ids
    deps=[env_preflight, catchment_refresh],
    group_name="phase_26_to_28_operator_llm",
    description=(
        "State fanout for Phase 26. Calls extract_operator_dogs_policy.py "
        "with all operator IDs returned by state_operator_ids_for_scoreable_beaches(state), "
        "chunked at 5 ops/subprocess. Follows the proven _chunked_subprocess "
        "convention from run_state_pipeline.py: per-chunk inline retry (30s "
        "backoff, 1 retry), failed chunks log + continue (never halt the "
        "asset). Underlying script is idempotent via --skip-recent so failed "
        "chunks self-recover on next launch. ast.literal_eval parses the "
        "script's `{'src_a': N, 'src_b': N, ...}` summary; rows_written is "
        "also cross-checked via DB delta."
    ),
)
def operator_llm_extract_for_state(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
    subproc: SubprocessResource,
) -> MaterializeResult:
    state = context.partition_key

    # 2026-05-13 cost gate: only operators "definitively assigned" to
    # a scoreable (daily/hourly tier) beach via BEP attribution.
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

    # Snapshot DB count BEFORE — used to compute rows_written delta after.
    # More reliable than regex-parsing the script's stdout, which prints
    # `{'src_a': N, 'src_b': N, ...}` not `upserted N`.
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM public.operator_policy_extractions ope "
                "  JOIN public.operators o ON o.id = ope.operator_id "
                " WHERE o.state_code = %s", (state,)
            )
            rows_before = cur.fetchone()[0]
    finally:
        conn.close()

    # Chunk into groups of 5.
    import time as _time
    chunk_size = 5
    chunks_done = 0
    chunks_failed = 0
    ops_skipped_recent = 0
    ops_with_errors = 0
    ops_written_total = 0
    total_chunks = (len(ids) + chunk_size - 1) // chunk_size
    failed_chunks_detail: list[str] = []
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i:i + chunk_size]
        t_chunk = _time.time()
        result = subproc.run(
            "scripts/extract_operator_dogs_policy.py",
            # --skip-recent is in DAYS. 7d matches run_state_pipeline.py
            # for cost reasons — operator policies change rarely, and
            # sentinel rows from extract_operator_dogs_policy.py (added
            # 2026-05-23 EVENING) cache the no-URL outcome.
            args=["--ids", ",".join(str(x) for x in chunk), "--skip-recent", "7"],
            timeout=600,
        )
        if result.returncode != 0:
            chunks_failed += 1
            detail = (
                f"chunk {chunks_done+chunks_failed}/{total_chunks} ops {chunk[0]}..{chunk[-1]}: "
                f"{result.stderr[-200:] or result.stdout[-200:]}"
            )
            failed_chunks_detail.append(detail)
            context.log.warning(detail)
            continue
        # Parse the script's per-chunk summary `{'src_a': N, 'errors': N, 'skipped_recent': N, ...}`
        # - skipped_recent: ops cache-skipped via --skip-recent 24
        # - errors: ops where extraction raised/aborted (definitive failure signal)
        m = re.search(r"'skipped_recent':\s*(\d+)", result.stdout)
        chunk_skipped = int(m.group(1)) if m else 0
        ops_skipped_recent += chunk_skipped
        m = re.search(r"'errors':\s*(\d+)", result.stdout)
        chunk_errors = int(m.group(1)) if m else 0
        ops_with_errors += chunk_errors
        # Sum non-meta src_* counters = ops where the script wrote something.
        chunk_written = sum(
            int(v) for k, v in re.findall(r"'(\w+)':\s*(\d+)", result.stdout)
            if k.startswith('src_')
        )
        ops_written_total += chunk_written
        chunks_done += 1
        context.log.info(
            f"  chunk {chunks_done}/{total_chunks} "
            f"(ops {chunk[0]}..{chunk[-1]}): +{chunk_written} written, "
            f"{chunk_skipped} cached, {chunk_errors} errors "
            f"in {_time.time()-t_chunk:.1f}s"
        )

    # Snapshot DB count AFTER — delta = rows actually written by this run.
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM public.operator_policy_extractions ope "
                "  JOIN public.operators o ON o.id = ope.operator_id "
                " WHERE o.state_code = %s", (state,)
            )
            rows_after = cur.fetchone()[0]
    finally:
        conn.close()
    total_rows = rows_after - rows_before

    # ── Failure gate ─────────────────────────────────────────────────
    #
    # Two definitive failure signals — both observable from chunk results.
    # Zero-rows alone is NOT a failure (all ops may have been cache-skipped
    # via --skip-recent, or had no extractable policy — both legitimate).
    #
    # (1) Subprocess returncode != 0 on >30% of chunks → systemic env issue.
    # (2) Script's own `errors` counter > 0 → script explicitly reported
    #     extraction failures (vs "ran but found nothing").
    #
    # If neither fires, the run is a success even with rows_written=0.
    failure_threshold = 0.30
    fail_rate = chunks_failed / total_chunks if total_chunks else 0.0
    ops_actually_attempted = len(ids) - ops_skipped_recent
    hard_fails: list[str] = []
    if ops_with_errors > 0:
        hard_fails.append(
            f"Phase 26 script reported {ops_with_errors} per-op extraction errors. "
            f"See failed_chunks_detail."
        )
    if fail_rate > failure_threshold:
        hard_fails.append(
            f"Phase 26 chunk failure rate {chunks_failed}/{total_chunks} "
            f"({fail_rate:.1%}) exceeds {failure_threshold:.0%} threshold — "
            f"likely systemic issue (env/API/auth). See failed_chunks_detail."
        )

    if hard_fails:
        # Log all failed chunks for debugging before raising
        for d in failed_chunks_detail[:5]:
            context.log.error(d)
        raise RuntimeError(" | ".join(hard_fails))

    return MaterializeResult(
        metadata={
            "state": MetadataValue.text(state),
            "operators_total": MetadataValue.int(len(ids)),
            "ops_skipped_recent": MetadataValue.int(ops_skipped_recent),
            "ops_actually_attempted": MetadataValue.int(ops_actually_attempted),
            "ops_with_errors": MetadataValue.int(ops_with_errors),
            "chunks_done": MetadataValue.int(chunks_done),
            "chunks_failed": MetadataValue.int(chunks_failed),
            "fail_rate_pct": MetadataValue.float(round(fail_rate * 100, 1)),
            "rows_written": MetadataValue.int(total_rows),
            "failed_chunks_detail": MetadataValue.text(
                "\n".join(failed_chunks_detail[:10]) or "(none)"
            ),
        }
    )


# ════════════════════════════════════════════════════════════════════════
#  Phase 27 — operator_merge (GLOBAL — not state-scoped)
# ════════════════════════════════════════════════════════════════════════

@asset(
    deps=[operator_llm_extract_for_state],
    group_name="phase_26_to_28_operator_llm",
    description=(
        "GLOBAL operator_merge: scripts/merge_operator_dogs_policy.py "
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
        "scripts/merge_operator_dogs_policy.py",
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
#  Phase 28 — rebuild_beach_evidence (per-state)
# ════════════════════════════════════════════════════════════════════════

@asset(
    name="rebuild_beach_evidence",
    partitions_def=state_partitions,
    # 2026-05-13: added promote_to_gold dep to serialize after catalog
    # assembly. Without this, Dagster scheduled rebuild_beach_evidence in
    # parallel with promote_to_gold (both run build_beach_evidence per fid
    # on the same BEP rows), wasting compute + racing on the same writes.
    # See project_build_beach_evidence_perf.md.
    deps=[operator_merge, operator_llm_extract_for_state, promote_to_gold],
    group_name="phase_26_to_28_operator_llm",
    code_version="2026-05-13-v4-operator-only-mode",
    description=(
        "Rebuilds beach-evidence rows for state's tier-1+2 fids in "
        "operator_only mode — only re-runs the 5 operator-dependent "
        "populators (and their resolvers/consensus/promote_canonical). "
        "Skips the 9 upstream-data-only populators (polygon_containment, "
        "cpad, pad_us, pad_us_dogs, state_default, research, park_url, "
        "park_url_governance, unified_v1, osm_amenities) that already ran "
        "in promote_to_gold's first-pass full-mode build. Cuts Phase 28 "
        "wall-clock ~60-80% (45min → 5-10min on 151 fids). "
        "Per-fid autocommit; race-safe because we serialized after "
        "promote_to_gold via deps."
    ),
)
def rebuild_beach_evidence(
    context: AssetExecutionContext,
    postgres_session: PostgresSessionResource,
) -> MaterializeResult:
    import time as _time
    state = context.partition_key
    conn = postgres_session.get_connection()
    conn.set_session(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '300s'")  # per-fid ceiling
            cur.execute(
                "SELECT array_agg(distinct g.fid) "
                "  FROM public.beaches_gold g "
                " WHERE g.state=%s AND g.is_active AND g.scoring_tier IN ('daily','hourly')",
                (state,),
            )
            fids = cur.fetchone()[0] or []
            if not fids:
                return MaterializeResult(
                    metadata={"state": state, "fids": 0, "skipped": True}
                )

            t0 = _time.time()
            done = 0
            failed = 0
            for fid in fids:
                try:
                    cur.execute(
                        "SELECT public.build_beach_evidence(%s, 'operator_only')",
                        (fid,),
                    )
                    done += 1
                except Exception as e:
                    failed += 1
                    context.log.warning(f"fid {fid} failed: {str(e)[:200]}")
            wall_s = round(_time.time() - t0, 1)
    finally:
        conn.close()
    return MaterializeResult(
        metadata={
            "state":          MetadataValue.text(state),
            "fids_total":     MetadataValue.int(len(fids)),
            "fids_succeeded": MetadataValue.int(done),
            "fids_failed":    MetadataValue.int(failed),
            "wall_clock_s":   MetadataValue.float(wall_s),
            "mode":           MetadataValue.text("operator_only"),
        }
    )


# ════════════════════════════════════════════════════════════════════════
#  Phase 28.5 — gold_set_candidates (per-state)
# ════════════════════════════════════════════════════════════════════════
#
# Auto-samples ~25 beaches × 7 fields per state into gold_set_curation_queue
# with stratified diversity (operator level × dogs outcome × confidence band).
# See project_gold_set_curator_queue.md for the design.

@asset(
    name="gold_set_candidates",
    partitions_def=state_partitions,
    deps=[rebuild_beach_evidence],
    group_name="phase_28p5_gold_set_curation",
    description=(
        "Auto-samples ~25 beaches × 7 priority fields per state into "
        "gold_set_curation_queue with stratified diversity. Idempotent — "
        "ON CONFLICT DO NOTHING on (state, gold_fid, field_name). Re-running "
        "after curator review only adds new strata, never duplicates existing."
    ),
)
def gold_set_candidates(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    state = context.partition_key
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT public.sample_gold_set_candidates(%s)", (state,))
            rows_enqueued = cur.fetchone()[0]
            cur.execute(
                "SELECT count(*) FILTER (WHERE status='pending'), "
                "       count(*) FILTER (WHERE status='reviewed'), "
                "       count(*) "
                "  FROM public.gold_set_curation_queue WHERE state=%s",
                (state,),
            )
            pending, reviewed, total = cur.fetchone()
        conn.commit()
    finally:
        conn.close()
    return MaterializeResult(
        metadata={
            "state":          MetadataValue.text(state),
            "rows_enqueued":  MetadataValue.int(rows_enqueued),
            "queue_pending":  MetadataValue.int(pending),
            "queue_reviewed": MetadataValue.int(reviewed),
            "queue_total":    MetadataValue.int(total),
            "curator_url":    MetadataValue.url(
                f"https://beachfranz.github.io/admin/gold-set-curator-v3.html?queue={state}"
            ),
        }
    )


# ════════════════════════════════════════════════════════════════════════
#  Phase 28.6 — gold_set_review_gate (per-state, blocking)
# ════════════════════════════════════════════════════════════════════════
#
# Blocks downstream phases until the curator queue is cleared OR the state
# has an explicit signoff OR the run carries tag gold_set_bypass=true.
# Bypass is logged to gold_set_bypass_audit — engineering use only.

@asset(
    name="gold_set_review_gate",
    partitions_def=state_partitions,
    deps=[gold_set_candidates],
    group_name="phase_28p5_gold_set_curation",
    description=(
        "Blocks downstream until gold-set queue is cleared (status != 'pending' "
        "for all rows) OR gold_set_signoff(state).approved=true OR run carries "
        "tag gold_set_bypass=true. Bypass logs to gold_set_bypass_audit. "
        "Re-materialize after curator clears the queue to unblock."
    ),
)
def gold_set_review_gate(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    state = context.partition_key
    run_tags = context.run.tags if context.run else {}
    bypass = str(run_tags.get("gold_set_bypass", "")).lower() in ("true", "1", "yes")
    bypass_reason = run_tags.get("gold_set_bypass_reason", "no reason provided")

    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT public.gold_set_curation_complete_for_state(%s)", (state,)
            )
            ready = bool(cur.fetchone()[0])
            cur.execute(
                "SELECT count(*) FILTER (WHERE status='pending'), "
                "       count(*) "
                "  FROM public.gold_set_curation_queue WHERE state=%s",
                (state,),
            )
            pending, total = cur.fetchone()

            if not ready and not bypass:
                raise RuntimeError(
                    f"gold_set_review_gate: state={state} not ready. "
                    f"queue_pending={pending}/{total}. "
                    f"To unblock: (a) curator clears queue via "
                    f"https://beachfranz.github.io/admin/gold-set-curator-v3.html?queue={state} "
                    f"then re-materialize this asset; OR (b) write gold_set_signoff "
                    f"row with approved=true for engineering-validated states; OR "
                    f"(c) re-launch with run tag gold_set_bypass=true (engineering only — "
                    f"logged to gold_set_bypass_audit)."
                )

            if bypass and not ready:
                cur.execute(
                    "SELECT public.gold_set_log_bypass(%s, %s, %s, 'dagster_tag')",
                    (state, str(context.run_id), bypass_reason),
                )
                conn.commit()
                context.log.warning(
                    f"GATE BYPASSED state={state} run_id={context.run_id} "
                    f"queue_pending={pending}/{total} reason={bypass_reason!r}"
                )
    finally:
        conn.close()

    return MaterializeResult(
        metadata={
            "state":         MetadataValue.text(state),
            "ready":         MetadataValue.bool(ready),
            "bypassed":      MetadataValue.bool(bypass and not ready),
            "queue_pending": MetadataValue.int(pending),
            "queue_total":   MetadataValue.int(total),
            "bypass_reason": MetadataValue.text(bypass_reason if bypass else "n/a"),
        }
    )
