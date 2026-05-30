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
#  Phase 32.5 — weather_advisories_refresh (post-scoring, cross-state)
# ════════════════════════════════════════════════════════════════════════
#
# Runs scripts/compute_weather_advisories.py --all-scored, which derives
# beach_advisory rows from beach_day_hourly_scores + beach_day_recommendations:
#   * per-metric deterministic_weather rows (UV, tide, sand, asphalt, wind, ...)
#   * bacteria_risk rows from precip_72h-driven daily field
#
# beach.html reads beach_advisory directly via loadWaterConditionAdvisories()
# to render the Cautions card. Without this asset firing daily, the card
# stays empty for beaches scored after the last manual run — confirmed
# via fid 8347 La Jolla Shores on 2026-05-30 (bacteria_risk='moderate'
# in beach_day_recommendations but no advisory written; Scout LLM mentioned
# bacteria in narrative text but caution pill never appeared).
#
# NOT partitioned — script handles cross-state scope via --all-scored
# (filter scoring_tier IN ('daily','hourly')). Single run covers everyone.

@asset(
    group_name="phase_29_to_33_per_fid",
    deps=[daily_refresh_fire],
    description=(
        "Daily — derives beach_advisory rows from beach_day_hourly_scores + "
        "beach_day_recommendations for every active+scored beach. Powers the "
        "Cautions card on beach.html (loadWaterConditionAdvisories). "
        "Runs compute_weather_advisories.py --all-scored."
    ),
)
def weather_advisories_refresh(
    context: AssetExecutionContext,
    subproc: SubprocessResource,
) -> MaterializeResult:
    result = subproc.run(
        "scripts/compute_weather_advisories.py",
        args=["--all-scored"],
        timeout=1800,  # 30 min — ~600 CA beaches at ~1s each + other states
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"compute_weather_advisories.py failed (exit {result.returncode}): "
            f"{(result.stderr or '')[-500:]}"
        )
    # Tail line typically reads "Done. Deterministic advisories upserted: N · retired: M"
    tail = (result.stdout or "").strip().splitlines()[-1] if result.stdout else ""
    return MaterializeResult(
        metadata={
            "summary": MetadataValue.text(tail),
            "log_tail": MetadataValue.text((result.stdout or "")[-3000:]),
        }
    )


# ════════════════════════════════════════════════════════════════════════
#  Phase 32.6 — codify_coverage_audit (cross-state, daily)
# ════════════════════════════════════════════════════════════════════════
#
# Audits how much of the scored beach catalog has structured dog-policy
# rules wired in beach_policy_source + beach_policy_source_temporal.
# beach.html / find.html consume zone_rules emitted from this layer.
#
# Reports per-state coverage % and lists the top jurisdictions with gaps
# so that follow-up codify work (manual per-city or the clone tool) can
# be scoped. Raises on regression below COVERAGE_THRESHOLD_PCT — catches
# the case where a state launch adds beaches faster than the codify
# pipeline catches up.
#
# Architecture pin: structured time_windows live in
# beach_policy_source_temporal (NOT beach_enrichment_provenance.claimed_values).
# See [[codify-cascade-reads-bps-not-bep]].

CODIFY_COVERAGE_THRESHOLD_PCT = 80


@asset(
    group_name="phase_29_to_33_per_fid",
    deps=[daily_refresh_fire],
    description=(
        "Daily — counts scored beaches per state with vs without an operative "
        "sand-section beach_policy_source row. Raises on regression below "
        f"{CODIFY_COVERAGE_THRESHOLD_PCT}% coverage."
    ),
)
def codify_coverage_audit(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH scope AS (
                  SELECT bg.fid, bg.state
                    FROM public.beaches_gold bg
                   WHERE bg.is_active
                     AND bg.scoring_tier IN ('daily','hourly')
                )
                SELECT s.state,
                       COUNT(*) AS total_scored,
                       COUNT(*) FILTER (WHERE EXISTS (
                         SELECT 1 FROM public.beach_policy_source bps
                          WHERE bps.beach_fid = s.fid
                            AND bps.operative_status = 'operative'
                            AND bps.section = 'sand'
                       )) AS with_sand_bps,
                       COUNT(*) FILTER (WHERE EXISTS (
                         SELECT 1 FROM public.beach_policy_source bps
                          JOIN public.beach_policy_source_temporal bpst ON bpst.bps_id = bps.id
                          WHERE bps.beach_fid = s.fid AND bpst.section = 'sand'
                            AND bps.operative_status = 'operative'
                       )) AS with_temporal
                  FROM scope s
                 GROUP BY s.state
                 ORDER BY s.state;
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    by_state = {}
    total_scored = 0
    total_with_bps = 0
    worst_state = None
    worst_pct = 101.0
    for state, total, with_bps, with_temporal in rows:
        pct = round(100.0 * with_bps / max(total, 1), 1)
        by_state[state] = {
            "scored": total,
            "with_sand_bps": with_bps,
            "with_temporal": with_temporal,
            "pct": pct,
        }
        total_scored += total
        total_with_bps += with_bps
        if pct < worst_pct:
            worst_pct = pct
            worst_state = state
        context.log.info(
            f"  {state}: {with_bps}/{total} sand bps ({pct}%), "
            f"{with_temporal} with time_windows"
        )

    overall_pct = round(100.0 * total_with_bps / max(total_scored, 1), 1)
    context.log.info(f"OVERALL: {total_with_bps}/{total_scored} = {overall_pct}%")

    if worst_state is not None and worst_pct < CODIFY_COVERAGE_THRESHOLD_PCT:
        raise RuntimeError(
            f"codify coverage regression: {worst_state}={worst_pct}% < "
            f"{CODIFY_COVERAGE_THRESHOLD_PCT}% threshold. Run codify_gap_clone "
            f"or manual per-city codify (see scripts/one_off/"
            f"close_sd_city_codify_gap.py as a worked example)."
        )

    return MaterializeResult(
        metadata={
            "overall_pct": MetadataValue.float(overall_pct),
            "total_scored": MetadataValue.int(total_scored),
            "total_with_sand_bps": MetadataValue.int(total_with_bps),
            "worst_state": MetadataValue.text(f"{worst_state}={worst_pct}%"),
            "by_state": MetadataValue.json(by_state),
        }
    )


# ════════════════════════════════════════════════════════════════════════
#  Phase 32.7 — codify_gap_clone (cross-state, weekly)
# ════════════════════════════════════════════════════════════════════════
#
# Runs scripts/codify_clone_gap_beaches.py across all MVP+ states.
# For each beach with no operative sand bps, finds a sibling beach in
# the same city (or CDPR park unit) that IS codified, and copies its
# rule structure. Catches newly-added beaches that fall into already-
# codified cities — keeps coverage from drifting downward as state
# inventory grows.
#
# What this DOESN'T handle: beaches in jurisdictions with no existing
# codify (no wired sibling). Those need per-city research codify per
# the close_sd_city_codify_gap.py pattern.

@asset(
    group_name="phase_29_to_33_per_fid",
    deps=[codify_coverage_audit],
    description=(
        "Weekly — clones operative sand bps + temporal rows to gap beaches "
        "from siblings in the same jurisdiction or CDPR park unit. Falls "
        "back gracefully when no sibling exists (reports unhandled list)."
    ),
)
def codify_gap_clone(
    context: AssetExecutionContext,
    subproc: SubprocessResource,
) -> MaterializeResult:
    result = subproc.run(
        "scripts/codify_clone_gap_beaches.py",
        args=[],  # default --states = all MVP+
        timeout=1800,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"codify_clone_gap_beaches.py failed (exit {result.returncode}): "
            f"{(result.stderr or '')[-500:]}"
        )
    tail = (result.stdout or "").strip().splitlines()[-1] if result.stdout else ""
    return MaterializeResult(
        metadata={
            "summary": MetadataValue.text(tail),
            "log_tail": MetadataValue.text((result.stdout or "")[-3000:]),
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
