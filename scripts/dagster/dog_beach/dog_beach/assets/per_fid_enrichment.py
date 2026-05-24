"""Phases 29-31 — per-fid LLM + photo enrichment.

Phase 29 (section_extract): Haiku call per beach, batched 8 beaches/call.
  Reads operator_dogs_policy.summary, maps to per-section beach rules
  (sand/water/trails/picnic_area). Writes BEP source='section_research_v1'.

Phase 30 (descriptions): Sonnet call per beach. Beach prose 2-3 sentences.
  Pulls zone_rules + CPAD parent + Overpass features within 300m, hashes,
  upserts to beach_descriptions.

Phase 31 (photos_wikimedia): Wikimedia Commons API per beach.
  CC-licensed photos; replaced Mapillary 2026-05-09.

Patterns:
  - Each phase has a per-state fanout asset (chunked subprocess, matches
    run_state_pipeline.py's _chunked_subprocess pattern).
  - Future: per-fid dynamic partitions (fid_partitions in partitions.py)
    can replace the chunked-fanout pattern for per-fid retry/visibility.
    Not v1 — requires sensor to populate fid_partitions first.
"""

import re

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
)

from ..partitions import state_partitions
from ..resources import PostgresPoolerResource, SubprocessResource
from .operator_llm_cascade import rebuild_beach_evidence


def _tier12_fids(postgres: PostgresPoolerResource, state: str) -> list[int]:
    """Mirrors _state_tier12_fids from run_state_pipeline.py."""
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT array_agg(distinct g.fid) "
                "  FROM public.beaches_gold g "
                " WHERE g.state=%s AND g.is_active AND g.scoring_tier IN ('daily','hourly')",
                (state,),
            )
            return cur.fetchone()[0] or []
    finally:
        conn.close()


def _run_chunked(
    context: AssetExecutionContext,
    subproc: SubprocessResource,
    script_path: str,
    fids: list[int],
    chunk_size: int,
    per_chunk_timeout: int,
    parse_pattern: str,
    extra_args: list[str] | None = None,
) -> tuple[int, int, int]:
    """Returns (total_parsed_value, chunks_done, chunks_failed)."""
    total = 0
    done = 0
    failed = 0
    import subprocess as _sp
    for i in range(0, len(fids), chunk_size):
        chunk = fids[i:i + chunk_size]
        args = ["--fids", ",".join(str(x) for x in chunk)]
        if extra_args:
            args.extend(extra_args)
        try:
            result = subproc.run(
                script_path,
                args=args,
                timeout=per_chunk_timeout,
            )
        except _sp.TimeoutExpired as e:
            # 2026-05-23 EVENING: a single description-generation run hung
            # for 42 min on one fid (Anthropic call w/ no client-side
            # timeout). subprocess.TimeoutExpired bubbling up was failing
            # the whole asset. Catch + treat as failed chunk so other
            # chunks proceed; surface in metadata via `failed`.
            failed += 1
            context.log.warning(
                f"Chunk {done+failed} timed out (fids {chunk[0]}..{chunk[-1]}, "
                f"timeout={per_chunk_timeout}s)"
            )
            continue
        if result.returncode != 0:
            failed += 1
            context.log.warning(
                f"Chunk {done+1} failed (fids {chunk[0]}..{chunk[-1]}): "
                f"{result.stderr[-300:]}"
            )
            continue
        m = re.search(parse_pattern, result.stdout)
        if m:
            total += int(m.group(1))
        done += 1
    return total, done, failed


# ════════════════════════════════════════════════════════════════════════
#  Derived columns + materialized refresh
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[rebuild_beach_evidence],
    group_name="phase_29_to_33_per_fid",
    description=(
        "Refresh nearest-dog-park materialized columns on beaches_gold. Uses "
        "the updated_at sentinel (gap #39) — a beach is 'processed' once the "
        "search ran, regardless of whether a DP was found within the 25mi cap "
        "(legitimate no-DP-in-cap outcome in rural states)."
    ),
)
def refresh_nearest_dog_park(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
    subproc: SubprocessResource,
) -> MaterializeResult:
    state = context.partition_key
    result = subproc.run(
        "scripts/refresh_nearest_dog_park.py",
        args=["--state", state],
        timeout=1800,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"refresh_nearest_dog_park.py exit {result.returncode}: "
            f"{(result.stderr or '')[-500:]}"
        )

    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FILTER (WHERE nearest_dog_park_updated_at IS NOT NULL), "
                "       COUNT(*) FILTER (WHERE nearest_dog_park_fid IS NOT NULL), "
                "       COUNT(*) "
                "  FROM public.beaches_gold WHERE state=%s AND is_active",
                (state,),
            )
            n_searched, n_with_dp, total = cur.fetchone()
    finally:
        conn.close()
    return MaterializeResult(
        metadata={
            "state": MetadataValue.text(state),
            "active_beaches": MetadataValue.int(total),
            "searched": MetadataValue.int(n_searched),
            "with_dog_park_in_25mi": MetadataValue.int(n_with_dp),
        }
    )


@asset(
    partitions_def=state_partitions,
    deps=[rebuild_beach_evidence],
    group_name="phase_29_to_33_per_fid",
    description=(
        "codify-cascade per-state enumeration of federal/county/city codify "
        "universe. v1 is REPORT-ONLY — emits a JSON manifest of any units "
        "needing agent dispatch; doesn't auto-dispatch."
    ),
)
def codify_cascade(
    context: AssetExecutionContext,
    subproc: SubprocessResource,
) -> MaterializeResult:
    state = context.partition_key
    result = subproc.run(
        "scripts/codify_cascade_phase.py",
        args=["--state", state, "--gate-pct", "80"],
        timeout=600,
    )
    # Distinguish two failure modes:
    #   1. Script crash (stderr non-empty, no manifest written) — hard fail.
    #   2. Coverage below gate-pct (exit 1, "passes_gate=False" in stdout,
    #      0 queue rows dispatched) — virgin-state launch reality. Log as
    #      warning + surface coverage% in metadata; don't halt the pipeline.
    stdout = result.stdout or ""
    stderr = result.stderr or ""
    if result.returncode != 0:
        is_gate_miss = "passes_gate=False" in stdout and "UPSERTed" in stdout
        if not is_gate_miss:
            raise RuntimeError(
                f"codify_cascade_phase.py crashed (exit {result.returncode}): "
                f"stderr={stderr[-500:]} stdout={stdout[-500:]}"
            )
        context.log.warning(
            f"codify_cascade for {state}: coverage below gate (state launch "
            f"reality — expected to clear as codify work lands). See log tail."
        )
    return MaterializeResult(
        metadata={
            "state": MetadataValue.text(state),
            "coverage_gate_passed": MetadataValue.bool(result.returncode == 0),
            "log_tail": MetadataValue.text(stdout[-2000:]),
        }
    )


@asset(
    partitions_def=state_partitions,
    deps=[rebuild_beach_evidence],
    group_name="phase_29_to_33_per_fid",
    description=(
        "v2 area-first zone_rules re-extract over all policy_sources per "
        "MVP+ beach. Reads beach_policy_source.full_text directly with the "
        "canonical-area-description prompt, rewrites bps/temporals. Multi-ps "
        "beaches get each source re-run. Chunked 30 fids/subprocess."
    ),
)
def zone_rules_v2_refresh(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
    subproc: SubprocessResource,
) -> MaterializeResult:
    state = context.partition_key
    fids = _tier12_fids(postgres, state)
    if not fids:
        return MaterializeResult(metadata={"state": state, "fids": 0, "skipped": True})

    total, done, failed = _run_chunked(
        context, subproc,
        "scripts/reextract_beach_all_ps.py",
        fids, chunk_size=30, per_chunk_timeout=900,
        parse_pattern=r"updated:\s+(\d+)",
        extra_args=["--skip-if-fresh-within", "7"],
    )
    return MaterializeResult(
        metadata={
            "state": MetadataValue.text(state),
            "fids_total": MetadataValue.int(len(fids)),
            "rules_updated": MetadataValue.int(total),
            "chunks_done": MetadataValue.int(done),
            "chunks_failed": MetadataValue.int(failed),
        }
    )


@asset(
    partitions_def=state_partitions,
    deps=[zone_rules_v2_refresh],
    group_name="phase_29_to_33_per_fid",
    description=(
        "Operating-hours canonical populate. Per-state idempotent call to "
        "public.refresh_operating_hours_for_state — picks highest-priority "
        "source (manual_curator > operator > osm > agency > inferred) and "
        "writes beaches_gold.operating_hours jsonb."
    ),
)
def operating_hours_refresh(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    state = context.partition_key
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT public.refresh_operating_hours_for_state(%s)", (state,)
            )
            n_processed = cur.fetchone()[0]
        conn.commit()
    finally:
        conn.close()
    return MaterializeResult(
        metadata={
            "state": MetadataValue.text(state),
            "beaches_processed": MetadataValue.int(n_processed),
        }
    )


# ════════════════════════════════════════════════════════════════════════
#  Phase 29 — section_extract (per-fid Haiku, batched 8 beaches/call)
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[operating_hours_refresh],
    group_name="phase_29_to_33_per_fid",
    description=(
        "Per-beach section-rule extraction via Haiku. Reads operator_dogs_policy.summary "
        "for the beach's matched operator and maps to per-section rules "
        "(sand / water / trails / picnic_area). Writes BEP source='section_research_v1'. "
        "Chunked 40 fids/subprocess (script batches 8 beaches per Haiku call)."
    ),
)
def section_extract(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
    subproc: SubprocessResource,
) -> MaterializeResult:
    state = context.partition_key
    fids = _tier12_fids(postgres, state)
    if not fids:
        return MaterializeResult(metadata={"state": state, "fids": 0, "skipped": True})

    total, done, failed = _run_chunked(
        context, subproc,
        "scripts/extract_beach_section_rules.py",
        fids, chunk_size=40, per_chunk_timeout=600,
        parse_pattern=r"sections_written:\s+(\d+)",
    )
    return MaterializeResult(
        metadata={
            "state": MetadataValue.text(state),
            "fids_total": MetadataValue.int(len(fids)),
            "sections_written": MetadataValue.int(total),
            "chunks_done": MetadataValue.int(done),
            "chunks_failed": MetadataValue.int(failed),
        }
    )


# ════════════════════════════════════════════════════════════════════════
#  Phase 29.5 — harvest_park_text (leaf-URL prose → park_url_extractions)
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[section_extract],
    group_name="phase_29_to_33_per_fid",
    description=(
        "Harvest descriptive prose from state-park leaf URLs into "
        "park_url_extractions. Fills description-text reservoir gap "
        "(Gap 2 per deferred-description-text-reservoir pin) for OR/WA "
        "where pad_us polygons don't carry park_url. Reuses each state's "
        "StateParksLoader.discover_parks() for polygon → leaf-URL mapping "
        "(already proven for photos). raw_text only — structured LLM "
        "extraction deferred. No-op for states without a wired harvester "
        "(CA harvested via the older park_url_scrape_queue path). "
        "Idempotent + cheap via --skip-if-fresh-within 90."
    ),
)
def harvest_park_text(
    context: AssetExecutionContext,
    subproc: SubprocessResource,
) -> MaterializeResult:
    state = context.partition_key
    if state not in ("CA", "OR", "WA"):
        context.log.info(f"[{state}] no leaf-URL harvester wired — skip")
        return MaterializeResult(
            metadata={"state": state, "skipped": True}
        )
    result = subproc.run(
        "scripts/harvest_park_text.py",
        args=["--state", state, "--workers", "6"],
        timeout=900,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"harvest_park_text failed for {state}: "
            f"{(result.stderr or '')[-500:]}"
        )
    m = re.search(r"rows_written=(\d+)", result.stdout or "")
    rows = int(m.group(1)) if m else 0
    return MaterializeResult(
        metadata={
            "state": MetadataValue.text(state),
            "rows_written": MetadataValue.int(rows),
            "log_tail": MetadataValue.text((result.stdout or "")[-1500:]),
        }
    )


# ════════════════════════════════════════════════════════════════════════
#  Phase 30 — descriptions (per-fid Sonnet, chunked 30 fids/subprocess)
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[section_extract, harvest_park_text],
    group_name="phase_29_to_33_per_fid",
    description=(
        "Per-beach narrative prose via Sonnet. SHA256-hashed input bundle "
        "(zone_rules + CPAD parent + Overpass features within 300m) — skipped "
        "if cached. Upserts to beach_descriptions. ~$0.003/beach."
    ),
)
def descriptions(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
    subproc: SubprocessResource,
) -> MaterializeResult:
    state = context.partition_key
    fids = _tier12_fids(postgres, state)
    if not fids:
        return MaterializeResult(metadata={"state": state, "fids": 0, "skipped": True})

    total, done, failed = _run_chunked(
        context, subproc,
        "scripts/generate_beach_descriptions.py",
        fids, chunk_size=30, per_chunk_timeout=900,
        parse_pattern=r"generated:\s+(\d+)",
    )
    return MaterializeResult(
        metadata={
            "state": MetadataValue.text(state),
            "fids_total": MetadataValue.int(len(fids)),
            "descriptions_generated": MetadataValue.int(total),
            "chunks_done": MetadataValue.int(done),
            "chunks_failed": MetadataValue.int(failed),
        }
    )


@asset(
    partitions_def=state_partitions,
    deps=[descriptions],
    group_name="phase_29_to_33_per_fid",
    description=(
        "Audit recently-generated beach descriptions for forbidden-pattern "
        "hits (hallucinated lifeguard claims, architecture-explicit leaks, "
        "etc.). Scoped to the state's tier-1+2 beaches generated within "
        "the last few hours. Surfaces audit hits but doesn't auto-fix."
    ),
)
def descriptions_audit(
    context: AssetExecutionContext,
    subproc: SubprocessResource,
) -> MaterializeResult:
    state = context.partition_key
    result = subproc.run(
        "scripts/audit_beach_descriptions.py",
        args=["--state", state, "--hours", "2"],
        timeout=300,
    )
    # Audit is informational — log hits, don't halt the pipeline.
    if result.returncode != 0:
        context.log.warning(
            f"descriptions_audit non-zero exit {result.returncode}: "
            f"{(result.stderr or '')[-300:]}"
        )
    return MaterializeResult(
        metadata={
            "state": MetadataValue.text(state),
            "log_tail": MetadataValue.text((result.stdout or "")[-2000:]),
        }
    )


# ════════════════════════════════════════════════════════════════════════
#  Phase 31 — photos_wikimedia (per-fid Commons API, chunked 100 fids)
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[descriptions],
    group_name="phase_29_to_33_per_fid",
    description=(
        "Wikimedia Commons photo loader. Replaced Mapillary 2026-05-09. "
        "Coverage varies by region — rural beaches may have 0; criterion "
        "tolerates 0. Pace 1.5s/request per WMF robots policy."
    ),
)
def photos_wikimedia(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
    subproc: SubprocessResource,
) -> MaterializeResult:
    state = context.partition_key
    fids = _tier12_fids(postgres, state)
    if not fids:
        return MaterializeResult(metadata={"state": state, "fids": 0, "skipped": True})

    total, done, failed = _run_chunked(
        context, subproc,
        "scripts/load_wikimedia_commons_photos.py",
        fids, chunk_size=100, per_chunk_timeout=600,
        parse_pattern=r"(\d+)\s+photos\s+saved",
    )
    return MaterializeResult(
        metadata={
            "state": MetadataValue.text(state),
            "fids_total": MetadataValue.int(len(fids)),
            "photos_saved": MetadataValue.int(total),
            "chunks_done": MetadataValue.int(done),
            "chunks_failed": MetadataValue.int(failed),
        }
    )
