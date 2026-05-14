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
) -> tuple[int, int, int]:
    """Returns (total_parsed_value, chunks_done, chunks_failed)."""
    total = 0
    done = 0
    failed = 0
    for i in range(0, len(fids), chunk_size):
        chunk = fids[i:i + chunk_size]
        result = subproc.run(
            script_path,
            args=["--fids", ",".join(str(x) for x in chunk)],
            timeout=per_chunk_timeout,
        )
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
#  Phase 29 — section_extract (per-fid Haiku, batched 8 beaches/call)
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[rebuild_beach_evidence],
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
#  Phase 30 — descriptions (per-fid Sonnet, chunked 30 fids/subprocess)
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[section_extract],
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
