"""dog_park_amenity_refresh — bulk re-extraction of dog-park amenities.

Distinct from dog_park_coverage (initial state launch):
  - dog_park_coverage extracts parks that DON'T have a source yet
  - dog_park_amenity_refresh re-extracts ALL active parks in a state,
    used after prompt extensions (e.g. when new amenity columns are added).

After extraction it promotes BEP → policy via
promote_canonical_dog_park_policies_for_state() and reports coverage
via dog_park_amenity_coverage().

Materialize from the Dagster UI with a Config:
  limit: int | None  — None = all parks in state; small int = smoke test

Workflow that proved out today (CA / OR ramp pattern):
  1. limit=5  (smoke)
  2. limit=20 (broader sample)
  3. limit=null (full state)

Per Franz 2026-05-27 LATE — promote ad-hoc to process.
"""
from datetime import datetime, timezone
from typing import Any, Optional

from dagster import (
    asset,
    AssetExecutionContext,
    Config,
    MetadataValue,
    MaterializeResult,
)

import sys
from pathlib import Path
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from scripts.common.db import connect
from scripts.dog_park_pipeline.extract import run_extractor
from scripts.dog_park_pipeline._inproc import call_main  # noqa: F401 — kept consistent w/ coverage
from ..partitions import state_partitions as DOG_PARK_STATES


class AmenityRefreshConfig(Config):
    """Per-materialization config for dp_amenity_reextract."""
    limit: Optional[int] = None    # null/None = no limit
    workers: int = 6


def _coverage_summary(state: str) -> dict[str, Any]:
    """Pull amenity coverage stats for the state via the SQL helper."""
    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT total_active,
                   has_fence_pct, has_drinking_water_pct, double_gate_pct,
                   small_dog_area_pct, large_dog_area_pct, lighting_pct,
                   has_shade_pct, has_agility_pct, has_water_play_pct,
                   has_picnic_tables_pct, surface_overlay_pct, description_overlay_pct
              FROM public.dog_park_amenity_coverage(%s)
        """, (state,))
        row = cur.fetchone()
        if not row:
            return {}
        keys = ['total_active', 'has_fence_pct', 'has_drinking_water_pct',
                'double_gate_pct', 'small_dog_area_pct', 'large_dog_area_pct',
                'lighting_pct', 'has_shade_pct', 'has_agility_pct',
                'has_water_play_pct', 'has_picnic_tables_pct',
                'surface_overlay_pct', 'description_overlay_pct']
        return {k: (float(v) if v is not None else None) for k, v in zip(keys, row)}
    finally:
        conn.close()


def _promote_state(state: str) -> int:
    """Call promote_canonical_dog_park_policies_for_state(state) → int count."""
    conn = connect(); conn.set_client_encoding("UTF8")
    try:
        cur = conn.cursor()
        cur.execute("SELECT public.promote_canonical_dog_park_policies_for_state(%s)", (state,))
        n = cur.fetchone()[0]
        conn.commit()
        return int(n)
    finally:
        conn.close()


@asset(
    partitions_def=DOG_PARK_STATES,
    group_name="dog_park_amenity_refresh",
    description=(
        "Bulk re-extraction of dog-park amenities for all active parks in "
        "--state. Use after prompt extensions (e.g. adding new amenity "
        "boolean columns). Configurable limit for smoke / staged ramp. "
        "After extract, promotes BEP → policy and reports coverage."
    ),
)
def dp_amenity_reextract(
    context: AssetExecutionContext,
    config: AmenityRefreshConfig,
) -> MaterializeResult:
    state = context.partition_key
    started = datetime.now(timezone.utc)

    # 1. Run the extractor in all-active mode
    context.log.info(f"[{state}] starting all-active re-extraction "
                     f"(limit={config.limit}, workers={config.workers})")
    extract_metrics = run_extractor(
        state=state,
        workers=config.workers,
        include_no_website=True,
        apply=True,
        mode="all_active",
        limit=config.limit,
    )

    # 2. Promote BEP rows → dog_park_dog_policy columns
    promoted = _promote_state(state)
    context.log.info(f"[{state}] promoted {promoted} parks to policy")

    # 3. Coverage summary
    cov = _coverage_summary(state)
    context.log.info(f"[{state}] coverage: {cov}")

    ended = datetime.now(timezone.utc)
    duration = (ended - started).total_seconds()

    # Dagster-friendly metadata
    md: dict[str, Any] = {
        "state": MetadataValue.text(state),
        "mode": MetadataValue.text("all_active"),
        "limit": MetadataValue.text(str(config.limit) if config.limit else "(no limit)"),
        "workers": MetadataValue.int(config.workers),
        "duration_sec": MetadataValue.float(duration),
        "extract_exit_code": MetadataValue.int(int(extract_metrics.get("exit_code", -1))),
        "n_v2_before": MetadataValue.int(int(extract_metrics.get("n_v2_before", 0))),
        "n_v2_after": MetadataValue.int(int(extract_metrics.get("n_v2_after", 0))),
        "n_v2_added": MetadataValue.int(int(extract_metrics.get("n_v2_added", 0))),
        "promoted_count": MetadataValue.int(promoted),
    }
    for k, v in cov.items():
        if v is None:
            md[f"coverage.{k}"] = MetadataValue.text("null")
        elif isinstance(v, int) or k == 'total_active':
            md[f"coverage.{k}"] = MetadataValue.int(int(v))
        else:
            md[f"coverage.{k}"] = MetadataValue.float(v)
    md["stdout_tail"] = MetadataValue.md(
        f"```\n{extract_metrics.get('stdout_tail', '')[-3000:]}\n```"
    )

    return MaterializeResult(metadata=md)
