"""dog_park_coverage — per-state 8-op dog-park coverage pipeline.

Wraps scripts/dog_park_pipeline/ ops as Dagster assets. Each asset:
  - State-partitioned (StaticPartitionsDefinition over CA/OR/WA/+)
  - Writes one row to public.dog_park_coverage_runs for observability
  - Idempotent — safe to materialize repeatedly
  - Returns the op's metrics dict for downstream introspection

Sequence (locked by upstream/downstream dependencies):
  preflight → pip_address_city → reclassify_junk → generic_display_names
    → walk_catalogs → ingest_discovery_queue → run_extractor → retry_no_match

Per CA proof point 2026-05-25 LATE: 0% → 79.5% operator-posted-policy
coverage in one pass. See [[dog-park-coverage-playbook]].
"""
# NB: NO `from __future__ import annotations` — Dagster needs real class
# refs for AssetExecutionContext + Config introspection, not string lazies.
from datetime import datetime, timezone
from typing import Any

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue,
    MaterializeResult,
)

import sys
from pathlib import Path
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from scripts.dog_park_pipeline import (
    preflight_check,
    pip_address_city_backfill,
    reclassify_obvious_junk,
    generic_name_display_override,
    seed_state_operators,
    walk_catalogs,
    ingest_discovery_queue,
    run_extractor,
    retry_no_match,
    triage_needs_review,
    write_run_metric,
)

# Upstream deps from the broader pipeline. dog-park assets must run AFTER:
#   - ensure_dog_parks_gold: state has rows in dog_parks_gold
#   - operators_for_state:   operator registry seeded for the state
# Per Franz 2026-05-26 wiring into state_launch_job.
from .upstream_loaders import ensure_dog_parks_gold
from .operator_seeding import operators_for_state


# Aligned with the shared state_partitions (full US) so dog-park assets
# can chain with the beach pipeline in state_launch_job. Per Franz
# 2026-05-26 — was a bespoke 4-state static def; raised to full US.
# Add to scripts/dog_park_pipeline/state_operator_seeds.json BEFORE
# materializing a partition for a new state (otherwise seed_operators
# step is a no-op).
from ..partitions import state_partitions as DOG_PARK_STATES


def _persist(state: str, metrics: dict) -> None:
    """Write metrics row + log + return Dagster-friendly metadata."""
    try:
        started = datetime.fromisoformat(metrics["started_at"])
        ended = datetime.fromisoformat(metrics["ended_at"])
    except Exception:
        started = datetime.now(timezone.utc); ended = started
    write_run_metric(state, metrics["op"], started, ended, metrics)


def _metadata(metrics: dict) -> dict[str, Any]:
    """Convert metrics dict to Dagster metadata-friendly shape."""
    md: dict[str, Any] = {}
    for k, v in metrics.items():
        if k in ("stdout_tail", "stderr_tail"):
            md[k] = MetadataValue.md(f"```\n{v[:1500]}\n```")
        elif isinstance(v, (int, float)):
            md[k] = MetadataValue.int(int(v)) if isinstance(v, int) else MetadataValue.float(v)
        elif isinstance(v, list):
            md[k] = MetadataValue.json(v)
        else:
            md[k] = MetadataValue.text(str(v))
    return md


# ── 1. preflight ──────────────────────────────────────────────────────

@asset(
    partitions_def=DOG_PARK_STATES,
    deps=[ensure_dog_parks_gold, operators_for_state],
    group_name="dog_park_coverage",
    description="Verify Playwright browser, state inventory, env vars before pipeline run.",
)
def dp_preflight(context: AssetExecutionContext) -> MaterializeResult:
    state = context.partition_key
    m = preflight_check(state)
    _persist(state, m)
    if not m.get("ok"):
        context.log.warning(f"Preflight issues for {state}: {m.get('issues')}")
    return MaterializeResult(metadata=_metadata(m))


# ── 2. pip_address_city_backfill ──────────────────────────────────────

@asset(
    partitions_def=DOG_PARK_STATES,
    group_name="dog_park_coverage",
    deps=[dp_preflight],
    description="PIP-fill dog_parks_gold.address_city via ST_Contains(jurisdictions.geom).",
)
def dp_pip_address_city(context: AssetExecutionContext) -> MaterializeResult:
    state = context.partition_key
    m = pip_address_city_backfill(state)
    _persist(state, m)
    return MaterializeResult(metadata=_metadata(m))


# ── 3. reclassify_obvious_junk ────────────────────────────────────────

@asset(
    partitions_def=DOG_PARK_STATES,
    group_name="dog_park_coverage",
    deps=[dp_pip_address_city],
    description="Flip mis-tagged tiny + private-residence parks to is_active=false.",
)
def dp_reclassify_junk(context: AssetExecutionContext) -> MaterializeResult:
    state = context.partition_key
    m = reclassify_obvious_junk(state)
    _persist(state, m)
    return MaterializeResult(metadata=_metadata(m))


# ── 4. generic_name_display_override ──────────────────────────────────

@asset(
    partitions_def=DOG_PARK_STATES,
    group_name="dog_park_coverage",
    deps=[dp_reclassify_junk],
    description="Set display_name_override = city + name for generic-named parks.",
)
def dp_generic_display_names(context: AssetExecutionContext) -> MaterializeResult:
    state = context.partition_key
    m = generic_name_display_override(state)
    _persist(state, m)
    return MaterializeResult(metadata=_metadata(m))


# ── 5. seed_operators ─────────────────────────────────────────────────

@asset(
    partitions_def=DOG_PARK_STATES,
    group_name="dog_park_coverage",
    deps=[dp_generic_display_names],
    description="Seed city operators from state_operator_seeds.json. PIP-backfills inferred_operator_id. Walker needs ≥3 ops with ≥3 attributed parks each.",
)
def dp_seed_operators(context: AssetExecutionContext) -> MaterializeResult:
    state = context.partition_key
    m = seed_state_operators(state)
    _persist(state, m)
    return MaterializeResult(metadata=_metadata(m))


# ── 6. walk_catalogs ──────────────────────────────────────────────────

@asset(
    partitions_def=DOG_PARK_STATES,
    group_name="dog_park_coverage",
    deps=[dp_seed_operators],
    description="Walk top-N operator catalogs via web_search. Discover unmatched parks → queue.",
)
def dp_walk_catalogs(context: AssetExecutionContext) -> MaterializeResult:
    state = context.partition_key
    m = walk_catalogs(state, workers=4, min_parks=3, apply=True)
    _persist(state, m)
    if m.get("exit_code", 0) != 0:
        context.log.warning(f"walk_catalogs non-zero exit for {state}: {m.get('exit_code')}")
    return MaterializeResult(metadata=_metadata(m))


# ── 6. ingest_discovery_queue ─────────────────────────────────────────

@asset(
    partitions_def=DOG_PARK_STATES,
    group_name="dog_park_coverage",
    deps=[dp_walk_catalogs],
    description="Geocode pending queue entries via Google Places + INSERT into dog_parks_gold.",
)
def dp_ingest_queue(context: AssetExecutionContext) -> MaterializeResult:
    state = context.partition_key
    m = ingest_discovery_queue(state, apply=True)
    _persist(state, m)
    return MaterializeResult(metadata=_metadata(m))


# ── 7. run_extractor ──────────────────────────────────────────────────

@asset(
    partitions_def=DOG_PARK_STATES,
    group_name="dog_park_coverage",
    deps=[dp_ingest_queue],
    description="Extract amenities (smart_fetch + web_search routes). --include-no-website.",
)
def dp_run_extractor(context: AssetExecutionContext) -> MaterializeResult:
    state = context.partition_key
    m = run_extractor(state, workers=6, include_no_website=True, apply=True)
    _persist(state, m)
    return MaterializeResult(metadata=_metadata(m))


# ── 8. retry_no_match ─────────────────────────────────────────────────

@asset(
    partitions_def=DOG_PARK_STATES,
    group_name="dog_park_coverage",
    deps=[dp_run_extractor],
    description="Query-variant web_search retry for parks with name_match=false.",
)
def dp_retry_no_match(context: AssetExecutionContext) -> MaterializeResult:
    state = context.partition_key
    m = retry_no_match(state, workers=6, apply=True)
    _persist(state, m)
    return MaterializeResult(metadata=_metadata(m))


# ── 9. triage_needs_review ────────────────────────────────────────────

@asset(
    partitions_def=DOG_PARK_STATES,
    group_name="dog_park_coverage",
    deps=[dp_retry_no_match],
    description="Spatial dedup on borderline (0.6-0.79 sim) queue rows: Google Places geocode + ST_Distance vs best_match_fid. <150m → duplicate; >500m → pending (new park).",
)
def dp_triage_needs_review(context: AssetExecutionContext) -> MaterializeResult:
    state = context.partition_key
    m = triage_needs_review(state, apply=True)
    _persist(state, m)
    return MaterializeResult(metadata=_metadata(m))
