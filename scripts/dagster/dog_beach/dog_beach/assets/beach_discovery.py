"""beach_discovery — per-state beach inventory discovery from walkers.

Three Dagster assets fan out the beach-discovery pipeline shipped 2026-05-27:

  1. beach_walk_state_parks  — fetch <state>-parks-system catalog +
     per-unit pages → LLM-extract named beach features → stage in
     public.beach_discovery_queue (handler='ut_stateparks_v1' etc.).
     Currently UT handler only; add per-state handlers as inventory gaps
     emerge.

  2. beach_walk_wikipedia    — fetch en.wikipedia.org/wiki/
     List_of_beaches_in_<State>, LLM-enumerate, stage to queue with
     handler='wikipedia_v1'. Beach-only (Wikipedia coverage for dog parks
     is too thin per Franz 2026-05-27).

  3. beach_ingest_queue       — for every pending row in queue:
     Google Places geocode (fallback when handler didn't supply coords) →
     dedupe vs arena (POI + OSM + manual all included) → INSERT arena →
     batched promote_to_gold (10 fids per batch to stay under CPAD-spatial-
     join statement_timeout for large coastal states).

Operator-catalog beach extraction is the SIDE-EFFECT of dp_walk_catalogs
(unified additive refactor 2026-05-27 — same fetched content, second
LLM call for beaches, routes to beach_discovery_queue). It runs in
dog_park_coverage group; beach_ingest_queue picks up those rows too.

CADENCE (Franz directive 2026-05-27): beach discovery runs ONLY when a
state comes on-line (via state_launch_job — includes
beach_walk_state_parks + beach_ingest_queue) AND once per year thereafter
(via beach_discovery_job + annual_beach_discovery_schedule — fires both
walkers + ingest). State-park sites + Wikipedia articles change slowly
enough that annual is sufficient. Don't burn LLM dollars monthly.
"""
from datetime import datetime, timezone
from typing import Any
import sys
from pathlib import Path

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue,
    MaterializeResult,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from scripts.walk_state_park_beaches import walk_state as _walk_state_parks
from scripts.walk_wikipedia_beach_lists import walk as _walk_wikipedia
from scripts.ingest_beach_discovery_queue import ingest as _ingest_queue

from ..partitions import state_partitions as STATE_PARTITIONS
from .catalog_assembly import beach_inventory_check
from .upstream_loaders import state_park_url_check, ensure_pad_us


def _metadata(stats: dict) -> dict[str, Any]:
    md: dict[str, Any] = {}
    for k, v in (stats or {}).items():
        if isinstance(v, bool):
            md[k] = MetadataValue.bool(v)
        elif isinstance(v, int):
            md[k] = MetadataValue.int(v)
        elif isinstance(v, float):
            md[k] = MetadataValue.float(v)
        elif isinstance(v, (list, dict)):
            md[k] = MetadataValue.json(v)
        else:
            md[k] = MetadataValue.text(str(v))
    return md


# ── 1. beach_walk_state_parks ─────────────────────────────────────────

@asset(
    partitions_def=STATE_PARTITIONS,
    deps=[beach_inventory_check, state_park_url_check, ensure_pad_us],
    group_name="phase_25b_beach_discovery",
    description=(
        "State-park-system walker. Reads state_park_urls.json[STATE], fetches "
        "listing + per-unit pages, LLM-extracts named beach features into "
        "beach_discovery_queue (source_handler='<state_handler>_v1'). "
        "States with HANDLERS in scripts/walk_state_park_beaches.py only "
        "produce output; others soft-skip. Idempotent via UNIQUE constraint "
        "on (state, parent_park_name, beach_name, source_handler)."
    ),
)
def beach_walk_state_parks(
    context: AssetExecutionContext,
) -> MaterializeResult:
    state = context.partition_key
    try:
        stats = _walk_state_parks(state, dry_run=False, limit=None)
    except SystemExit as e:
        # walk_state raises SystemExit when no handler exists for the state
        # ('No handler for state=XX. Add to HANDLERS in ...'). Treat as
        # soft-skip rather than failure — only some states have handlers.
        context.log.warning(f"beach_walk_state_parks: {state}: {e}")
        stats = {"parks_seen": 0, "no_handler": True}
    return MaterializeResult(metadata=_metadata(stats))


# ── 2. beach_walk_wikipedia ───────────────────────────────────────────

@asset(
    partitions_def=STATE_PARTITIONS,
    deps=[beach_inventory_check],
    group_name="phase_25b_beach_discovery",
    description=(
        "Wikipedia per-state beach-list walker. Fetches "
        "en.wikipedia.org/wiki/List_of_beaches_in_<State>, Sonnet enumerates "
        "every named beach with county, stages to beach_discovery_queue "
        "(source_handler='wikipedia_v1'). Beach-only (per Franz 2026-05-27). "
        "Cost ~$0.05/state per Sonnet call. NOT in state_launch_job — runs "
        "via monthly_wikipedia_walk_schedule to avoid relaunch cost."
    ),
)
def beach_walk_wikipedia(
    context: AssetExecutionContext,
) -> MaterializeResult:
    state = context.partition_key
    stats = _walk_wikipedia(state, dry_run=False, limit=None)
    return MaterializeResult(metadata=_metadata(stats))


# ── 3. beach_ingest_queue ─────────────────────────────────────────────

@asset(
    partitions_def=STATE_PARTITIONS,
    deps=[beach_walk_state_parks],
    group_name="phase_25b_beach_discovery",
    description=(
        "Promote pending beach_discovery_queue rows to arena + beaches_gold. "
        "Google Places geocode fallback for rows missing coords (operator + "
        "Wikipedia walkers). Dedupe vs arena (POI/OSM/manual). Batched "
        "promote_to_gold (10 fids/batch) keeps CPAD spatial join under "
        "statement_timeout. Reads queue rows from ALL upstream walkers "
        "(state-park, Wikipedia, operator-catalog side-effect); declared "
        "dep on beach_walk_state_parks is the per-launch trigger — other "
        "walker outputs are picked up opportunistically from the queue. "
        "scoring_tier defaults to 'none', is_scoreable=false."
    ),
)
def beach_ingest_queue(
    context: AssetExecutionContext,
) -> MaterializeResult:
    state = context.partition_key
    stats = _ingest_queue(state, dry_run=False)
    return MaterializeResult(metadata=_metadata(stats))
