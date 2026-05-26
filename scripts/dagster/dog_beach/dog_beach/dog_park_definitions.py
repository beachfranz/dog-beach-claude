"""dog_park_definitions — standalone Dagster Definitions for dog-park coverage.

Loadable INDEPENDENT of the main dog_beach.defs module (which has pre-existing
import-chain issues unrelated to dog-park work). Run with:

  dagster dev -m dog_beach.dog_park_definitions

Or from a code location config pointing at this module. The asset / job
definitions here are self-contained — they only depend on
scripts/dog_park_pipeline/ (pure Python, no Dagster resource graph).

Per-state run:
  dagster job execute -m dog_beach.dog_park_definitions \\
    -j dog_park_coverage_job --partition CA
"""
from pathlib import Path
import os
from dotenv import load_dotenv

# Load .env (mirrors main __init__.py pattern)
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
load_dotenv(_REPO_ROOT / "scripts" / "pipeline" / ".env")

from dagster import Definitions, define_asset_job, AssetSelection

from .assets.dog_park_coverage import (
    dp_preflight,
    dp_pip_address_city,
    dp_reclassify_junk,
    dp_generic_display_names,
    dp_walk_catalogs,
    dp_ingest_queue,
    dp_run_extractor,
    dp_retry_no_match,
)


dog_park_coverage_job = define_asset_job(
    name="dog_park_coverage_job",
    selection=AssetSelection.groups("dog_park_coverage"),
    description=(
        "8-op dog-park coverage pipeline. State-partitioned (CA/OR/WA). "
        "Per CA proof point 2026-05-25 LATE: 0% → 79.5% operator-posted-policy "
        "coverage in one pass. See [[dog-park-coverage-playbook]]."
    ),
)


defs = Definitions(
    assets=[
        dp_preflight,
        dp_pip_address_city,
        dp_reclassify_junk,
        dp_generic_display_names,
        dp_walk_catalogs,
        dp_ingest_queue,
        dp_run_extractor,
        dp_retry_no_match,
    ],
    jobs=[dog_park_coverage_job],
)
