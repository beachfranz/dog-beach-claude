"""Backward-compat shim — _external_source_status moved to
scripts/loaders/_external_source_status.py on 2026-05-22 LATE
during the clean-pipeline-v2 promotion sweep.

Re-exports the public symbols so the 2 remaining sibling-import callers
in this directory (bulk_load_census_tracts.py, bulk_load_dog_amenities.py)
keep working until they're either promoted or archived.

When both dormant callers are archived, this shim file can be deleted too.
"""
import sys as _sys
from pathlib import Path as _Path

# Add repo root so `from scripts.loaders...` resolves when this file is
# loaded via the dormant callers' sys.path.insert(parent) trick.
_sys.path.insert(0, str(_Path(__file__).resolve().parent.parent.parent))

from scripts.loaders._external_source_status import record_status  # noqa: F401,E402
