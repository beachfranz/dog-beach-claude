"""dog_park_pipeline — clean per-state op entry-points for Dagster wrapping.

Each op:
  - Takes state: str (e.g., 'OR', 'WA', 'CA')
  - Wraps existing scripts where possible
  - Returns metrics dict for observability
  - Idempotent — safe to re-run

Optimal sequence (per CA session 2026-05-25 LATE → 79.5% coverage):
  1. preflight_check         — Playwright + browser installed
  2. pip_address_city_backfill — UPDATE address_city via ST_Contains
  3. reclassify_obvious_junk  — flip tiny/HOA parks is_active=false
  4. generic_name_display_override — display_name_override = city + name
  5. walk_catalogs           — discover unmatched parks → queue
  6. ingest_discovery_queue  — geocode + INSERT new gold rows
  7. run_extractor           — extract all default-source parks (incl new)
  8. retry_no_match          — rescue gaps with stronger web_search prompt

See pin [[dog-park-coverage-playbook]] for full rationale.
"""
from .preflight import preflight_check
from .pip_backfill import pip_address_city_backfill
from .reclassify import reclassify_obvious_junk
from .display_names import generic_name_display_override
from .seed_operators import seed_state_operators
from .walk import walk_catalogs
from .ingest import ingest_discovery_queue
from .extract import run_extractor
from .retry import retry_no_match
from .triage import triage_needs_review
from .metrics import write_run_metric
from .dp_photos import (
    dp_photos_load_flickr,
    dp_photos_load_wikimedia,
    dp_photos_load_websearch,
    dp_photos_vision_tag,
    dp_photos_curate,
)

__all__ = [
    "preflight_check",
    "pip_address_city_backfill",
    "reclassify_obvious_junk",
    "generic_name_display_override",
    "seed_state_operators",
    "walk_catalogs",
    "ingest_discovery_queue",
    "run_extractor",
    "retry_no_match",
    "triage_needs_review",
    "write_run_metric",
    "dp_photos_load_flickr",
    "dp_photos_load_wikimedia",
    "dp_photos_load_websearch",
    "dp_photos_vision_tag",
    "dp_photos_curate",
]
