"""Pre-defined execution plans (jobs).

Jobs bundle asset selections into named, schedulable units. Each job
encapsulates a recipe (e.g. "full state launch", "daily refresh only",
"bep refire").

Jobs:
  state_launch_job         All 33 phases for a single state (full launch path)
  catalog_assembly_job     Phases 1-25 (everything up through dedup_distance_name)
  llm_cascade_job          Phases 26-29 (operator extract → merge → rebuild_beach_evidence → section)
  daily_refresh_job        Phase 32 only (scoring fan-out)
  pipeline_health_audit_job Phase 33 only (population audit)
  rebuild_beach_evidence_job           Phase 28 only (for ad-hoc refire after operator policy changes)

Materializing via job: `dagster job execute -j state_launch_job --config-yaml ...`
Or via UI's Launchpad.
"""

from dagster import define_asset_job, AssetSelection

# Dog-park coverage pipeline — 8 ops, state-partitioned (CA/OR/WA).
# Per CA proof point: 0% → 79.5%. See [[dog-park-coverage-playbook]].
dog_park_coverage_job = define_asset_job(
    name="dog_park_coverage_job",
    selection=AssetSelection.groups("dog_park_coverage"),
    description=(
        "8-op dog-park coverage pipeline. State-partitioned (CA/OR/WA). "
        "Run with: dagster job execute -j dog_park_coverage_job --partition OR"
    ),
)

from .assets.upstream_loaders import (
    env_preflight,
    chain_integrity_check,
    state_policy_seed,
    seasonal_closure_seed,
    state_park_url_check,
    ensure_tiger_places,
    ensure_pad_us,
    pad_us_geom_geog_check,
    ensure_county_subdivisions,
    pad_us_manager_class_preflight,
    ensure_overpass,
    ensure_poi_landing,
    ensure_amenities,
    ensure_dog_features,
    precheck,
)
from .assets.operator_seeding import operators_for_state
from .assets.catalog_assembly import (
    arena_seed, cluster_group, cluster_extras, promote_to_gold,
    beach_inventory_check, ensure_pip_membership, validate_state_geom,
    address_poi, address_city, name_source, strip_plus_codes,
    catchment_refresh,
    purge_pollution, dedup, dedup_distance_name, geom_queue,
)
from .assets.operator_llm_cascade import (
    operator_llm_extract_for_state,
    operator_merge,
    rebuild_beach_evidence,
    gold_set_candidates,
    gold_set_review_gate,
)
from .assets.per_fid_enrichment import (
    refresh_nearest_dog_park, codify_cascade,
    zone_rules_v2_refresh, operating_hours_refresh,
    section_extract, harvest_park_text, descriptions, descriptions_audit, photos_wikimedia,
)
from .assets.photos_and_vision import (
    state_photo_galleries, photo_centroid_backfill, photos_curate,
)
from .assets.scoring_and_audit import (
    hourly_status_refresh, codify_coverage_check,
    daily_refresh_fire, field_population_check,
)
from .assets.weather_grid import (
    refresh_weather_grid, rebuild_weather_grid_inventory,
)


# ── Full state launch (Phases 1-33) ──────────────────────────────────

state_launch_job = define_asset_job(
    name="state_launch_job",
    description=(
        "Full pipeline for a single state (Phases 1-33). Mirrors what "
        "`python scripts/run_state_pipeline.py --state X` does in the CLI. "
        "Run from the launchpad: select state partition and launch."
    ),
    selection=AssetSelection.assets(
        env_preflight,
        chain_integrity_check,
        state_policy_seed, seasonal_closure_seed, state_park_url_check,
        ensure_tiger_places, ensure_pad_us,
        pad_us_geom_geog_check, ensure_county_subdivisions, pad_us_manager_class_preflight,
        ensure_overpass, ensure_poi_landing,
        ensure_amenities, ensure_dog_features,
        precheck,
        operators_for_state,
        arena_seed, cluster_group, cluster_extras, promote_to_gold,
        beach_inventory_check, ensure_pip_membership, validate_state_geom,
        address_poi, address_city, name_source, strip_plus_codes,
        catchment_refresh,
        refresh_nearest_dog_park, codify_cascade,
        purge_pollution, dedup, dedup_distance_name, geom_queue,
        operator_llm_extract_for_state, operator_merge, rebuild_beach_evidence,
        zone_rules_v2_refresh, operating_hours_refresh,
        gold_set_candidates, gold_set_review_gate,
        section_extract, harvest_park_text, descriptions, descriptions_audit, photos_wikimedia,
        state_photo_galleries, photo_centroid_backfill, photos_curate,
        hourly_status_refresh, codify_coverage_check,
        daily_refresh_fire, field_population_check,
    ),
)


# ── Catalog assembly only (Phases 1-25) ──────────────────────────────

catalog_assembly_job = define_asset_job(
    name="catalog_assembly_job",
    description="Phases 1-25 — everything up through late-stage dedup.",
    selection=AssetSelection.assets(
        chain_integrity_check,
        state_policy_seed, seasonal_closure_seed, state_park_url_check,
        ensure_tiger_places, ensure_pad_us,
        pad_us_geom_geog_check, ensure_county_subdivisions, pad_us_manager_class_preflight,
        ensure_overpass, ensure_poi_landing,
        ensure_amenities, ensure_dog_features, precheck,
        operators_for_state,
        arena_seed, cluster_group, cluster_extras, promote_to_gold,
        beach_inventory_check, ensure_pip_membership, validate_state_geom,
        address_poi, address_city, name_source, strip_plus_codes,
        catchment_refresh,
        refresh_nearest_dog_park, codify_cascade,
        purge_pollution, dedup, dedup_distance_name, geom_queue,
    ),
)


# ── LLM cascade (Phases 26-29) ───────────────────────────────────────

llm_cascade_job = define_asset_job(
    name="llm_cascade_job",
    description=(
        "Phases 26-29: operator LLM extract → merge → bep refire → "
        "per-beach section extract. Costs LLM dollars."
    ),
    selection=AssetSelection.assets(
        env_preflight,
        operator_llm_extract_for_state, operator_merge,
        rebuild_beach_evidence,
        gold_set_candidates, gold_set_review_gate,
        section_extract,
    ),
)


# ── Daily refresh (Phase 32 only) ────────────────────────────────────

daily_refresh_job = define_asset_job(
    name="daily_refresh_job",
    description="Phase 32 only — fires daily-beach-refresh edge function per state.",
    selection=AssetSelection.assets(daily_refresh_fire),
)


# ── Pipeline health audit (Phase 33 only) ────────────────────────────

pipeline_health_audit_job = define_asset_job(
    name="pipeline_health_audit_job",
    description="Phase 33 only — runs state_population_audit.py per state.",
    selection=AssetSelection.assets(field_population_check),
)


# ── BEP refire only (Phase 28) ───────────────────────────────────────

rebuild_beach_evidence_job = define_asset_job(
    name="rebuild_beach_evidence_job",
    description=(
        "Phase 28 only — refire_bep_cascade for state's tier-1+2 fids. "
        "Ad-hoc after operator policy changes or PAD-US re-loads."
    ),
    selection=AssetSelection.assets(rebuild_beach_evidence),
)


# ── Weather grid refresh (W1.5) ──────────────────────────────────────

weather_grid_refresh_job = define_asset_job(
    name="weather_grid_refresh_job",
    description=(
        "W1.5 — refreshes weather_grid_hourly from Open-Meteo. Per-cell "
        "tier-based stale thresholds (hot 1h, warm 6h, cold 24h). See "
        "[[weather-grid-reference-layer]]."
    ),
    selection=AssetSelection.assets(refresh_weather_grid),
)


# ── Weather grid inventory rebuild (W1.8 backstop) ───────────────────

weather_grid_inventory_job = define_asset_job(
    name="weather_grid_inventory_job",
    description=(
        "W1.8 — daily backstop. UNION-rebuilds weather_grid from "
        "active beaches_gold + dog_parks_gold in case triggers drift."
    ),
    selection=AssetSelection.assets(rebuild_weather_grid_inventory),
)
