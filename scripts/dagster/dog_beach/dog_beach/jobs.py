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
from dagster import in_process_executor, multiprocess_executor

# Dog-park coverage pipeline — 9 ops, state-partitioned (CA/OR/WA/MD).
# Per CA proof point: 0% → 79.5%. See [[dog-park-coverage-playbook]].
#
# IN-PROCESS EXECUTOR: each op already does ThreadPoolExecutor parallelism
# internally for per-park work. Running ops in the same Python process
# avoids Windows multiprocess-spawn re-import issues that surfaced 2026-05-25
# LATE (walker step crashed silently in subprocess; ran clean in-process).
dog_park_coverage_job = define_asset_job(
    name="dog_park_coverage_job",
    selection=AssetSelection.groups("dog_park_coverage"),
    executor_def=in_process_executor,
    description=(
        "9-op dog-park coverage pipeline. State-partitioned (CA/OR/WA/MD). "
        "Run with: dagster job execute -j dog_park_coverage_job --partition OR"
    ),
)


# Dog-park photo pipeline (Franz 2026-05-26).
# Mirrors the beach Phase 31 photo cascade for dog parks:
# Flickr + Wikimedia + Unsplash + Websearch -> vision tagging -> curate.
#
# MULTIPROCESS EXECUTOR (Franz 2026-05-26 — parallel source fan-out):
# the 4 source loaders have NO inter-deps (all depend only on
# dp_triage_needs_review upstream), so they fan out in parallel via
# multiprocess_executor's worker pool. Vision + curate stay sequential
# via their existing deps. Coverage job stayed in_process because dp_walk_catalogs
# had real Windows multiprocess-spawn import issues; photo loaders are
# simpler subprocess.run() wrappers + don't hit that bug.
#
# max_concurrent=4 limits parallel workers to the 4 source loaders so
# we don't blow past API rate caps. Adjust upward if vision-tag chunk
# size scales beyond a single worker.
dog_park_photo_job = define_asset_job(
    name="dog_park_photo_job",
    selection=AssetSelection.groups("phase_31_dp_photos"),
    executor_def=multiprocess_executor.configured({"max_concurrent": 4}),
    description=(
        "Dog-park photo pipeline: Flickr + Wikimedia + Unsplash + Websearch "
        "(parallel) -> vision tags -> vision-gated curate (top 3 per park "
        "where has_dog=true OR scene in outdoor/park/beach). State-partitioned. "
        "Cost cap ~$5/state for vision."
    ),
)


# Beach photo pipeline (Franz 2026-05-26 — mirror of dog_park_photo_job).
# Selects phase_31_photos minus the two assets whose partition_def doesn't
# match state_partitions (vision_tags per-source, keep_prob_model unparted)
# PLUS photos_wikimedia (which lives in group phase_29_to_33_per_fid, not
# phase_31_photos — historical placement; the group selection alone misses
# it).
# Multiprocess executor so the 5+ source loaders fan out in parallel.
# Use: dagster job execute -j beach_photo_job --partition MD
from .assets.photos_and_vision import (
    photo_vision_tags as _beach_vision_tags,
    photo_keep_prob_model as _beach_keep_prob,
)
from .assets.per_fid_enrichment import photos_wikimedia as _beach_photos_wikimedia
beach_photo_job = define_asset_job(
    name="beach_photo_job",
    selection=(
        (AssetSelection.groups("phase_31_photos")
         - AssetSelection.assets(_beach_vision_tags, _beach_keep_prob))
        | AssetSelection.assets(_beach_photos_wikimedia)
    ),
    executor_def=multiprocess_executor.configured({"max_concurrent": 6}),
    description=(
        "Beach photo pipeline: Flickr + Wikimedia + Unsplash + Websearch + CCC "
        "(parallel) + state_photo_galleries + photo_centroid_backfill -> "
        "photos_curate. State-partitioned. Vision tagging + keep_prob_model "
        "run via separate triggers (different partition_defs)."
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
    ensure_dog_parks_gold,
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
    photo_vision_tags, photo_keep_prob_model,   # excluded from state_launch_job
)
# DP photo assets (dp_photos_*, dp_photo_vision_tags, dp_photos_curate) are
# referenced by dog_park_photo_job via group selection, not named import.
from .assets.scoring_and_audit import (
    hourly_status_refresh, codify_coverage_check,
    daily_refresh_fire, field_population_check,
    weather_advisories_refresh,
    codify_coverage_audit, codify_gap_clone,
    dog_park_data_quality_audit,
)
from .assets.weather_grid import (
    refresh_weather_grid, rebuild_weather_grid_inventory,
)
from .assets.beach_discovery import (
    beach_walk_state_parks, beach_walk_wikipedia, beach_ingest_queue,
)


# ── Full state launch (Phases 1-33) ──────────────────────────────────

state_launch_job = define_asset_job(
    name="state_launch_job",
    description=(
        "Full pipeline for a single state (Phases 1-33). Mirrors what "
        "`python scripts/run_state_pipeline.py --state X` does in the CLI. "
        "Runs beach + dog-park subsystems for the state. Per Franz "
        "2026-05-26 — dog-park coverage and photo assets now included so "
        "one launch fires everything."
    ),
    selection=(
        AssetSelection.assets(
            env_preflight,
            chain_integrity_check,
            state_policy_seed, seasonal_closure_seed, state_park_url_check,
            ensure_tiger_places, ensure_pad_us,
            pad_us_geom_geog_check, ensure_county_subdivisions, pad_us_manager_class_preflight,
            ensure_overpass, ensure_poi_landing,
            ensure_amenities, ensure_dog_features,
            ensure_dog_parks_gold,
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
            hourly_status_refresh, codify_coverage_check,
            daily_refresh_fire, field_population_check,
        )
        # Beach photo cascade (Franz 2026-05-26 — was Wikimedia-only):
        # Flickr/Pixabay/Pexels/Unsplash/CCC + state_photo_galleries +
        # photo_centroid_backfill + photos_curate. Subtract the two
        # group members whose partitions_def doesn't match state_partitions
        # (Dagster requires all selected assets to share a partition def):
        #   photo_vision_tags (vision_source_partitions per source)
        #   photo_keep_prob_model (unpartitioned — single global model)
        # Both run via separate triggers outside state_launch_job.
        | (AssetSelection.groups("phase_31_photos")
           - AssetSelection.assets(photo_vision_tags, photo_keep_prob_model))
        # Dog-park coverage (10 ops): seeded operators + classify + amenity extract
        | AssetSelection.groups("dog_park_coverage")
        # Dog-park photos (6 ops): Flickr/Wikimedia/Unsplash/Websearch + vision + curate
        | AssetSelection.groups("phase_31_dp_photos")
        # Beach discovery (Franz 2026-05-27 — state-park-walker + ingest,
        # Wikipedia excluded since it runs on monthly cron per Franz 2026-05-27)
        | AssetSelection.assets(beach_walk_state_parks, beach_ingest_queue)
    ),
)


# ── Beach discovery job (annual cadence per Franz 2026-05-27) ────────
# Per Franz directive: beach discovery runs ONLY when a state comes on-line
# (via state_launch_job) AND once per year thereafter (this job, fired by
# annual_beach_discovery_schedule). Don't burn LLM dollars walking the
# same state-park sites + Wikipedia articles monthly when they change rarely.

beach_discovery_job = define_asset_job(
    name="beach_discovery_job",
    description=(
        "Annual beach-discovery re-walk per state. Fires both walkers + "
        "ingest: beach_walk_state_parks (per-state-handler) + "
        "beach_walk_wikipedia (Sonnet enumerates List_of_beaches_in_<State>) "
        "→ beach_ingest_queue (Google Places geocode → arena → "
        "promote_to_gold, scoring_tier='none'). "
        "Per state cost ~$0.05 Sonnet + ~$0.20 Google Places. Annual run "
        "across 50 states ≈ $12.50/year. Per Franz 2026-05-27 — state-park "
        "sites + Wikipedia change slowly enough that annual is sufficient."
    ),
    selection=AssetSelection.assets(
        beach_walk_state_parks, beach_walk_wikipedia, beach_ingest_queue),
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


# ── Weather advisories refresh (Phase 32.5, cross-state) ─────────────

weather_advisories_job = define_asset_job(
    name="weather_advisories_job",
    description=(
        "Phase 32.5 — derives beach_advisory rows for every scored beach "
        "across all states. Runs compute_weather_advisories.py --all-scored. "
        "Powers the Cautions card on beach.html."
    ),
    selection=AssetSelection.assets(weather_advisories_refresh),
)


# ── Codify coverage audit + gap clone (Phases 32.6 + 32.7) ──────────

codify_coverage_audit_job = define_asset_job(
    name="codify_coverage_audit_job",
    description=(
        "Phase 32.6 — per-state structured-codify coverage audit. Raises "
        "when any state drops below the threshold; logs gap-by-jurisdiction."
    ),
    selection=AssetSelection.assets(codify_coverage_audit),
)

codify_gap_clone_job = define_asset_job(
    name="codify_gap_clone_job",
    description=(
        "Phase 32.7 — runs codify_clone_gap_beaches.py across all MVP+ "
        "states. Catches newly-added beaches in already-codified cities."
    ),
    selection=AssetSelection.assets(codify_gap_clone),
)

dog_park_data_quality_audit_job = define_asset_job(
    name="dog_park_data_quality_audit_job",
    description=(
        "Phase 32.8 — surfaces dog-park amenity inconsistencies "
        "(currently: lighting=true + dawn-dusk hours). Raises on any "
        "detection."
    ),
    selection=AssetSelection.assets(dog_park_data_quality_audit),
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
