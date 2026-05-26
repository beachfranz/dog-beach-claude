"""Dog Beach Scout — Dagster project (v2, fresh 2026-05-13).

Replaces the prior project at scripts/dagster/_archive_2026_05_13/ which
predated run_state_pipeline.py and had accumulated stale assets (e.g. the
verdicts.beaches asset pointing at the dropped public.beaches table).

This v2 is organized around the current 33-phase pipeline (see
docs/pipeline-overview.md). One asset per substantive pipeline phase,
per-state partitioned where natural, with AssetChecks replacing each
phase's criterion SQL.

Migration is intentionally incremental — only phases that have been
lifted into Dagster live here. run_state_pipeline.py remains the canonical
orchestrator until every phase is migrated and the feedback pin
feedback_use_pipeline_infrastructure.md is updated.

Resource configuration: every external API + DB connection is exposed as
a ConfigurableResource (see resources.py). Secrets resolve from
scripts/pipeline/.env at runtime via EnvVar. Resources for parked or
deprecated APIs (BestTime, Mapillary, Geoapify, NPS Multimedia) include
is_active=False so calling them when off raises a clear error.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from dagster import Definitions, EnvVar

# Load .env (mirrors run_state_pipeline.py's pattern).
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
load_dotenv(_REPO_ROOT / "scripts" / "pipeline" / ".env")

from .resources import (
    # database
    PostgresPoolerResource,
    PostgresSessionResource,
    # LLM / search
    AnthropicResource,
    TavilyResource,
    # supabase platform
    SupabaseEdgeFunctionResource,
    SubprocessResource,
    # external GIS
    ArcGISRestResource,
    OverpassResource,
    GoogleMapsResource,
    GeoapifyResource,
    # scoring inputs
    OpenMeteoResource,
    NOAACoOpsResource,
    BestTimeResource,
    # photos
    WikimediaCommonsResource,
    FlickrResource,
    PixabayResource,
    PexelsResource,
    UnsplashResource,
    MapillaryResource,
    NPSMultimediaResource,
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
    arena_seed,
    cluster_group,
    cluster_extras,
    promote_to_gold,
    beach_inventory_check,
    ensure_pip_membership,
    validate_state_geom,
    address_poi,
    address_city,
    name_source,
    strip_plus_codes,
    catchment_refresh,
    purge_pollution,
    dedup,
    dedup_distance_name,
    geom_queue,
)
from .assets.operator_llm_cascade import (
    operator_policy_extraction,
    operator_llm_extract_for_state,
    operator_merge,
    rebuild_beach_evidence,
    gold_set_candidates,
    gold_set_review_gate,
)
from .assets.per_fid_enrichment import (
    refresh_nearest_dog_park,
    codify_cascade,
    zone_rules_v2_refresh,
    operating_hours_refresh,
    section_extract,
    harvest_park_text,
    descriptions,
    descriptions_audit,
    photos_wikimedia,
)
from .assets.photos_and_vision import (
    photos_flickr,
    photos_pixabay,
    photos_pexels,
    photos_unsplash,
    photos_ccc,
    photo_vision_tags,
    photo_keep_prob_model,
    state_photo_galleries,
    photo_centroid_backfill,
    photos_curate,
    # Dog-park photo pipeline (Franz 2026-05-26)
    dp_photos_flickr,
    dp_photos_wikimedia,
    dp_photos_unsplash,
    dp_photo_vision_tags,
    dp_photos_curate,
)
from .assets.scoring_and_audit import (
    hourly_status_refresh,
    codify_coverage_check,
    daily_refresh_fire,
    field_population_check,
)
from .assets.weather_grid import (
    refresh_weather_grid,
    rebuild_weather_grid_inventory,
)
from .assets.dog_park_coverage import (
    dp_preflight,
    dp_pip_address_city,
    dp_reclassify_junk,
    dp_generic_display_names,
    dp_seed_operators,
    dp_walk_catalogs,
    dp_ingest_queue,
    dp_run_extractor,
    dp_retry_no_match,
    dp_triage_needs_review,
)
from .checks.operator_seeding import operators_for_state_has_breadth
from .checks.catalog_assembly import (
    promote_to_gold_complete,
    name_source_complete,
    catchment_refresh_complete,
    descriptions_coverage,
    daily_refresh_coverage,
)
from .assets.frontend import ALL_LINEAGE_SPECS
from .sensors.reactive import (
    pad_us_loaded_sensor,
    new_operators_sensor,
    new_fids_sensor,
    arena_changed_sensor,
    geom_change_sensor,
)
from .schedules.time_based import (
    daily_beach_refresh_schedule,
    hourly_now_refresh_schedule,
    weekly_pipeline_health_schedule,
    hourly_weather_grid_schedule,
    daily_weather_grid_inventory_schedule,
    monthly_dog_park_coverage_schedule,
)
from .jobs import (
    state_launch_job,
    catalog_assembly_job,
    llm_cascade_job,
    daily_refresh_job,
    pipeline_health_audit_job,
    rebuild_beach_evidence_job,
    weather_grid_refresh_job,
    weather_grid_inventory_job,
    dog_park_coverage_job,
    dog_park_photo_job,
)


_SUPABASE_HOST = "aws-1-us-east-1.pooler.supabase.com"
_SUPABASE_USER = "postgres.ehlzbwtrsxaaukurekau"
_SUPABASE_URL = "https://ehlzbwtrsxaaukurekau.supabase.co"


defs = Definitions(
    assets=[
        # Phase 0 env preflight + upstream loaders
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
        # Phase 10 operator seeding
        operators_for_state,
        # Phases 11-25 catalog assembly
        arena_seed,
        cluster_group,
        cluster_extras,
        promote_to_gold,
        beach_inventory_check,
        ensure_pip_membership,
        validate_state_geom,
        address_poi,
        address_city,
        name_source,
        strip_plus_codes,
        catchment_refresh,
        refresh_nearest_dog_park,
        codify_cascade,
        purge_pollution,
        dedup,
        dedup_distance_name,
        geom_queue,
        # Phases 26-28 operator LLM + cascade
        operator_policy_extraction,
        operator_llm_extract_for_state,
        operator_merge,
        rebuild_beach_evidence,
        zone_rules_v2_refresh,
        operating_hours_refresh,
        # Phase 28.5 — gold-set curator queue + review gate
        gold_set_candidates,
        gold_set_review_gate,
        # Phases 29-31 per-fid LLM + photos
        section_extract,
        harvest_park_text,
        descriptions,
        descriptions_audit,
        photos_wikimedia,
        # Phase 31 photo loaders (per-source) + vision + ML
        photos_flickr,
        photos_pixabay,
        photos_pexels,
        photos_unsplash,
        photos_ccc,
        photo_vision_tags,
        photo_keep_prob_model,
        state_photo_galleries,
        photo_centroid_backfill,
        photos_curate,
        # Phases 32-33 scoring + audit
        hourly_status_refresh,
        codify_coverage_check,
        daily_refresh_fire,
        field_population_check,
        # Weather reference layer (W1.5 + W1.8)
        refresh_weather_grid,
        rebuild_weather_grid_inventory,
        # Dog-park coverage pipeline (10 ops, state-partitioned CA/OR/WA/MD)
        dp_preflight,
        dp_pip_address_city,
        dp_reclassify_junk,
        dp_generic_display_names,
        dp_seed_operators,
        dp_walk_catalogs,
        dp_ingest_queue,
        dp_run_extractor,
        dp_retry_no_match,
        dp_triage_needs_review,
        # Dog-park photo pipeline (Franz 2026-05-26)
        dp_photos_flickr,
        dp_photos_wikimedia,
        dp_photos_unsplash,
        dp_photo_vision_tags,
        dp_photos_curate,
        # Lineage-only AssetSpecs for downstream consumers
        *ALL_LINEAGE_SPECS,
    ],
    sensors=[
        pad_us_loaded_sensor,
        new_operators_sensor,
        new_fids_sensor,
        arena_changed_sensor,
        geom_change_sensor,
    ],
    schedules=[
        daily_beach_refresh_schedule,
        hourly_now_refresh_schedule,
        weekly_pipeline_health_schedule,
        hourly_weather_grid_schedule,
        daily_weather_grid_inventory_schedule,
        monthly_dog_park_coverage_schedule,
    ],
    jobs=[
        state_launch_job,
        catalog_assembly_job,
        llm_cascade_job,
        daily_refresh_job,
        pipeline_health_audit_job,
        rebuild_beach_evidence_job,
        weather_grid_refresh_job,
        weather_grid_inventory_job,
        dog_park_coverage_job,
        dog_park_photo_job,
    ],
    asset_checks=[
        operators_for_state_has_breadth,
        promote_to_gold_complete,
        name_source_complete,
        catchment_refresh_complete,
        descriptions_coverage,
        daily_refresh_coverage,
    ],
    resources={
        # ─── database ───
        "postgres": PostgresPoolerResource(
            host=_SUPABASE_HOST, port=6543, user=_SUPABASE_USER,
            password=EnvVar("SUPABASE_DB_PASSWORD"),
        ),
        "postgres_session": PostgresSessionResource(
            host=_SUPABASE_HOST, port=5432, user=_SUPABASE_USER,
            password=EnvVar("SUPABASE_DB_PASSWORD"),
        ),
        # ─── LLM / search ───
        "anthropic": AnthropicResource(
            api_key=EnvVar("ANTHROPIC_API_KEY"),
        ),
        "tavily": TavilyResource(
            api_key=EnvVar("TAVILY_API_KEY"),
        ),
        # ─── supabase platform ───
        "supabase_edge": SupabaseEdgeFunctionResource(
            supabase_url=_SUPABASE_URL,
            service_key=EnvVar("SUPABASE_SERVICE_KEY"),
            admin_secret=EnvVar("ADMIN_SECRET"),
        ),
        "subproc": SubprocessResource(
            repo_root=str(_REPO_ROOT),
        ),
        # ─── external GIS ───
        "arcgis": ArcGISRestResource(),
        "overpass": OverpassResource(),
        "google_maps": GoogleMapsResource(
            api_key=EnvVar("GOOGLE_MAPS_API_KEY"),
        ),
        "geoapify": GeoapifyResource(
            api_key=EnvVar("GEOAPIFY_API_KEY"),
            is_active=False,
        ),
        # ─── scoring inputs ───
        "open_meteo": OpenMeteoResource(),
        "noaa_co_ops": NOAACoOpsResource(),
        "best_time": BestTimeResource(
            api_key=EnvVar("BESTTIME_API_KEY"),
            is_active=False,
        ),
        # ─── photos ───
        "wikimedia_commons": WikimediaCommonsResource(),
        "flickr": FlickrResource(
            api_key=EnvVar("FLICKR_API_KEY"),
        ),
        "pixabay": PixabayResource(
            api_key=EnvVar("PIXABAY_API_KEY"),
        ),
        "pexels": PexelsResource(
            api_key=EnvVar("PEXELS_API_KEY"),
        ),
        "unsplash": UnsplashResource(
            access_key=EnvVar("UNSPLASH_ACCESS_KEY"),
        ),
        "mapillary": MapillaryResource(
            access_token=EnvVar("MAPILLARY_ACCESS_TOKEN"),
            is_active=False,
        ),
        "nps_multimedia": NPSMultimediaResource(
            is_active=False,
        ),
    },
)
