"""Phases 11-25 — catalog assembly.

Each asset wraps the corresponding `action` SQL from
scripts/run_state_pipeline.py PHASES list. All per-state partitioned.
Dep chain follows the PHASES order; arena → cluster → promote →
enrichment → dedup → geom_queue.

Pattern: SQL action returns a single integer (rows_affected). The asset
captures that into MaterializeResult metadata. Long SQL strings live in
module-level _SQL_* constants for readability — full text mirrors
PHASES, with $STATE replaced by a psycopg2 %s parameter.
"""

import sys

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
)

from ..partitions import state_partitions
from ..resources import PostgresPoolerResource, PostgresSessionResource
from .upstream_loaders import precheck, _STATE_FIPS
from .operator_seeding import operators_for_state


def _exec_one_int(
    conn,
    sql: str,
    params: tuple = (),
) -> int:
    """Run SQL that returns a single int (rows_affected pattern)."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        result = cur.fetchone()
    conn.commit()
    return int(result[0] if result and result[0] is not None else 0)


def _materialize_with_rows(state: str, rows: int, **extra) -> MaterializeResult:
    metadata = {
        "state": MetadataValue.text(state),
        "rows_affected": MetadataValue.int(rows),
        **{k: MetadataValue.int(v) if isinstance(v, int) else MetadataValue.text(str(v))
           for k, v in extra.items()},
    }
    return MaterializeResult(metadata=metadata)


# ════════════════════════════════════════════════════════════════════════
#  Phase 11 — arena_seed (landing → arena promote)
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[precheck],
    group_name="phase_11_to_25_catalog",
    description=(
        "Three global landing→arena promotions: POI landing → arena, "
        "OSM landing → arena, and arena names refreshed from OSM. "
        "Criterion: at least one arena row with county_fips in this state."
    ),
)
def arena_seed(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    conn = postgres.get_connection()
    try:
        rows = _exec_one_int(conn,
            "SELECT coalesce((SELECT promoted FROM public.promote_poi_landing_to_arena()), 0)::int "
            "     + coalesce((SELECT promoted FROM public.promote_osm_landing_to_arena()), 0)::int "
            "     + coalesce((SELECT arena_rows_updated FROM public.refresh_arena_names_from_osm_landing()), 0)::int")
    finally:
        conn.close()
    return _materialize_with_rows(context.partition_key, rows)


# ════════════════════════════════════════════════════════════════════════
#  Phase 12 — cluster_group (state-scoped clustering)
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[arena_seed],
    group_name="phase_11_to_25_catalog",
    description=(
        "State-scoped arena clustering. Runs populate_arena_group_id(state). "
        "State-scope avoids the global O(N²) timeout once arena exceeds 10k rows."
    ),
)
def cluster_group(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    state = context.partition_key
    conn = postgres.get_connection()
    try:
        rows = _exec_one_int(conn,
            "SELECT coalesce((SELECT relation_grouped FROM public.populate_arena_group_id(%s)), 0)::int",
            (state,))
    finally:
        conn.close()
    return _materialize_with_rows(state, rows)


# ════════════════════════════════════════════════════════════════════════
#  Phase 13 — cluster_extras
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[cluster_group],
    group_name="phase_11_to_25_catalog",
    description="Cross-source dedup extras: intra-OSM trigram, intra-POI, cross-source pairs.",
)
def cluster_extras(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    conn = postgres.get_connection()
    try:
        rows = _exec_one_int(conn,
            "SELECT coalesce((SELECT intra_osm_trigram FROM public.populate_arena_extras()), 0)::int")
    finally:
        conn.close()
    return _materialize_with_rows(context.partition_key, rows)


# ════════════════════════════════════════════════════════════════════════
#  Phase 14 — promote (arena → beaches_gold)
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[cluster_extras, operators_for_state],
    group_name="phase_11_to_25_catalog",
    description=(
        "Promotes state's arena rows to beaches_gold via promote_to_gold. "
        "Fires the full per-fid populator + resolver + consumer-table chain. "
        "Idempotent. Depends on operators_for_state because Phase 14's "
        "populate_from_operators_gold reads operator data."
    ),
)
def promote_to_gold(
    context: AssetExecutionContext,
    postgres_session: PostgresSessionResource,
) -> MaterializeResult:
    """Chunked promote: SQL function processes the fid array in a single
    transaction, calling ~14 populator/resolver fns per fid. For OR with
    220 arena rows that's ~3,000 fn calls — hits the 120s pooler timeout.
    Chunk to 10 fids/call with autocommit between so each batch lands
    independently. Session-mode pooler so statement_timeout actually sticks."""
    import time
    state = context.partition_key
    state_fp = _STATE_FIPS[state]
    CHUNK = 10
    conn = postgres_session.get_connection()
    conn.set_session(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '600s'")
            cur.execute(
                "SELECT array_agg(a.fid) "
                "  FROM public.arena a "
                "  JOIN public.counties c ON c.geoid = a.county_fips "
                " WHERE a.is_active AND c.state_fp = %s",
                (state_fp,),
            )
            fids = cur.fetchone()[0] or []
            total_rows = 0
            chunks_done = 0
            t0 = time.time()
            for i in range(0, len(fids), CHUNK):
                chunk = fids[i:i + CHUNK]
                t_chunk = time.time()
                cur.execute(
                    "SELECT coalesce((SELECT rows_promoted + rows_already_in_gold "
                    "                   FROM public.promote_to_gold(%s::bigint[], false::boolean, true::boolean)), 0)::int",
                    (chunk,),
                )
                rows = int(cur.fetchone()[0] or 0)
                total_rows += rows
                chunks_done += 1
                context.log.info(
                    f"  chunk {chunks_done}/{(len(fids)+CHUNK-1)//CHUNK} "
                    f"(fids {chunk[0]}..{chunk[-1]}): +{rows} in {time.time()-t_chunk:.1f}s"
                )
            wall_s = round(time.time() - t0, 1)
    finally:
        conn.close()
    return _materialize_with_rows(
        state, total_rows,
        fids_processed=len(fids),
        chunks_done=chunks_done,
        wall_clock_s=wall_s,
    )


# ════════════════════════════════════════════════════════════════════════
#  Phases 15-18 — post-promote enrichment
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[promote_to_gold],
    group_name="phase_11_to_25_catalog",
    description="Propagate POI address fields (street/city/state/zip) into beaches_gold.address.",
)
def address_poi(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    state = context.partition_key
    conn = postgres.get_connection()
    try:
        rows = _exec_one_int(conn,
            "SELECT public._enrich_address_from_poi_for_state(%s)", (state,))
    finally:
        conn.close()
    return _materialize_with_rows(state, rows)


@asset(
    partitions_def=state_partitions,
    deps=[address_poi],
    group_name="phase_11_to_25_catalog",
    description="TIGER places PIP for address_city fallback when POI didn't supply one.",
)
def address_city(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    state = context.partition_key
    conn = postgres.get_connection()
    try:
        rows = _exec_one_int(conn,
            "SELECT public._enrich_address_city_for_state(%s)", (state,))
    finally:
        conn.close()
    return _materialize_with_rows(state, rows)


@asset(
    partitions_def=state_partitions,
    deps=[address_city],
    group_name="phase_11_to_25_catalog",
    description="Backfill arena.name_source then propagate to beaches_gold.",
)
def name_source(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    state = context.partition_key
    conn = postgres.get_connection()
    try:
        rows = _exec_one_int(conn,
            "SELECT public._enrich_name_source_for_state(%s)", (state,))
    finally:
        conn.close()
    return _materialize_with_rows(state, rows)


@asset(
    partitions_def=state_partitions,
    deps=[name_source],
    group_name="phase_11_to_25_catalog",
    description="Strip plus-code prefixes (^[2-9CFGHJMPQRVWX]{4,}\\+...) from reverse-geocoded addresses.",
)
def strip_plus_codes(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    state = context.partition_key
    conn = postgres.get_connection()
    try:
        rows = _exec_one_int(conn,
            "SELECT public.strip_plus_codes_from_addresses(%s)", (state,))
    finally:
        conn.close()
    return _materialize_with_rows(state, rows)


# ════════════════════════════════════════════════════════════════════════
#  Phase 20 — scoring-readiness (was 19–21; align_scoreable + noaa_station_check
#  retired 2026-05-13 alongside is_scoreable column drop. Matrix C' tier
#  is now the only gate.)
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[strip_plus_codes],
    group_name="phase_11_to_25_catalog",
    description=(
        "Refresh catchment → state_pct → scoring_tier chain. Idempotent. "
        "Wraps refresh_catchment_cascade(state) which chains "
        "recompute_beach_catchment → refresh_catchment_state_pct → refresh_scoring_tier."
    ),
)
def catchment_refresh(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    state = context.partition_key
    conn = postgres.get_connection()
    try:
        rows = _exec_one_int(conn,
            "SELECT coalesce((SELECT catchment_recomputed FROM public.refresh_catchment_cascade(%s)), 0)::int",
            (state,))
    finally:
        conn.close()
    return _materialize_with_rows(state, rows)


# ════════════════════════════════════════════════════════════════════════
#  Phases 22-25 — late-stage cleanup
# ════════════════════════════════════════════════════════════════════════

@asset(
    partitions_def=state_partitions,
    deps=[catchment_refresh],
    group_name="phase_11_to_25_catalog",
    description="Purge cross-state extractions polluting the state's beach data.",
)
def purge_pollution(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    state = context.partition_key
    conn = postgres.get_connection()
    try:
        rows = _exec_one_int(conn,
            "SELECT rows_purged FROM public.purge_cross_state_extractions(%s)", (state,))
    finally:
        conn.close()
    return _materialize_with_rows(state, rows)


@asset(
    partitions_def=state_partitions,
    deps=[purge_pollution],
    group_name="phase_11_to_25_catalog",
    description=(
        "Late-stage dedup via arena-cluster matching. Per-state since 2026-05-12 "
        "(was hardcoded CA). Pair-safety + name-group sub-clustering."
    ),
)
def dedup(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    state = context.partition_key
    conn = postgres.get_connection()
    try:
        rows = _exec_one_int(conn,
            "SELECT coalesce((SELECT kills FROM public.run_late_stage_dedup(%s)), 0)::int",
            (state,))
    finally:
        conn.close()
    return _materialize_with_rows(state, rows)


@asset(
    partitions_def=state_partitions,
    deps=[dedup],
    group_name="phase_11_to_25_catalog",
    description=(
        "Distance+name dedup (1km threshold). Catches OSM+POI cross-source-cluster "
        "dups that arena-cluster-based dedup misses (Ocean Shores Bulkhead, "
        "Picnic Beach OC patterns)."
    ),
)
def dedup_distance_name(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    state = context.partition_key
    conn = postgres.get_connection()
    try:
        rows = _exec_one_int(conn,
            "SELECT coalesce((SELECT kills FROM public.run_distance_name_dedup(%s, 1000)), 0)::int",
            (state,))
    finally:
        conn.close()
    return _materialize_with_rows(state, rows)


@asset(
    partitions_def=state_partitions,
    deps=[dedup_distance_name],
    group_name="phase_11_to_25_catalog",
    description="Drain pending geom-change cascade entries (100 at a time).",
)
def geom_queue(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    conn = postgres.get_connection()
    try:
        rows = _exec_one_int(conn,
            "SELECT coalesce((SELECT fids_processed FROM public.process_geom_change_queue(100)), 0)::int")
    finally:
        conn.close()
    return _materialize_with_rows(context.partition_key, rows)
