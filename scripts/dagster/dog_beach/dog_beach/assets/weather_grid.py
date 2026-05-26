"""Phase W1.5 — weather_grid_hourly loader.

Reads the materialized weather_grid inventory (populated by triggers on
beaches_gold + dog_parks_gold per W1.3) and refreshes Open-Meteo data
into weather_grid_hourly. Replaces the per-entity Open-Meteo fetch in
daily-beach-refresh / daily-dog-park-refresh — see project pin
[[weather-grid-reference-layer]].

Resource profile:
- ~3,962 active cells across CA + OR + WA + early MD/DE/NH expansion
- Multi-location batched fetch (~100 cells per Open-Meteo API call)
- ~40 API calls per refresh / ~960 calls/day at hourly cadence
- Well under Open-Meteo's 10K/day free-tier limit

Stale-cell filter — only fetches cells whose MAX(fetched_at) is null or
older than fresh_threshold_hours (default 1). Naturally idempotent;
re-running mid-window is a no-op for cells already fresh.

Lives on Dagster (NOT edge function) per locked decision #4 — sidesteps
the WORKER_RESOURCE_LIMIT that killed per-entity-fetch refresh.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta
from typing import Any

import requests
from dagster import (
    AssetExecutionContext,
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
)

from ..resources import OpenMeteoResource, PostgresPoolerResource


# Open-Meteo "Best Match" picks per-location optimal model (HRRR for NA).
# past_days=3 covers bacteria-risk lookback (72h precip rolling sum).
# forecast_days=7 covers the daily-refresh window.
_OPEN_METEO_HOURLY_FIELDS = ",".join([
    "temperature_2m",
    "apparent_temperature",
    "precipitation_probability",
    "precipitation",
    "weathercode",
    "windspeed_10m",
    "uv_index",
    "cloud_cover",
    "is_day",
])

_BATCH_SIZE = 100  # cells per Open-Meteo API call (multi-location)


class WeatherGridConfig(Config):
    """Run-time overrides for refresh_weather_grid.

    Per-cell stale thresholds (differentiated cadence per [[weather-grid-reference-layer]]):
      hot_threshold_hours:  cells with ≥1 active scoring_tier='hourly' beach. Default 1.
      warm_threshold_hours: cells with active 'daily'-tier beach OR scoreable dog park. Default 6.
      cold_threshold_hours: cells with no active consumers (orphans). Default 24.

    state_filter: if set, only refresh cells with this state. Useful for
      ad-hoc refreshes during verification.
    force_full_refresh: if true, bypass all stale thresholds and refresh
      every cell. Use only for full-reload scenarios.
    """
    hot_threshold_hours: float = 1.0
    warm_threshold_hours: float = 6.0
    cold_threshold_hours: float = 24.0
    state_filter: str | None = None
    force_full_refresh: bool = False


@asset(
    group_name="weather_grid",
    description=(
        "Refreshes Open-Meteo hourly weather into weather_grid_hourly for "
        "stale cells in the weather_grid inventory. Batched multi-location "
        "fetch (~100 cells per API call). Per project pin "
        "[[weather-grid-reference-layer]]."
    ),
)
def refresh_weather_grid(
    context: AssetExecutionContext,
    config: WeatherGridConfig,
    postgres: PostgresPoolerResource,
    open_meteo: OpenMeteoResource,
) -> MaterializeResult:
    context.log.info(
        f"refresh_weather_grid starting — hot={config.hot_threshold_hours}h "
        f"warm={config.warm_threshold_hours}h cold={config.cold_threshold_hours}h "
        f"state_filter={config.state_filter} force={config.force_full_refresh}"
    )

    # ─── 1. Pick stale cells with tier-based threshold ──────────────────
    # Each cell gets a stale threshold based on the "hottest" consumer
    # in its 0.025° bin:
    #   ≥1 scoring_tier='hourly' beach   → HOT (1h default)
    #   ≥1 'daily' beach or scoreable DP → WARM (6h default)
    #   no active consumers              → COLD (24h default; orphans)
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            params: list[Any] = [
                config.hot_threshold_hours,
                config.warm_threshold_hours,
                config.cold_threshold_hours,
            ]
            sql = """
                WITH cell_tier AS (
                    SELECT g.grid_lat, g.grid_lon,
                        CASE
                            WHEN EXISTS (
                                SELECT 1 FROM public.beaches_gold b
                                WHERE b.weather_grid_lat=g.grid_lat
                                  AND b.weather_grid_lon=g.grid_lon
                                  AND b.is_active AND b.scoring_tier='hourly'
                            ) THEN %s::numeric
                            WHEN EXISTS (
                                SELECT 1 FROM public.beaches_gold b
                                WHERE b.weather_grid_lat=g.grid_lat
                                  AND b.weather_grid_lon=g.grid_lon
                                  AND b.is_active AND b.scoring_tier='daily'
                            ) OR EXISTS (
                                SELECT 1 FROM public.dog_parks_gold d
                                WHERE d.weather_grid_lat=g.grid_lat
                                  AND d.weather_grid_lon=g.grid_lon
                                  AND d.is_active AND d.is_scoreable
                            ) THEN %s::numeric
                            ELSE %s::numeric
                        END AS stale_threshold_hours,
                        g.state
                    FROM public.weather_grid g
                )
                SELECT ct.grid_lat, ct.grid_lon, ct.stale_threshold_hours
                FROM cell_tier ct
                LEFT JOIN LATERAL (
                    SELECT MAX(fetched_at) AS last_fetched
                    FROM public.weather_grid_hourly
                    WHERE grid_lat = ct.grid_lat AND grid_lon = ct.grid_lon
                ) lf ON true
                WHERE (%s::boolean
                       OR lf.last_fetched IS NULL
                       OR lf.last_fetched < (now() - (ct.stale_threshold_hours || ' hours')::interval))
            """
            params.append(config.force_full_refresh)
            if config.state_filter:
                sql += " AND ct.state = %s"
                params.append(config.state_filter)
            sql += " ORDER BY ct.stale_threshold_hours, ct.grid_lat, ct.grid_lon"
            cur.execute(sql, params)
            rows = cur.fetchall()
            stale_cells = [(float(r[0]), float(r[1])) for r in rows]
            tier_counts = {1.0: 0, 6.0: 0, 24.0: 0, "other": 0}
            for r in rows:
                t = float(r[2])
                if t == config.hot_threshold_hours:
                    tier_counts[1.0] = tier_counts.get(1.0, 0) + 1
                elif t == config.warm_threshold_hours:
                    tier_counts[6.0] = tier_counts.get(6.0, 0) + 1
                elif t == config.cold_threshold_hours:
                    tier_counts[24.0] = tier_counts.get(24.0, 0) + 1
                else:
                    tier_counts["other"] += 1
            context.log.info(
                f"Stale cells: {len(stale_cells)} total "
                f"({tier_counts.get(1.0,0)} hot, {tier_counts.get(6.0,0)} warm, {tier_counts.get(24.0,0)} cold)"
            )
    finally:
        conn.close()

    if not stale_cells:
        context.log.info("No stale cells to refresh.")
        return MaterializeResult(metadata={"cells_refreshed": 0, "skipped": True})

    context.log.info(f"Found {len(stale_cells)} stale cells; batching {_BATCH_SIZE}/call")

    # ─── 2. Batched multi-location fetch ──────────────────────────────
    total_inserted = 0
    total_failed_cells = 0
    api_calls = 0
    t0 = time.time()
    now_utc = datetime.now(timezone.utc)

    for batch_idx in range(0, len(stale_cells), _BATCH_SIZE):
        batch = stale_cells[batch_idx : batch_idx + _BATCH_SIZE]
        lats = ",".join(f"{lat:.3f}" for lat, _ in batch)
        lons = ",".join(f"{lon:.3f}" for _, lon in batch)

        try:
            resp = requests.get(
                open_meteo.forecast_endpoint,
                params={
                    "latitude": lats,
                    "longitude": lons,
                    "hourly": _OPEN_METEO_HOURLY_FIELDS,
                    "temperature_unit": "fahrenheit",
                    "windspeed_unit": "mph",
                    "precipitation_unit": "mm",
                    "timezone": "UTC",
                    "past_days": "3",
                    "forecast_days": "7",
                },
                timeout=open_meteo.default_timeout_seconds,
            )
            resp.raise_for_status()
            api_calls += 1
        except Exception as e:
            context.log.warning(f"Batch {batch_idx//_BATCH_SIZE+1} failed: {e}")
            total_failed_cells += len(batch)
            continue

        data = resp.json()
        # Multi-location returns a list; single-location returns a dict.
        # Normalize.
        if isinstance(data, dict):
            data = [data]

        if len(data) != len(batch):
            context.log.warning(
                f"Batch returned {len(data)} responses for {len(batch)} requested cells — partial"
            )

        # ─── 3. Upsert rows for each cell ─────────────────────────────
        rows_to_insert: list[tuple] = []
        for (grid_lat, grid_lon), cell_data in zip(batch, data):
            hourly = cell_data.get("hourly", {})
            times = hourly.get("time", [])
            if not times:
                total_failed_cells += 1
                continue
            for i, t in enumerate(times):
                # Open-Meteo returns "YYYY-MM-DDTHH:MM" in the requested
                # timezone (UTC here). Parse as UTC.
                forecast_ts = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
                is_observed = forecast_ts < now_utc

                def field(name: str) -> Any:
                    arr = hourly.get(name, [])
                    return arr[i] if i < len(arr) else None

                rows_to_insert.append((
                    grid_lat, grid_lon, forecast_ts, is_observed,
                    field("temperature_2m"),
                    field("apparent_temperature"),
                    field("windspeed_10m"),
                    field("precipitation_probability"),
                    field("precipitation"),
                    field("weathercode"),
                    field("uv_index"),
                    field("cloud_cover"),
                    1 if field("is_day") == 1 else (0 if field("is_day") == 0 else None),
                ))

        # Upsert all rows for this batch (still single transaction)
        if rows_to_insert:
            conn = postgres.get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("SET LOCAL statement_timeout='600s'")
                    # Chunk the insert to avoid huge single statements
                    INSERT_CHUNK = 1000
                    for chunk_start in range(0, len(rows_to_insert), INSERT_CHUNK):
                        chunk = rows_to_insert[chunk_start : chunk_start + INSERT_CHUNK]
                        values_sql = ",".join(cur.mogrify(
                            "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::boolean)", row
                        ).decode("utf-8") for row in chunk)
                        cur.execute(f"""
                            INSERT INTO public.weather_grid_hourly (
                                grid_lat, grid_lon, forecast_ts, is_observed,
                                temp_air, feels_like, wind_speed,
                                precip_chance, precip_mm, weather_code,
                                uv_index, cloud_cover, is_day
                            )
                            VALUES {values_sql}
                            ON CONFLICT (grid_lat, grid_lon, forecast_ts) DO UPDATE SET
                                is_observed = EXCLUDED.is_observed,
                                temp_air = EXCLUDED.temp_air,
                                feels_like = EXCLUDED.feels_like,
                                wind_speed = EXCLUDED.wind_speed,
                                precip_chance = EXCLUDED.precip_chance,
                                precip_mm = EXCLUDED.precip_mm,
                                weather_code = EXCLUDED.weather_code,
                                uv_index = EXCLUDED.uv_index,
                                cloud_cover = EXCLUDED.cloud_cover,
                                is_day = EXCLUDED.is_day,
                                fetched_at = now()
                        """)
                        total_inserted += len(chunk)
                conn.commit()
            finally:
                conn.close()

        context.log.info(
            f"Batch {batch_idx//_BATCH_SIZE+1}/{(len(stale_cells)+_BATCH_SIZE-1)//_BATCH_SIZE}: "
            f"{len(batch)} cells / {len(rows_to_insert)} rows"
        )

    elapsed = time.time() - t0
    context.log.info(
        f"refresh_weather_grid complete — {len(stale_cells)} cells, "
        f"{total_inserted} rows upserted, {total_failed_cells} failed, "
        f"{api_calls} Open-Meteo calls, {elapsed:.1f}s wall"
    )

    return MaterializeResult(
        metadata={
            "cells_refreshed": len(stale_cells) - total_failed_cells,
            "cells_failed": total_failed_cells,
            "rows_upserted": total_inserted,
            "api_calls": api_calls,
            "elapsed_seconds": MetadataValue.float(round(elapsed, 1)),
            "fresh_threshold_hours": config.fresh_threshold_hours,
        }
    )


@asset(
    group_name="weather_grid",
    description=(
        "W1.8 trigger backstop — rebuilds weather_grid from a UNION of "
        "active beaches_gold + dog_parks_gold. Cheap insurance against "
        "trigger drift. Per [[weather-grid-reference-layer]]."
    ),
)
def rebuild_weather_grid_inventory(
    context: AssetExecutionContext,
    postgres: PostgresPoolerResource,
) -> MaterializeResult:
    """Idempotent rebuild — only inserts cells not already in weather_grid."""
    conn = postgres.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO public.weather_grid (grid_lat, grid_lon, geom, state)
                SELECT DISTINCT
                    weather_grid_lat,
                    weather_grid_lon,
                    ST_SetSRID(ST_MakePoint(weather_grid_lon::float, weather_grid_lat::float), 4326)::geography,
                    state
                FROM (
                    SELECT weather_grid_lat, weather_grid_lon, state
                    FROM public.beaches_gold
                    WHERE weather_grid_lat IS NOT NULL AND is_active
                      AND scoring_tier IN ('daily','hourly')
                    UNION
                    SELECT weather_grid_lat, weather_grid_lon, state
                    FROM public.dog_parks_gold
                    WHERE weather_grid_lat IS NOT NULL AND is_active AND is_scoreable
                ) u
                ON CONFLICT (grid_lat, grid_lon) DO NOTHING
                RETURNING 1
            """)
            inserted = cur.rowcount
            cur.execute("SELECT COUNT(*) FROM public.weather_grid")
            total = cur.fetchone()[0]
        conn.commit()
    finally:
        conn.close()
    context.log.info(f"Inventory rebuild — {inserted} new cells; {total} total")
    return MaterializeResult(metadata={"cells_inserted": inserted, "cells_total": total})
