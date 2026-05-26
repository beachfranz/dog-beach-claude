"""W1.6 — one-time 7-day backfill of weather_grid_hourly.

Standalone Python that mirrors the Dagster asset's logic without
requiring the Dagster runtime. Run once after W1.3 + W1.4 to bootstrap
weather data so consumer cutover (W2) has something to read.

Usage:
    cd C:/Users/beach/Documents/dog-beach-claude
    .venv-pipeline/Scripts/python.exe scripts/one_off/backfill_weather_grid.py

Idempotent — re-running is safe (upsert on PK conflict).
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import requests
from scripts.common.db import connect


_OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
_HOURLY_FIELDS = ",".join([
    "temperature_2m", "apparent_temperature",
    "precipitation_probability", "precipitation",
    "weathercode", "windspeed_10m", "uv_index",
    "cloud_cover", "is_day",
])
_BATCH_SIZE = 100


def fetch_active_cells(state_filter: str | None = None, skip_already_loaded: bool = True) -> list[tuple[float, float]]:
    """Cells registered in weather_grid. If skip_already_loaded=True,
    excludes cells that already have rows in weather_grid_hourly (idempotent
    retry — only fetches cells the previous run didn't get to)."""
    with connect() as c, c.cursor() as cur:
        base_sql = "SELECT g.grid_lat, g.grid_lon FROM public.weather_grid g"
        clauses = []
        params: list = []
        if skip_already_loaded:
            base_sql += """
                LEFT JOIN LATERAL (
                    SELECT 1 FROM public.weather_grid_hourly
                    WHERE grid_lat = g.grid_lat AND grid_lon = g.grid_lon
                    LIMIT 1
                ) wh ON true
            """
            clauses.append("wh IS NULL")
        if state_filter:
            clauses.append("g.state = %s")
            params.append(state_filter)
        if clauses:
            base_sql += " WHERE " + " AND ".join(clauses)
        base_sql += " ORDER BY g.grid_lat, g.grid_lon"
        cur.execute(base_sql, params)
        return [(float(r[0]), float(r[1])) for r in cur.fetchall()]


def fetch_batch(cells: list[tuple[float, float]], max_retries: int = 3) -> list[dict] | None:
    """Multi-location Open-Meteo fetch. Resilient to network blips +
    rate-limit hits via retry. Returns list of per-cell responses or None
    if all retries exhausted."""
    lats = ",".join(f"{lat:.3f}" for lat, _ in cells)
    lons = ",".join(f"{lon:.3f}" for _, lon in cells)
    params = {
        "latitude": lats, "longitude": lons,
        "hourly": _HOURLY_FIELDS,
        "temperature_unit": "fahrenheit",
        "windspeed_unit": "mph",
        "precipitation_unit": "mm",
        "timezone": "UTC",
        "past_days": "3",
        "forecast_days": "7",
    }
    for attempt in range(max_retries):
        try:
            resp = requests.get(_OPEN_METEO, params=params, timeout=60)
            if resp.status_code == 429:
                wait = 60 + (attempt * 30)
                print(f"  HTTP 429 (rate limit); sleeping {wait}s...")
                time.sleep(wait)
                continue
            if not resp.ok:
                print(f"  HTTP {resp.status_code}: {resp.text[:200]}")
                return None
            data = resp.json()
            return [data] if isinstance(data, dict) else data
        except (requests.ConnectionError, requests.Timeout) as e:
            wait = 5 * (attempt + 1)
            print(f"  Network blip ({type(e).__name__}); attempt {attempt+1}/{max_retries}, sleeping {wait}s...")
            time.sleep(wait)
    print(f"  Exhausted {max_retries} retries; giving up on this batch")
    return None


def upsert_rows(rows: list[tuple]) -> int:
    """Upsert weather_grid_hourly rows in chunks to avoid huge statements."""
    if not rows:
        return 0
    inserted = 0
    INSERT_CHUNK = 1000
    with connect() as c, c.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout='600s'")
        for i in range(0, len(rows), INSERT_CHUNK):
            chunk = rows[i:i + INSERT_CHUNK]
            values = ",".join(
                cur.mogrify(
                    "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::boolean)", row
                ).decode("utf-8") for row in chunk
            )
            cur.execute(f"""
                INSERT INTO public.weather_grid_hourly (
                    grid_lat, grid_lon, forecast_ts, is_observed,
                    temp_air, feels_like, wind_speed,
                    precip_chance, precip_mm, weather_code,
                    uv_index, cloud_cover, is_day
                )
                VALUES {values}
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
            inserted += len(chunk)
        c.commit()
    return inserted


def main():
    # Skip cells already loaded (idempotent retry)
    cells = fetch_active_cells(skip_already_loaded=True)
    print(f"Found {len(cells)} cells needing backfill (already-loaded cells skipped)")
    if not cells:
        print("All cells already loaded — nothing to do.")
        return
    now_utc = datetime.now(timezone.utc)

    total_rows = 0
    failed = 0
    api_calls = 0
    batches = [cells[i:i + _BATCH_SIZE] for i in range(0, len(cells), _BATCH_SIZE)]
    print(f"Fetching in {len(batches)} batches of {_BATCH_SIZE} (with 12s throttle to stay under 600 location-fetches/min)...")

    SLEEP_BETWEEN_BATCHES = 12  # ~5 calls/min = ~500 location-fetches/min < 600/min cap

    t0 = time.time()
    for bi, batch in enumerate(batches):
        b_t0 = time.time()
        responses = fetch_batch(batch)
        api_calls += 1
        if responses is None:
            failed += len(batch)
            time.sleep(SLEEP_BETWEEN_BATCHES)
            continue

        if len(responses) != len(batch):
            print(f"  Batch {bi+1}: response length {len(responses)} != requested {len(batch)}")

        rows = []
        for (grid_lat, grid_lon), cell_data in zip(batch, responses):
            hourly = cell_data.get("hourly", {})
            times = hourly.get("time", [])
            if not times:
                failed += 1
                continue
            for i, t in enumerate(times):
                forecast_ts = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
                is_observed = forecast_ts < now_utc
                def f(name):
                    arr = hourly.get(name, [])
                    return arr[i] if i < len(arr) else None
                is_day = f("is_day")
                is_day_bool = (1 if is_day == 1 else (0 if is_day == 0 else None))
                rows.append((
                    grid_lat, grid_lon, forecast_ts, is_observed,
                    f("temperature_2m"), f("apparent_temperature"),
                    f("windspeed_10m"),
                    f("precipitation_probability"), f("precipitation"),
                    f("weathercode"), f("uv_index"), f("cloud_cover"),
                    is_day_bool,
                ))

        inserted = upsert_rows(rows)
        total_rows += inserted
        elapsed = time.time() - b_t0
        print(f"  B{bi+1:>2}/{len(batches)}: {len(batch)} cells / {inserted} rows ({elapsed:.0f}s)")

        # Throttle (skip on last batch)
        if bi < len(batches) - 1:
            time.sleep(SLEEP_BETWEEN_BATCHES)

    print(f"\nDone in {time.time()-t0:.0f}s — {len(cells)-failed}/{len(cells)} cells, "
          f"{total_rows} rows, {api_calls} API calls, {failed} failed cells")


if __name__ == "__main__":
    main()
